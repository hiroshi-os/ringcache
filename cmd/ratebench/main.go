// Command ratebench measures token-bucket accuracy and middleware latency
// against a live ringcache cluster.
//
// It is not a synthetic estimate: it issues real HTTP through pkg/ratelimit
// middleware (and a no-limiter baseline) and prints measured rates / percentiles.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/ringcache/pkg/ratelimit"
)

func main() {
	var (
		addrs      = flag.String("addrs", env("RINGCACHE_ADDRS", "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082"), "ringcache base URLs")
		target     = flag.Float64("target", 100, "target allow rate (tokens/s)")
		burst      = flag.Float64("burst", 20, "burst")
		mult       = flag.Float64("mult", 2, "offer load as mult × target")
		duration   = flag.Duration("duration", 60*time.Second, "run duration")
		apiKey     = flag.String("key", "ratebench", "API key / bucket key")
		timeout    = flag.Duration("timeout", 5*time.Millisecond, "ringcache client timeout (fail-open)")
		killAfter  = flag.Duration("kill-after", 0, "if >0, kill primary after this delay (fail-open scenario)")
		killScript = flag.String("kill-cmd", "", "shell command to kill primary (optional; else HTTP stop via pid file)")
		baseline   = flag.Bool("baseline", false, "also measure p50/p99 without limiter")
	)
	flag.Parse()

	addrList := splitCSV(*addrs)
	if err := waitHealthy(addrList, 15*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "health: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("ratebench target=%.0f/s burst=%.0f offer=%.0fx duration=%s timeout=%s key=%s\n",
		*target, *burst, *mult, *duration, *timeout, *apiKey)
	fmt.Printf("started %s\n", time.Now().UTC().Format(time.RFC3339))

	if *baseline {
		runPhase("baseline_no_limiter", addrList, *apiKey, *target, *burst, *mult, *duration, *timeout, false, 0, "")
		return
	}
	runPhase("limited", addrList, *apiKey, *target, *burst, *mult, *duration, *timeout, true, *killAfter, *killScript)
}

func runPhase(name string, addrs []string, apiKey string, target, burst, mult float64, duration, timeout time.Duration, limited bool, killAfter time.Duration, killCmd string) {
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
	if limited {
		cli := &ratelimit.Client{Addrs: addrs, Timeout: timeout}
		handler = ratelimit.Middleware(ratelimit.Config{
			Client: cli, Rate: target, Burst: burst, Cost: 1,
			APIKeyHeader: "X-API-Key",
			Logger:       log.New(io.Discard, "", 0),
		})(handler)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	offerRate := target * mult
	interval := time.Duration(float64(time.Second) / offerRate)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}

	var (
		allowed       atomic.Int64
		denied        atomic.Int64
		failOpenStart time.Time
		failOpenEnd   time.Time
		failOpenMu    sync.Mutex
		lats          []time.Duration
		latMu         sync.Mutex
	)
	failOpenBefore := ratelimit.FailOpenTotal.Value()

	stop := make(chan struct{})
	var workerWg sync.WaitGroup
	var prodWg sync.WaitGroup
	const workers = 128
	jobs := make(chan struct{}, 1024)
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	for w := 0; w < workers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for range jobs {
				req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
				req.Header.Set("X-API-Key", apiKey)
				t0 := time.Now()
				res, err := client.Do(req)
				dt := time.Since(t0)
				if err != nil {
					continue
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				latMu.Lock()
				lats = append(lats, dt)
				latMu.Unlock()
				if res.StatusCode == 429 {
					denied.Add(1)
				} else if res.StatusCode == 200 {
					allowed.Add(1)
				}
			}
		}()
	}
	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				select {
				case jobs <- struct{}{}:
				default:
					// backlog: drop tick (still under offer pressure)
				}
			}
		}
	}()

	if killAfter > 0 && limited {
		go func() {
			time.Sleep(killAfter)
			primary := lookupPrimary(addrs[0], apiKey)
			fmt.Printf("%s: killing primary=%s at t+%s\n", name, primary, killAfter)
			failOpenMu.Lock()
			failOpenStart = time.Now()
			failOpenMu.Unlock()
			if killCmd != "" {
				_ = exec.Command("bash", "-c", killCmd).Run()
			} else {
				killLocalPrimary(primary)
			}
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				time.Sleep(50 * time.Millisecond)
				ok := 0
				for _, a := range addrs {
					res, err := http.Get(strings.TrimRight(a, "/") + "/health")
					if err == nil {
						res.Body.Close()
						if res.StatusCode == 200 {
							ok++
						}
					}
				}
				if ok >= 2 {
					failOpenMu.Lock()
					if failOpenEnd.IsZero() {
						failOpenEnd = time.Now()
					}
					failOpenMu.Unlock()
					return
				}
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	prodWg.Wait()
	close(jobs)
	workerWg.Wait()

	a := float64(allowed.Load())
	elapsed := duration.Seconds()
	allowRate := a / elapsed
	errPct := 0.0
	if target > 0 {
		errPct = (allowRate - target) / target * 100
	}

	latMu.Lock()
	p50, p99 := percentiles(lats)
	latMu.Unlock()

	failOpenDelta := ratelimit.FailOpenTotal.Value() - failOpenBefore
	fmt.Printf("%s  offered=%.0f/s  allowed=%d  denied=%d  allow_rate=%.2f/s  error_vs_target=%+.2f%%  p50=%s  p99=%s  samples=%d  fail_open_delta=%d\n",
		name, offerRate, allowed.Load(), denied.Load(), allowRate, errPct, p50, p99, len(lats), failOpenDelta)

	failOpenMu.Lock()
	if !failOpenStart.IsZero() {
		end := failOpenEnd
		if end.IsZero() {
			end = time.Now()
		}
		fmt.Printf("%s  fail_open_window=%s  (primary kill -> survivors healthy)\n", name, end.Sub(failOpenStart))
	}
	failOpenMu.Unlock()
}

func lookupPrimary(addr, key string) string {
	res, err := http.Get(strings.TrimRight(addr, "/") + "/ring?key=" + key)
	if err != nil {
		return ""
	}
	defer res.Body.Close()
	var d struct {
		Owners []string `json:"owners"`
	}
	_ = json.NewDecoder(res.Body).Decode(&d)
	if len(d.Owners) == 0 {
		return ""
	}
	return d.Owners[0]
}

func killLocalPrimary(id string) {
	letter := strings.TrimPrefix(id, "node-")
	if letter == id && len(id) > 0 {
		letter = string(id[len(id)-1])
	}
	paths := []string{
		filepath.Join(os.TempDir(), "ringcache-"+letter+".pid"),
		"/tmp/ringcache-" + letter + ".pid",
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var pid int
		fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &pid)
		if pid <= 0 {
			continue
		}
		// On Windows, taskkill with the Win32 PID is reliable.
		if err := exec.Command("taskkill", "/F", "/PID", fmt.Sprintf("%d", pid)).Run(); err == nil {
			_ = os.Remove(p)
			return
		}
		if proc, err := os.FindProcess(pid); err == nil {
			_ = proc.Kill()
			_ = os.Remove(p)
			return
		}
	}
	// Git Bash / MSYS pid fallback.
	script := fmt.Sprintf(
		`pidf="/tmp/ringcache-%s.pid"; if [ -f "$pidf" ]; then kill "$(cat "$pidf")" 2>/dev/null || true; rm -f "$pidf"; fi`,
		letter,
	)
	_ = exec.Command("bash", "-lc", script).Run()
	fmt.Fprintf(os.Stderr, "kill attempted for %s\n", id)
}

func percentiles(lats []time.Duration) (p50, p99 time.Duration) {
	if len(lats) == 0 {
		return 0, 0
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p := func(q float64) time.Duration {
		x := q * float64(len(lats)-1)
		i := int(math.Round(x))
		if i < 0 {
			i = 0
		}
		if i >= len(lats) {
			i = len(lats) - 1
		}
		return lats[i]
	}
	return p(0.50), p(0.99)
}

func waitHealthy(addrs []string, d time.Duration) error {
	deadline := time.Now().Add(d)
	var last error
	for time.Now().Before(deadline) {
		ok := 0
		for _, a := range addrs {
			res, err := http.Get(strings.TrimRight(a, "/") + "/health")
			if err != nil {
				last = err
				continue
			}
			res.Body.Close()
			if res.StatusCode == 200 {
				ok++
			}
		}
		if ok == len(addrs) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("not healthy: %v", last)
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, strings.TrimRight(p, "/"))
		}
	}
	return out
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

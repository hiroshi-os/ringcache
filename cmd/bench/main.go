// Command bench drives a live ringcache cluster and prints measured latencies.
//
// This is not a synthetic estimate: it issues real HTTP SET/GET against the
// addresses you pass and reports wall-clock ops/s plus percentile latencies.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type setBody struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTLMs int64  `json:"ttl_ms"`
}

func main() {
	var (
		addrs = flag.String("addrs", env("RINGCACHE_ADDRS", "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082"), "comma-separated base URLs")
		n     = flag.Int("n", 4000, "operations per phase")
		c     = flag.Int("c", 32, "concurrent workers")
		ttl   = flag.Int64("ttl-ms", 60000, "TTL for SETs")
		keyn  = flag.Int("keys", 1000, "distinct key space")
	)
	flag.Parse()
	bases := splitCSV(*addrs)
	if len(bases) == 0 {
		fmt.Fprintln(os.Stderr, "no addrs")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	if err := waitHealthy(client, bases, 15*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "health: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("ringcache bench  n=%d  c=%d  keys=%d  addrs=%s\n", *n, *c, *keyn, strings.Join(bases, ","))
	fmt.Printf("started %s\n", time.Now().UTC().Format(time.RFC3339))

	setLats, setErrs, setWall := runPhase(client, bases, *n, *c, *keyn, func(cli *http.Client, base string, i int) error {
		body, _ := json.Marshal(setBody{
			Key:   keyOf(i, *keyn),
			Value: fmt.Sprintf("v-%d", i),
			TTLMs: *ttl,
		})
		req, err := http.NewRequest(http.MethodPut, base+"/v1/set", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := cli.Do(req)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			return fmt.Errorf("set status %d", res.StatusCode)
		}
		return nil
	})
	printPhase("SET", setLats, setErrs, setWall)

	getLats, getErrs, getWall := runPhase(client, bases, *n, *c, *keyn, func(cli *http.Client, base string, i int) error {
		url := base + "/v1/get?key=" + keyOf(i, *keyn)
		res, err := cli.Get(url)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 && res.StatusCode != 404 {
			return fmt.Errorf("get status %d", res.StatusCode)
		}
		return nil
	})
	printPhase("GET", getLats, getErrs, getWall)
}

func runPhase(client *http.Client, bases []string, n, conc, keyn int, fn func(*http.Client, string, int) error) ([]time.Duration, int, time.Duration) {
	var (
		idx  atomic.Int64
		errN atomic.Int64
		wg   sync.WaitGroup
		mu   sync.Mutex
		lats []time.Duration
	)
	lats = make([]time.Duration, 0, n)
	start := time.Now()
	wg.Add(conc)
	for w := 0; w < conc; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(idx.Add(1) - 1)
				if i >= n {
					return
				}
				base := bases[i%len(bases)]
				t0 := time.Now()
				err := fn(client, base, i)
				dt := time.Since(t0)
				if err != nil {
					errN.Add(1)
					continue
				}
				mu.Lock()
				lats = append(lats, dt)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return lats, int(errN.Load()), time.Since(start)
}

func printPhase(name string, lats []time.Duration, errs int, wall time.Duration) {
	if len(lats) == 0 {
		fmt.Printf("%s  errors=%d  wall=%s  no successful samples\n", name, errs, wall)
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, d := range lats {
		sum += d
	}
	ops := float64(len(lats)) / wall.Seconds()
	p := func(q float64) time.Duration {
		if len(lats) == 1 {
			return lats[0]
		}
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
	fmt.Printf("%s  ok=%d  errors=%d  wall=%s  ops/s=%.0f  p50=%s  p95=%s  p99=%s  max=%s  mean=%s\n",
		name, len(lats), errs, wall.Round(time.Millisecond), ops, p(0.50), p(0.95), p(0.99), lats[len(lats)-1], sum/time.Duration(len(lats)))
}

func waitHealthy(client *http.Client, bases []string, d time.Duration) error {
	deadline := time.Now().Add(d)
	var last error
	for time.Now().Before(deadline) {
		ok := 0
		for _, b := range bases {
			res, err := client.Get(strings.TrimRight(b, "/") + "/health")
			if err != nil {
				last = err
				continue
			}
			res.Body.Close()
			if res.StatusCode == 200 {
				ok++
			} else {
				last = fmt.Errorf("%s status %d", b, res.StatusCode)
			}
		}
		if ok == len(bases) {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("cluster not healthy: %v", last)
}

func keyOf(i, keyn int) string {
	return fmt.Sprintf("bench:%d", i%keyn)
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

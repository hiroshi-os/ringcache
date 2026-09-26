// Command ratelimit-demo is a tiny API server behind pkg/ratelimit middleware.
//
//	go run ./examples/ratelimit-demo -listen :9090 -addrs http://127.0.0.1:8080,...
//	curl -H 'X-API-Key: alice' http://127.0.0.1:9090/api/hello
package main

import (
	"encoding/json"
	"expvar"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hiroshi-os/ringcache/pkg/ratelimit"
)

func main() {
	var (
		listen   = flag.String("listen", ":9090", "demo API listen address")
		addrs    = flag.String("addrs", env("RINGCACHE_ADDRS", "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082"), "ringcache URLs")
		rate     = flag.Float64("rate", 5, "tokens per second")
		burst    = flag.Float64("burst", 5, "burst")
		timeout  = flag.Duration("timeout", 5*time.Millisecond, "ringcache RPC timeout")
		inflight = flag.Int("max-inflight", 32, "per-instance concurrent cap per key")
		dryRun   = flag.Bool("dry-run", false, "log would-be 429s but allow")
	)
	flag.Parse()

	cli := &ratelimit.Client{
		Addrs:   splitCSV(*addrs),
		Timeout: *timeout,
	}
	mw := ratelimit.Middleware(ratelimit.Config{
		Client:      cli,
		Rate:        *rate,
		Burst:       *burst,
		MaxInFlight: *inflight,
		DryRun:      *dryRun,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"hello": "world",
			"key":   r.Header.Get("X-API-Key"),
		})
	})
	mux.Handle("/debug/vars", expvar.Handler())

	log.Printf("ratelimit-demo on %s rate=%.0f burst=%.0f timeout=%s (RATELIMIT_DISABLED=%s)",
		*listen, *rate, *burst, *timeout, os.Getenv("RATELIMIT_DISABLED"))
	log.Fatal(http.ListenAndServe(*listen, mw(mux)))
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

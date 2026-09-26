package ratelimit

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FailOpenTotal counts requests allowed because ringcache was unreachable,
// errored, or timed out. Exposed via expvar as "ratelimit_fail_open_total".
var FailOpenTotal = expvar.NewInt("ratelimit_fail_open_total")

func init() {
	// Ensure the name matches the task exactly even if the var is renamed.
	_ = FailOpenTotal
}

// Decision is the outcome of a Take against ringcache.
type Decision struct {
	Allowed      bool    `json:"allowed"`
	Remaining    float64 `json:"remaining"`
	RetryAfterMs int64   `json:"retry_after_ms"`
	Primary      string  `json:"primary,omitempty"`
	ServedBy     string  `json:"served_by,omitempty"`
}

// Client talks to a ringcache cluster's /v1/ratelimit/take endpoint.
type Client struct {
	Addrs   []string
	Timeout time.Duration
	HTTP    *http.Client

	once   sync.Once
	shared *http.Client
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	c.once.Do(func() {
		timeout := c.Timeout
		if timeout <= 0 {
			timeout = 5 * time.Millisecond
		}
		c.shared = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          128,
				MaxIdleConnsPerHost:   32,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: timeout,
				ForceAttemptHTTP2:     false,
			},
		}
	})
	return c.shared
}

// Take asks any coordinator; that node forwards to the key's PRIMARY.
func (c *Client) Take(ctx context.Context, key string, rate, burst, cost float64) (Decision, error) {
	if len(c.Addrs) == 0 {
		return Decision{}, fmt.Errorf("no ringcache addrs")
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Millisecond
	}
	httpClient := c.httpClient()
	body, _ := json.Marshal(map[string]any{
		"key": key, "rate": rate, "burst": burst, "cost": cost,
	})
	var lastErr error
	for _, addr := range c.Addrs {
		base := strings.TrimRight(addr, "/")
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/ratelimit/take", bytes.NewReader(body))
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := httpClient.Do(req)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d %s", res.StatusCode, bytes.TrimSpace(raw))
			continue
		}
		var d Decision
		if err := json.Unmarshal(raw, &d); err != nil {
			lastErr = err
			continue
		}
		return d, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("all ringcache addrs failed")
	}
	return Decision{}, lastErr
}

// Config configures the HTTP middleware.
type Config struct {
	Client *Client
	// Rate / Burst for the per-API-key token bucket (tokens per second / cap).
	Rate  float64
	Burst float64
	// Cost per request (default 1).
	Cost float64
	// Header carrying the API key (default X-API-Key).
	APIKeyHeader string
	// MaxInFlight is the per-instance concurrent-request cap per key (0 = off).
	MaxInFlight int
	// Disabled bypasses everything (also set by RATELIMIT_DISABLED=1).
	Disabled bool
	// DryRun logs would-be 429s but still allows the request.
	DryRun bool
	// Logger for dry-run / fail-open notes. Defaults to log.Default().
	Logger *log.Logger
	// KeyFunc extracts the rate-limit key from the request. Default: header.
	KeyFunc func(*http.Request) string
}

// Middleware returns net/http middleware.
//
// FAIL OPEN: if ringcache is unreachable, errors, or does not answer within
// Client.Timeout (default 5ms), the request is allowed and
// ratelimit_fail_open_total is incremented.
//
// The concurrent limiter is an in-process semaphore — per-instance only.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	if cfg.APIKeyHeader == "" {
		cfg.APIKeyHeader = "X-API-Key"
	}
	if cfg.Cost <= 0 {
		cfg.Cost = 1
	}
	if cfg.Rate <= 0 {
		cfg.Rate = 100
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 20
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.KeyFunc == nil {
		hdr := cfg.APIKeyHeader
		cfg.KeyFunc = func(r *http.Request) string {
			return strings.TrimSpace(r.Header.Get(hdr))
		}
	}
	if os.Getenv("RATELIMIT_DISABLED") == "1" {
		cfg.Disabled = true
	}

	var inflight sync.Map // key → *int64

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.Disabled {
				next.ServeHTTP(w, r)
				return
			}
			key := cfg.KeyFunc(r)
			if key == "" {
				key = "anonymous"
			}

			if cfg.MaxInFlight > 0 {
				release, ok := acquireInflight(&inflight, key, cfg.MaxInFlight)
				if !ok {
					if cfg.DryRun {
						cfg.Logger.Printf("ratelimit dry-run: would 429 concurrent key=%s", key)
					} else {
						w.Header().Set("Retry-After", "1")
						http.Error(w, `{"error":"too many concurrent requests"}`, http.StatusTooManyRequests)
						return
					}
				} else {
					defer release()
				}
			}

			if cfg.Client == nil {
				next.ServeHTTP(w, r)
				return
			}

			d, err := cfg.Client.Take(r.Context(), key, cfg.Rate, cfg.Burst, cfg.Cost)
			if err != nil {
				FailOpenTotal.Add(1)
				// Avoid flooding logs under load / port exhaustion.
				if FailOpenTotal.Value() <= 5 || FailOpenTotal.Value()%1000 == 0 {
					cfg.Logger.Printf("ratelimit fail-open key=%s (count=%d): %v", key, FailOpenTotal.Value(), err)
				}
				next.ServeHTTP(w, r)
				return
			}
			if d.Allowed {
				next.ServeHTTP(w, r)
				return
			}
			retrySec := int(math.Ceil(float64(d.RetryAfterMs) / 1000.0))
			if retrySec < 1 {
				retrySec = 1
			}
			if cfg.DryRun {
				cfg.Logger.Printf("ratelimit dry-run: would 429 key=%s remaining=%.2f retry_after_ms=%d",
					key, d.Remaining, d.RetryAfterMs)
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("Retry-After", strconv.Itoa(retrySec))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = fmt.Fprintf(w, `{"error":"rate limited","remaining":%g,"retry_after_ms":%d}`,
				d.Remaining, d.RetryAfterMs)
		})
	}
}

func acquireInflight(m *sync.Map, key string, max int) (release func(), ok bool) {
	v, _ := m.LoadOrStore(key, new(int64))
	counter := v.(*int64)
	for {
		cur := atomic.LoadInt64(counter)
		if cur >= int64(max) {
			return nil, false
		}
		if atomic.CompareAndSwapInt64(counter, cur, cur+1) {
			return func() { atomic.AddInt64(counter, -1) }, true
		}
	}
}

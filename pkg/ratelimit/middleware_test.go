package ratelimit_test

import (
	"bytes"
	"encoding/json"
	"expvar"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hiroshi-os/ringcache/internal/node"
	"github.com/hiroshi-os/ringcache/pkg/ratelimit"
)

func startRLCluster(t *testing.T) []string {
	t.Helper()
	placeholder := map[string]string{
		"a": "http://127.0.0.1:1",
		"b": "http://127.0.0.1:2",
		"c": "http://127.0.0.1:3",
	}
	var srvs []*httptest.Server
	var nodes []*node.Node
	for _, id := range []string{"a", "b", "c"} {
		peers := map[string]string{}
		for k, v := range placeholder {
			peers[k] = v
		}
		n, err := node.New(node.Config{
			ID: id, Listen: "test", Peers: peers,
			Replicas: 2, Capacity: 64, VNodes: 32,
			ReplicaTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(n.Handler())
		t.Cleanup(func() { srv.Close(); n.Close() })
		srvs = append(srvs, srv)
		nodes = append(nodes, n)
	}
	urls := map[string]string{"a": srvs[0].URL, "b": srvs[1].URL, "c": srvs[2].URL}
	for _, srv := range srvs {
		for id, u := range urls {
			body, _ := json.Marshal(map[string]string{"id": id, "url": u})
			res, err := http.Post(srv.URL+"/admin/members", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
	}
	_ = nodes
	return []string{srvs[0].URL, srvs[1].URL, srvs[2].URL}
}

func TestMiddleware429AndRetryAfter(t *testing.T) {
	os.Unsetenv("RATELIMIT_DISABLED")
	addrs := startRLCluster(t)
	cli := &ratelimit.Client{Addrs: addrs, Timeout: 500 * time.Millisecond}
	mw := ratelimit.Middleware(ratelimit.Config{
		Client: cli, Rate: 5, Burst: 5, Cost: 1,
	})
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	denied := 0
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
		req.Header.Set("X-API-Key", "user-429")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode == 429 {
			denied++
			ra := res.Header.Get("Retry-After")
			n, err := strconv.Atoi(ra)
			if err != nil || n < 1 {
				t.Fatalf("Retry-After=%q", ra)
			}
		}
	}
	if denied == 0 {
		t.Fatal("expected some 429s")
	}
}

func TestMiddlewareFailOpenOnTimeout(t *testing.T) {
	os.Unsetenv("RATELIMIT_DISABLED")
	before := failOpenCount()
	cli := &ratelimit.Client{
		Addrs:   []string{"http://127.0.0.1:1"},
		Timeout: 5 * time.Millisecond,
	}
	mw := ratelimit.Middleware(ratelimit.Config{Client: cli, Rate: 1, Burst: 1})
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("fail-open should allow, got %d", res.StatusCode)
	}
	after := failOpenCount()
	if after <= before {
		t.Fatalf("fail_open counter did not increment: before=%d after=%d", before, after)
	}
}

func TestMiddlewareKillSwitch(t *testing.T) {
	t.Setenv("RATELIMIT_DISABLED", "1")
	cli := &ratelimit.Client{Addrs: []string{"http://127.0.0.1:1"}, Timeout: time.Millisecond}
	mw := ratelimit.Middleware(ratelimit.Config{Client: cli, Rate: 1, Burst: 1})
	called := 0
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(200)
	}))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	for i := 0; i < 5; i++ {
		res, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("status %d", res.StatusCode)
		}
	}
	if called != 5 {
		t.Fatalf("called=%d", called)
	}
}

func TestConcurrentLimiter(t *testing.T) {
	os.Unsetenv("RATELIMIT_DISABLED")
	gate := make(chan struct{})
	mw := ratelimit.Middleware(ratelimit.Config{
		Client:      nil,
		MaxInFlight: 2,
		KeyFunc:     func(r *http.Request) string { return "same" },
	})
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-gate
		w.WriteHeader(200)
	}))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	var wg sync.WaitGroup
	var got429 atomic.Int64
	var got200 atomic.Int64
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := http.Get(srv.URL + "/")
			if err != nil {
				return
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode == 429 {
				got429.Add(1)
			}
			if res.StatusCode == 200 {
				got200.Add(1)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()
	if got429.Load() < 1 {
		t.Fatalf("expected concurrent 429s, 200=%d 429=%d", got200.Load(), got429.Load())
	}
	if got200.Load() != 2 {
		t.Fatalf("expected exactly 2 allowed in-flight, got 200=%d", got200.Load())
	}
}

func failOpenCount() int64 {
	v := expvar.Get("ratelimit_fail_open_total")
	if v == nil {
		return 0
	}
	iv, ok := v.(*expvar.Int)
	if !ok {
		return 0
	}
	return iv.Value()
}

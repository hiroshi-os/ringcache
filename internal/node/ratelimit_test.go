package node_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hiroshi-os/ringcache/internal/node"
)

func TestRateLimitPrimaryOnlyAndForward(t *testing.T) {
	placeholder := map[string]string{
		"a": "http://127.0.0.1:1",
		"b": "http://127.0.0.1:2",
		"c": "http://127.0.0.1:3",
	}
	var srvs []*httptest.Server
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

	key := "rl:primary-test"
	// Discover primary via /ring.
	res, err := http.Get(srvs[0].URL + "/ring?key=" + key)
	if err != nil {
		t.Fatal(err)
	}
	var ring struct {
		Owners []string `json:"owners"`
	}
	json.NewDecoder(res.Body).Decode(&ring)
	res.Body.Close()
	if len(ring.Owners) == 0 {
		t.Fatal("no owners")
	}
	primary := ring.Owners[0]

	body, _ := json.Marshal(map[string]any{
		"key": key, "rate": 10, "burst": 3, "cost": 1,
	})
	// Hit a non-primary coordinator — should forward and still enforce burst=3.
	coord := srvs[0]
	for i, id := range []string{"a", "b", "c"} {
		if id != primary {
			coord = srvs[i]
			break
		}
	}
	allowed := 0
	var lastServed string
	for i := 0; i < 5; i++ {
		res, err := http.Post(coord.URL+"/v1/ratelimit/take", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("status %d %s", res.StatusCode, raw)
		}
		var out struct {
			Allowed  bool   `json:"allowed"`
			ServedBy string `json:"served_by"`
			Primary  string `json:"primary"`
		}
		json.Unmarshal(raw, &out)
		if out.Primary != primary {
			t.Fatalf("primary=%s want %s", out.Primary, primary)
		}
		lastServed = out.ServedBy
		if out.Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed=%d want 3 (burst)", allowed)
	}
	if lastServed != primary {
		t.Fatalf("served_by=%s want primary %s", lastServed, primary)
	}
}

package node

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func startTestNode(t *testing.T, id string, peers map[string]string) (*httptest.Server, *Node) {
	t.Helper()
	n, err := New(Config{
		ID:             id,
		Listen:         "test",
		Peers:          peers,
		Replicas:       2,
		Capacity:       64,
		VNodes:         32,
		ReplicaTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(func() {
		srv.Close()
		n.Close()
	})
	return srv, n
}

func TestReplicatedSetGetAcrossCoordinators(t *testing.T) {
	peers := map[string]string{
		"a": "placeholder",
		"b": "placeholder",
		"c": "placeholder",
	}
	sa, _ := startTestNode(t, "a", peers)
	sb, _ := startTestNode(t, "b", peers)
	sc, _ := startTestNode(t, "c", peers)
	// All nodes share this map (Config.Peers is not cloned).
	peers["a"], peers["b"], peers["c"] = sa.URL, sb.URL, sc.URL

	body, _ := json.Marshal(map[string]any{"key": "k1", "value": "v1", "ttl_ms": 5000})
	res, err := http.Post(sa.URL+"/v1/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("set status %d %s", res.StatusCode, raw)
	}
	var wr writeResp
	if err := json.Unmarshal(raw, &wr); err != nil {
		t.Fatal(err)
	}
	if wr.Acked < 1 {
		t.Fatalf("acked=%d failed=%v", wr.Acked, wr.Failed)
	}

	// GET from a node that may not be the coordinator.
	res, err = http.Get(sc.URL + "/v1/get?key=k1")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("get status %d %s", res.StatusCode, raw)
	}
	var g getResp
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if !g.Found || g.Value != "v1" {
		t.Fatalf("get %#v", g)
	}
}

func TestDeleteRemovesReplicas(t *testing.T) {
	peers := map[string]string{"a": "", "b": "", "c": ""}
	sa, _ := startTestNode(t, "a", peers)
	sb, _ := startTestNode(t, "b", peers)
	sc, _ := startTestNode(t, "c", peers)
	peers["a"], peers["b"], peers["c"] = sa.URL, sb.URL, sc.URL

	body, _ := json.Marshal(map[string]any{"key": "gone", "value": "x", "ttl_ms": 0})
	res, err := http.Post(sb.URL+"/v1/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("set %d", res.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodDelete, sa.URL+"/v1/delete?key=gone", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	res, err = http.Get(sc.URL + "/v1/get?key=gone")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusNotFound && res.StatusCode != http.StatusOK {
		t.Fatalf("unexpected %d", res.StatusCode)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var g getResp
	_ = json.Unmarshal(raw, &g)
	if g.Found {
		t.Fatalf("deleted key still found: %s", raw)
	}
}

package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func startTestNode(t *testing.T, id string, peers map[string]string) (*httptest.Server, *Node) {
	t.Helper()
	copied := make(map[string]string, len(peers))
	for k, v := range peers {
		copied[k] = v
	}
	n, err := New(Config{
		ID:             id,
		Listen:         "test",
		Peers:          copied,
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

func startCluster(t *testing.T) (map[string]string, map[string]*Node) {
	t.Helper()
	placeholder := map[string]string{
		"a": "http://127.0.0.1:1",
		"b": "http://127.0.0.1:2",
		"c": "http://127.0.0.1:3",
	}
	sa, na := startTestNode(t, "a", placeholder)
	sb, nb := startTestNode(t, "b", placeholder)
	sc, nc := startTestNode(t, "c", placeholder)
	urls := map[string]string{"a": sa.URL, "b": sb.URL, "c": sc.URL}
	nodes := map[string]*Node{"a": na, "b": nb, "c": nc}
	for _, n := range nodes {
		for id, u := range urls {
			if err := n.join(id, u); err != nil {
				t.Fatal(err)
			}
		}
	}
	return urls, nodes
}

func pickKeyWithOwner(t *testing.T, n *Node, want string) (string, []string) {
	t.Helper()
	for i := 0; i < 4000; i++ {
		k := fmt.Sprintf("pick:%d", i)
		owners := n.ring.Owners(k, n.cfg.Replicas)
		for _, o := range owners {
			if o == want {
				return k, owners
			}
		}
	}
	t.Fatalf("no key owned by %s", want)
	return "", nil
}

func TestReplicatedSetGetAcrossCoordinators(t *testing.T) {
	urls, _ := startCluster(t)

	body, _ := json.Marshal(map[string]any{"key": "k1", "value": "v1", "ttl_ms": 5000})
	res, err := http.Post(urls["a"]+"/v1/set", "application/json", bytes.NewReader(body))
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

	res, err = http.Get(urls["c"] + "/v1/get?key=k1")
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
	urls, _ := startCluster(t)

	body, _ := json.Marshal(map[string]any{"key": "gone", "value": "x", "ttl_ms": 0})
	res, err := http.Post(urls["b"]+"/v1/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("set %d", res.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodDelete, urls["a"]+"/v1/delete?key=gone", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	res, err = http.Get(urls["c"] + "/v1/get?key=gone")
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

func TestSetPartialAckWhenOwnerDown(t *testing.T) {
	placeholder := map[string]string{
		"a": "http://127.0.0.1:1",
		"b": "http://127.0.0.1:2",
		"c": "http://127.0.0.1:3",
	}
	sa, na := startTestNode(t, "a", placeholder)
	sb, nb := startTestNode(t, "b", placeholder)
	sc, nc := startTestNode(t, "c", placeholder)
	for _, n := range []*Node{na, nb, nc} {
		_ = n.join("a", sa.URL)
		_ = n.join("b", sb.URL)
		_ = n.join("c", sc.URL)
	}
	sb.Close()

	key, owners := pickKeyWithOwner(t, na, "b")
	body, _ := json.Marshal(map[string]any{"key": key, "value": "partial", "ttl_ms": 0})
	res, err := http.Post(sa.URL+"/v1/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("set status %d %s (owners=%v)", res.StatusCode, raw, owners)
	}
	var wr writeResp
	if err := json.Unmarshal(raw, &wr); err != nil {
		t.Fatal(err)
	}
	if wr.Acked < 1 {
		t.Fatalf("acked=%d (success is acked>=1) failed=%v", wr.Acked, wr.Failed)
	}
	sawB := false
	for _, id := range wr.Failed {
		if id == "b" {
			sawB = true
		}
	}
	if !sawB {
		t.Fatalf("expected owner b in failed, got acked=%d failed=%v owners=%v", wr.Acked, wr.Failed, owners)
	}
}

func TestJoinLeaveUpdatesOwners(t *testing.T) {
	n, err := New(Config{
		ID:             "a",
		Listen:         "test",
		Peers:          map[string]string{"a": "http://127.0.0.1:1", "b": "http://127.0.0.1:2"},
		Replicas:       2,
		Capacity:       8,
		VNodes:         32,
		ReplicaTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	const samples = 400
	before := make([]string, samples)
	for i := 0; i < samples; i++ {
		before[i] = n.ring.Primary(fmt.Sprintf("m:%d", i))
	}
	if err := n.join("c", "http://127.0.0.1:3"); err != nil {
		t.Fatal(err)
	}
	changed := 0
	for i := 0; i < samples; i++ {
		if n.ring.Primary(fmt.Sprintf("m:%d", i)) != before[i] {
			changed++
		}
	}
	if changed == 0 {
		t.Fatal("join did not remap any primaries")
	}
	if err := n.leave("c"); err != nil {
		t.Fatal(err)
	}
	if err := n.leave("a"); err == nil {
		t.Fatal("self leave should fail")
	}
	reverted := 0
	for i := 0; i < samples; i++ {
		if n.ring.Primary(fmt.Sprintf("m:%d", i)) == before[i] {
			reverted++
		}
	}
	if reverted != samples {
		t.Fatalf("leave did not restore primaries: %d/%d", reverted, samples)
	}
}

func TestAdminMembersHTTP(t *testing.T) {
	n, err := New(Config{
		ID:             "a",
		Listen:         "test",
		Peers:          map[string]string{"a": "http://127.0.0.1:8080"},
		Replicas:       1,
		Capacity:       8,
		VNodes:         8,
		ReplicaTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(func() { srv.Close(); n.Close() })

	body, _ := json.Marshal(map[string]string{"id": "b", "url": "http://127.0.0.1:8081"})
	res, err := http.Post(srv.URL+"/admin/members", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("join %d %s", res.StatusCode, raw)
	}
	res, err = http.Get(srv.URL + "/admin/members")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if !bytes.Contains(raw, []byte(`"b"`)) {
		t.Fatalf("members after join: %s", raw)
	}
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/admin/members?id=b", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("leave %d", res.StatusCode)
	}
	if n.ring.Size() != 1 {
		t.Fatalf("size=%d", n.ring.Size())
	}
}

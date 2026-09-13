// Package node is one ringcache process: local store + hash ring + HTTP.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hiroshi-os/ringcache/internal/ring"
	"github.com/hiroshi-os/ringcache/internal/store"
)

const (
	maxKeyBytes   = 512
	maxValueBytes = 1 << 20
)

// Config is process configuration. Peers maps node ID → base URL
// (e.g. "node-a" → "http://127.0.0.1:8080"). The local ID must be present.
type Config struct {
	ID             string
	Listen         string
	Peers          map[string]string
	Replicas       int
	Capacity       int
	VNodes         int
	ReplicaTimeout time.Duration
	BreakFor       time.Duration
}

// Node is a cache member.
type Node struct {
	cfg    Config
	ring   *ring.Ring
	store  *store.Store
	client *http.Client
	log    *log.Logger

	mu        sync.Mutex
	downUntil map[string]time.Time
	peers     map[string]string
}

// New builds a node. The store janitor starts immediately.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node id required")
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.Replicas < 1 {
		cfg.Replicas = 1
	}
	if cfg.Capacity < 1 {
		cfg.Capacity = 1024
	}
	if cfg.VNodes < 1 {
		cfg.VNodes = 150
	}
	if cfg.ReplicaTimeout <= 0 {
		cfg.ReplicaTimeout = 200 * time.Millisecond
	}
	if cfg.BreakFor <= 0 {
		cfg.BreakFor = 2 * time.Second
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("peers must include self %q", cfg.ID)
	}
	if cfg.Replicas > len(cfg.Peers) {
		cfg.Replicas = len(cfg.Peers)
	}
	peers := make(map[string]string, len(cfg.Peers))
	ids := make([]string, 0, len(cfg.Peers))
	for id, u := range cfg.Peers {
		peers[id] = u
		ids = append(ids, id)
	}
	sort.Strings(ids)
	r := ring.New(cfg.VNodes)
	for _, id := range ids {
		r.Add(id)
	}
	return &Node{
		cfg:       cfg,
		ring:      r,
		store:     store.New(cfg.Capacity),
		client:    &http.Client{Timeout: cfg.ReplicaTimeout},
		log:       log.New(log.Writer(), "["+cfg.ID+"] ", log.LstdFlags|log.Lmicroseconds),
		downUntil: make(map[string]time.Time),
		peers:     peers,
	}, nil
}

// Close stops the local store janitor.
func (n *Node) Close() {
	n.store.Close()
}

// Handler returns the HTTP mux.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/stats", n.handleStats)
	mux.HandleFunc("/ring", n.handleRing)
	mux.HandleFunc("/v1/get", n.handleGet)
	mux.HandleFunc("/get", n.handleGet)
	mux.HandleFunc("/v1/set", n.handleSet)
	mux.HandleFunc("/set", n.handleSet)
	mux.HandleFunc("/v1/delete", n.handleDelete)
	mux.HandleFunc("/delete", n.handleDelete)
	mux.HandleFunc("/internal/kv", n.handleInternalKV)
	mux.HandleFunc("/admin/members", n.handleMembers)
	return mux
}

type kvBody struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	TTLMs     int64  `json:"ttl_ms"`
	WrittenAt int64  `json:"written_at,omitempty"`
}

type getResp struct {
	Key            string `json:"key"`
	Found          bool   `json:"found"`
	Value          string `json:"value,omitempty"`
	ServedBy       string `json:"served_by,omitempty"`
	TTLRemainingMs *int64 `json:"ttl_remaining_ms,omitempty"`
	WrittenAt      int64  `json:"written_at,omitempty"`
}

type writeResp struct {
	Key      string   `json:"key"`
	Acked    int      `json:"acked"`
	Replicas []string `json:"replicas"`
	Failed   []string `json:"failed"`
}

type statsResp struct {
	ID       string      `json:"id"`
	Listen   string      `json:"listen"`
	Replicas int         `json:"replicas"`
	VNodes   int         `json:"vnodes_per_node"`
	Store    store.Stats `json:"store"`
}

type ringResp struct {
	ID       string            `json:"id"`
	Nodes    []string          `json:"nodes"`
	Peers    map[string]string `json:"peers"`
	VNodes   int               `json:"vnodes_per_node"`
	RingLen  int               `json:"ring_len"`
	Replicas int               `json:"replicas"`
	Key      string            `json:"key,omitempty"`
	Owners   []string          `json:"owners,omitempty"`
	Formula  string            `json:"vnode_formula"`
	Hash     string            `json:"hash"`
}

func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": n.cfg.ID})
}

func (n *Node) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, statsResp{
		ID:       n.cfg.ID,
		Listen:   n.cfg.Listen,
		Replicas: n.cfg.Replicas,
		VNodes:   n.cfg.VNodes,
		Store:    n.store.Stats(),
	})
}

func (n *Node) handleRing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	resp := ringResp{
		ID:       n.cfg.ID,
		Nodes:    n.ring.Nodes(),
		Peers:    n.snapshotPeers(),
		VNodes:   n.cfg.VNodes,
		RingLen:  n.ring.Len(),
		Replicas: n.cfg.Replicas,
		Formula:  `hash32(id + "#" + i)  // FNV-1a, i in [0, V)`,
		Hash:     "fnv-1a-32",
	}
	if key := r.URL.Query().Get("key"); key != "" {
		resp.Key = key
		resp.Owners = n.ring.Owners(key, n.cfg.Replicas)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if err := validateKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	owners := n.ring.Owners(key, n.cfg.Replicas)
	if len(owners) == 0 {
		http.Error(w, "empty ring", http.StatusServiceUnavailable)
		return
	}

	// Prefer local if we own the key, then walk owners (skip recently-down peers).
	order := preferLocal(owners, n.cfg.ID)
	var reached bool
	for _, id := range order {
		ent, ok, err := n.readOwner(r.Context(), id, key)
		if err != nil {
			n.markDown(id)
			n.log.Printf("get %q via %s: %v", key, id, err)
			continue
		}
		reached = true
		if !ok {
			continue
		}
		// Best-effort read repair: refill owners that missed before this hit.
		n.readRepair(key, ent, owners, id)
		writeJSON(w, http.StatusOK, getPayload(key, id, ent))
		return
	}
	if !reached {
		http.Error(w, "all replicas unreachable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusNotFound, getResp{Key: key, Found: false})
}

func (n *Node) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	var body kvBody
	if err := readJSON(r, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateKey(body.Key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateValue(body.Value); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if body.TTLMs < 0 {
		http.Error(w, "ttl_ms must be >= 0", http.StatusBadRequest)
		return
	}
	if body.WrittenAt == 0 {
		body.WrittenAt = time.Now().UnixNano()
	}
	owners := n.ring.Owners(body.Key, n.cfg.Replicas)
	acked, failed := n.fanout(r.Context(), http.MethodPut, owners, body)
	status := http.StatusOK
	if acked == 0 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, writeResp{Key: body.Key, Acked: acked, Replicas: owners, Failed: emptyIfNil(failed)})
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if err := validateKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	owners := n.ring.Owners(key, n.cfg.Replicas)
	body := kvBody{Key: key}
	acked, failed := n.fanout(r.Context(), http.MethodDelete, owners, body)
	status := http.StatusOK
	if acked == 0 && len(owners) > 0 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, writeResp{Key: key, Acked: acked, Replicas: owners, Failed: emptyIfNil(failed)})
}

func (n *Node) handleInternalKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		key := r.URL.Query().Get("key")
		if err := validateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ent, ok := n.store.Get(key)
		if !ok {
			writeJSON(w, http.StatusNotFound, getResp{Key: key, Found: false})
			return
		}
		writeJSON(w, http.StatusOK, getPayload(key, n.cfg.ID, ent))
	case http.MethodPut:
		var body kvBody
		if err := readJSON(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := validateKey(body.Key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ttl := time.Duration(body.TTLMs) * time.Millisecond
		applied := n.store.Set(body.Key, body.Value, ttl, body.WrittenAt)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "applied": applied, "id": n.cfg.ID})
	case http.MethodDelete:
		key := r.URL.Query().Get("key")
		if err := validateKey(key); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ok := n.store.Delete(key)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": ok, "id": n.cfg.ID})
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

func (n *Node) readOwner(ctx context.Context, id, key string) (store.Entry, bool, error) {
	if id == n.cfg.ID {
		ent, ok := n.store.Get(key)
		return ent, ok, nil
	}
	if n.isDown(id) {
		return store.Entry{}, false, fmt.Errorf("%s marked down", id)
	}
	base := n.peerURL(id)
	if base == "" {
		return store.Entry{}, false, fmt.Errorf("unknown peer %s", id)
	}
	url := strings.TrimRight(base, "/") + "/internal/kv?key=" + urlQuery(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return store.Entry{}, false, err
	}
	res, err := n.client.Do(req)
	if err != nil {
		return store.Entry{}, false, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return store.Entry{}, false, nil
	}
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return store.Entry{}, false, fmt.Errorf("status %d %s", res.StatusCode, bytes.TrimSpace(b))
	}
	var g getResp
	if err := json.NewDecoder(res.Body).Decode(&g); err != nil {
		return store.Entry{}, false, err
	}
	if !g.Found {
		return store.Entry{}, false, nil
	}
	ent := store.Entry{Value: g.Value, WrittenAt: g.WrittenAt}
	if g.TTLRemainingMs != nil && *g.TTLRemainingMs > 0 {
		ent.ExpiresAt = time.Now().Add(time.Duration(*g.TTLRemainingMs) * time.Millisecond)
	}
	return ent, true, nil
}

func (n *Node) fanout(ctx context.Context, method string, owners []string, body kvBody) (acked int, failed []string) {
	type result struct {
		id  string
		err error
	}
	ch := make(chan result, len(owners))
	for _, id := range owners {
		id := id
		go func() {
			ch <- result{id: id, err: n.writeOwner(ctx, method, id, body)}
		}()
	}
	for i := 0; i < len(owners); i++ {
		res := <-ch
		if res.err != nil {
			n.markDown(res.id)
			n.log.Printf("%s %q via %s: %v", method, body.Key, res.id, res.err)
			failed = append(failed, res.id)
			continue
		}
		acked++
	}
	sort.Strings(failed)
	return acked, failed
}

func (n *Node) writeOwner(ctx context.Context, method, id string, body kvBody) error {
	// Writes always attempt the peer. A startup blip must not hide a replica
	// behind the read-side breaker for the next two seconds.
	if id == n.cfg.ID {
		switch method {
		case http.MethodPut:
			ttl := time.Duration(body.TTLMs) * time.Millisecond
			n.store.Set(body.Key, body.Value, ttl, body.WrittenAt)
			return nil
		case http.MethodDelete:
			n.store.Delete(body.Key)
			return nil
		default:
			return fmt.Errorf("bad method %s", method)
		}
	}
	base := strings.TrimRight(n.peerURL(id), "/")
	if base == "" {
		return fmt.Errorf("unknown peer %s", id)
	}
	var req *http.Request
	var err error
	if method == http.MethodDelete {
		req, err = http.NewRequestWithContext(ctx, http.MethodDelete, base+"/internal/kv?key="+urlQuery(body.Key), nil)
	} else {
		raw, mErr := json.Marshal(body)
		if mErr != nil {
			return mErr
		}
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, base+"/internal/kv", bytes.NewReader(raw))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	if err != nil {
		return err
	}
	res, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("status %d %s", res.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}

func (n *Node) readRepair(key string, ent store.Entry, owners []string, servedBy string) {
	ttlMs := int64(0)
	if !ent.ExpiresAt.IsZero() {
		rem := time.Until(ent.ExpiresAt).Milliseconds()
		if rem <= 0 {
			return
		}
		ttlMs = rem
	}
	body := kvBody{Key: key, Value: ent.Value, TTLMs: ttlMs, WrittenAt: ent.WrittenAt}
	for _, id := range owners {
		if id == servedBy {
			continue
		}
		id := id
		go func() {
			// Per-goroutine timeout: the parent must not cancel when handleGet
			// returns (the previous shared ctx was canceled immediately).
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ReplicaTimeout)
			defer cancel()
			if err := n.writeOwner(ctx, http.MethodPut, id, body); err != nil {
				n.log.Printf("read-repair %q → %s: %v", key, id, err)
			}
		}()
	}
}

func (n *Node) handleMembers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"id":    n.cfg.ID,
			"nodes": n.ring.Nodes(),
			"peers": n.snapshotPeers(),
		})
	case http.MethodPut, http.MethodPost:
		var body struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		}
		if err := readJSON(r, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := n.join(body.ID, body.URL); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":    true,
			"id":    n.cfg.ID,
			"nodes": n.ring.Nodes(),
			"peers": n.snapshotPeers(),
		})
	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if err := n.leave(id); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":    true,
			"id":    n.cfg.ID,
			"nodes": n.ring.Nodes(),
			"peers": n.snapshotPeers(),
		})
	default:
		http.Error(w, "method", http.StatusMethodNotAllowed)
	}
}

// join adds a physical node to this process's ring and peer map.
// Membership is not gossip: every live node must be told (see scripts/rebalance-demo.sh).
func (n *Node) join(id, rawURL string) error {
	id = strings.TrimSpace(id)
	rawURL = strings.TrimSpace(rawURL)
	if id == "" || rawURL == "" {
		return errors.New("id and url required")
	}
	if _, err := url.ParseRequestURI(rawURL); err != nil {
		return fmt.Errorf("url: %w", err)
	}
	n.mu.Lock()
	n.peers[id] = rawURL
	n.mu.Unlock()
	n.ring.Add(id)
	return nil
}

// leave removes a physical node from this process's ring. Self cannot leave.
func (n *Node) leave(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("id required")
	}
	if id == n.cfg.ID {
		return errors.New("cannot remove self")
	}
	n.mu.Lock()
	delete(n.peers, id)
	delete(n.downUntil, id)
	n.mu.Unlock()
	n.ring.Remove(id)
	return nil
}

func (n *Node) snapshotPeers() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]string, len(n.peers))
	for k, v := range n.peers {
		out[k] = v
	}
	return out
}

func (n *Node) peerURL(id string) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.peers[id]
}

func (n *Node) markDown(id string) {
	if id == n.cfg.ID {
		return
	}
	n.mu.Lock()
	n.downUntil[id] = time.Now().Add(n.cfg.BreakFor)
	n.mu.Unlock()
}

func (n *Node) isDown(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	until, ok := n.downUntil[id]
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	delete(n.downUntil, id)
	return false
}

func preferLocal(owners []string, self string) []string {
	out := make([]string, 0, len(owners))
	for _, id := range owners {
		if id == self {
			out = append(out, id)
		}
	}
	for _, id := range owners {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

func getPayload(key, servedBy string, ent store.Entry) getResp {
	g := getResp{
		Key:       key,
		Found:     true,
		Value:     ent.Value,
		ServedBy:  servedBy,
		WrittenAt: ent.WrittenAt,
	}
	if !ent.ExpiresAt.IsZero() {
		ms := time.Until(ent.ExpiresAt).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		g.TTLRemainingMs = &ms
	}
	return g
}

func validateKey(key string) error {
	if key == "" {
		return errors.New("key required")
	}
	if len(key) > maxKeyBytes {
		return fmt.Errorf("key exceeds %d bytes", maxKeyBytes)
	}
	if strings.ContainsRune(key, 0) {
		return errors.New("key must not contain NUL")
	}
	return nil
}

func validateValue(v string) error {
	if len(v) > maxValueBytes {
		return fmt.Errorf("value exceeds %d bytes", maxValueBytes)
	}
	return nil
}

func urlQuery(key string) string {
	return url.QueryEscape(key)
}

func readJSON(r *http.Request, dst any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, maxValueBytes+4096))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func emptyIfNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hiroshi-os/ringcache/internal/node"
)

func main() {
	cfg, err := parseConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	n, err := node.New(cfg)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	defer n.Close()

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           n.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("ringcache %s listening on %s replicas=%d capacity=%d vnodes=%d peers=%d",
			cfg.ID, cfg.Listen, cfg.Replicas, cfg.Capacity, cfg.VNodes, len(cfg.Peers))
		errCh <- srv.ListenAndServe()
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	case s := <-sig:
		log.Printf("shutdown (%s)", s)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

func parseConfig() (node.Config, error) {
	var (
		id       = flag.String("id", env("RINGCACHE_ID", "node-a"), "physical node id")
		listen   = flag.String("listen", env("RINGCACHE_LISTEN", ":8080"), "listen address")
		peers    = flag.String("peers", env("RINGCACHE_PEERS", ""), "comma-separated id=url (must include self)")
		replicas = flag.Int("replicas", envInt("RINGCACHE_REPLICAS", 2), "replication factor R")
		capacity = flag.Int("capacity", envInt("RINGCACHE_CAPACITY", 10000), "max keys in the local LRU")
		vnodes   = flag.Int("vnodes", envInt("RINGCACHE_VNODES", 150), "virtual nodes per physical node")
		timeout  = flag.Int("replica-timeout-ms", envInt("RINGCACHE_REPLICA_TIMEOUT_MS", 200), "peer RPC timeout")
	)
	flag.Parse()

	peerMap, err := parsePeers(*peers)
	if err != nil {
		return node.Config{}, err
	}
	if len(peerMap) == 0 {
		// Single-node default so `go run ./cmd/ringcache` works.
		peerMap = map[string]string{*id: "http://127.0.0.1" + normalizeListen(*listen)}
	}
	return node.Config{
		ID:             *id,
		Listen:         *listen,
		Peers:          peerMap,
		Replicas:       *replicas,
		Capacity:       *capacity,
		VNodes:         *vnodes,
		ReplicaTimeout: time.Duration(*timeout) * time.Millisecond,
	}, nil
}

func parsePeers(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return map[string]string{}, nil
	}
	out := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, url, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(url) == "" {
			return nil, fmt.Errorf("peer %q: want id=url", part)
		}
		out[strings.TrimSpace(id)] = strings.TrimSpace(url)
	}
	return out, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func normalizeListen(listen string) string {
	if strings.HasPrefix(listen, ":") {
		return listen
	}
	if i := strings.LastIndex(listen, ":"); i >= 0 {
		return listen[i:]
	}
	return ":8080"
}

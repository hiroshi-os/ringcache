#!/usr/bin/env bash
# Accuracy + latency measurements for pkg/ratelimit. Numbers go to stdout;
# copy measured lines into bench/RESULTS.md (do not invent figures).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DURATION="${DURATION:-60s}"
TARGET="${TARGET:-100}"
BURST="${BURST:-20}"

if ! curl -sf http://127.0.0.1:8080/health >/dev/null 2>&1; then
  ./scripts/run-cluster.sh
fi

mkdir -p bin
go build -o bin/ratebench ./cmd/ratebench

echo "=== hardware ==="
uname -a 2>/dev/null || true
date -u +%Y-%m-%dT%H:%M:%SZ
go version
git rev-parse HEAD

echo "=== baseline (no limiter) ==="
./bin/ratebench -target "$TARGET" -burst "$BURST" -mult 2 -duration "$DURATION" -baseline -timeout 50ms

echo "=== 2x offer ==="
./bin/ratebench -target "$TARGET" -burst "$BURST" -mult 2 -duration "$DURATION" -timeout 50ms

echo "=== 5x offer ==="
./bin/ratebench -target "$TARGET" -burst "$BURST" -mult 5 -duration "$DURATION" -timeout 50ms

echo "=== kill primary mid-run (30s, kill at 10s) ==="
./bin/ratebench -target "$TARGET" -burst "$BURST" -mult 2 -duration 30s -kill-after 10s -timeout 50ms

echo "done — paste measured lines into bench/RESULTS.md"

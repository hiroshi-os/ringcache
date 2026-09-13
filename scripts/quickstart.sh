#!/usr/bin/env bash
# 60-second path: build, start 3 nodes, SET, GET.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> 3-node cluster on 127.0.0.1:8080-8082 (R=2, V=150)"
./scripts/run-cluster.sh

echo
echo "==> SET user:1 via node-a (200 means acked≥1, not a quorum)"
curl -sS --max-time 3 -X PUT http://127.0.0.1:8080/v1/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1","value":"ok","ttl_ms":60000}'
echo

echo
echo "==> GET via node-b (any node coordinates; first live owner wins)"
curl -sS --max-time 3 'http://127.0.0.1:8081/v1/get?key=user:1'
echo

echo
echo "==> owners for user:1"
curl -sS --max-time 3 'http://127.0.0.1:8080/ring?key=user:1'
echo

echo
echo "Up. Next (optional):"
echo "  ./scripts/failure-demo.sh      # kill a replica; then partial-ack miss"
echo "  ./scripts/rebalance-demo.sh    # join/leave ~1/N remap"
echo "  make stop"

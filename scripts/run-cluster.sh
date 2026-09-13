#!/usr/bin/env bash
# Start a 3-node cluster on 127.0.0.1:8080-8082 (no Docker required).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p bin
if [[ ! -x bin/ringcache ]]; then
  go build -o bin/ringcache ./cmd/ringcache
fi

./scripts/stop-cluster.sh 2>/dev/null || true

PEERS="node-a=http://127.0.0.1:8080,node-b=http://127.0.0.1:8081,node-c=http://127.0.0.1:8082"
common=(
  -replicas 2
  -capacity 10000
  -vnodes 150
  -replica-timeout-ms 200
  -peers "$PEERS"
)

./bin/ringcache -id node-a -listen 127.0.0.1:8080 "${common[@]}" > /tmp/ringcache-a.log 2>&1 &
echo $! > /tmp/ringcache-a.pid
./bin/ringcache -id node-b -listen 127.0.0.1:8081 "${common[@]}" > /tmp/ringcache-b.log 2>&1 &
echo $! > /tmp/ringcache-b.pid
./bin/ringcache -id node-c -listen 127.0.0.1:8082 "${common[@]}" > /tmp/ringcache-c.log 2>&1 &
echo $! > /tmp/ringcache-c.pid

ok=0
for i in $(seq 1 50); do
  if curl -sf http://127.0.0.1:8080/health >/dev/null \
    && curl -sf http://127.0.0.1:8081/health >/dev/null \
    && curl -sf http://127.0.0.1:8082/health >/dev/null; then
    ok=1
    break
  fi
  sleep 0.1
done
if [[ "$ok" -ne 1 ]]; then
  echo "cluster failed to become healthy" >&2
  tail -n 20 /tmp/ringcache-a.log /tmp/ringcache-b.log /tmp/ringcache-c.log >&2 || true
  exit 1
fi
echo "cluster up: http://127.0.0.1:8080  :8081  :8082"
echo "pids: $(cat /tmp/ringcache-a.pid) $(cat /tmp/ringcache-b.pid) $(cat /tmp/ringcache-c.pid)"

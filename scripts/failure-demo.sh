#!/usr/bin/env bash
# Kill one node and show that a replicated key is still readable.
#
# Real behavior (not a guarantee of "HA"):
#   - R=2, N=3: every key has two owners. Killing ONE node leaves at least
#     one replica IF the original SET was acked by both owners.
#   - If SET only acked the node you then kill, GET returns 404/503.
#   - Membership is static: the dead node stays on the ring; peers skip it
#     for ~2s via a fail-open breaker, then retry and time out again.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

USE_COMPOSE=0
if [[ "${1:-}" == "--compose" ]]; then
  USE_COMPOSE=1
elif command -v docker >/dev/null 2>&1 && docker compose ps --status running 2>/dev/null | grep -q node-a; then
  USE_COMPOSE=1
fi

if [[ "$USE_COMPOSE" -eq 0 ]]; then
  if ! curl -sf http://127.0.0.1:8080/health >/dev/null 2>&1; then
    ./scripts/run-cluster.sh
  fi
fi

# Pick a key whose owner set includes node-b so killing that process is a
# real replica loss, not a no-op on a key that never lived there.
KEY=""
for i in $(seq 1 400); do
  cand="demo:failure:$i"
  owners="$(curl -sf "http://127.0.0.1:8080/ring?key=$cand" || true)"
  if echo "$owners" | grep -q '"node-b"'; then
    KEY="$cand"
    break
  fi
done
if [[ -z "$KEY" ]]; then
  echo "could not find a key owned by node-b" >&2
  exit 1
fi

echo "=== 1. SET via node-a (key=$KEY, chosen so node-b is an owner) ==="
curl -sS -X PUT http://127.0.0.1:8080/v1/set \
  -H 'Content-Type: application/json' \
  -d "{\"key\":\"$KEY\",\"value\":\"still-here\",\"ttl_ms\":60000}"
echo

echo "=== 2. owners ==="
curl -sS "http://127.0.0.1:8080/ring?key=$KEY"
echo

echo "=== 3. GET from all live nodes ==="
for p in 8080 8081 8082; do
  echo -n ":$p "
  curl -sS "http://127.0.0.1:$p/v1/get?key=$KEY" || echo "(down)"
  echo
done

echo "=== 4. kill node-b ==="
if [[ "$USE_COMPOSE" -eq 1 ]]; then
  docker compose stop node-b
else
  if [[ -f /tmp/ringcache-b.pid ]]; then
    kill "$(cat /tmp/ringcache-b.pid)" || true
    rm -f /tmp/ringcache-b.pid
  else
    echo "no node-b pid; cannot kill" >&2
    exit 1
  fi
fi
sleep 0.3

echo "=== 5. GET after node-b is down (expect a hit if SET acked ≥1 surviving replica) ==="
hit=0
for p in 8080 8082; do
  echo -n ":$p "
  body="$(curl -sS -w '\n%{http_code}' "http://127.0.0.1:$p/v1/get?key=$KEY" || true)"
  echo "$body"
  if echo "$body" | grep -q '"found":true'; then
    hit=1
  fi
done

echo "=== 6. GET node-b (should fail to connect) ==="
if curl -sf --max-time 1 "http://127.0.0.1:8081/health"; then
  echo "node-b still healthy (unexpected)"
else
  echo "node-b unreachable (expected)"
fi

if [[ "$hit" -eq 1 ]]; then
  echo "RESULT: read succeeded after one node death"
  exit 0
fi
echo "RESULT: no live replica had the key (partial write or both owners included node-b and the other failed)" >&2
exit 1

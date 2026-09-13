#!/usr/bin/env bash
# Failure modes, not a "the cluster heals" story.
#
#   A) SET acked by both owners, then kill one replica → GET still hits
#      the surviving owner. The dead id stays on the ring.
#   B) SET while one owner is already down (acked=1), then kill the
#      surviving replica → GET misses. Success is acked≥1, not durability.
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib.sh
source "$ROOT/scripts/lib.sh"
cd "$ROOT"

USE_COMPOSE=0
if [[ "${1:-}" == "--compose" ]]; then
  USE_COMPOSE=1
elif command -v docker >/dev/null 2>&1 && docker_cmd compose ps --status running 2>/dev/null | grep -q node-a; then
  USE_COMPOSE=1
fi

STARTED_CLUSTER=0
kill_node() {
  local id="$1"
  if [[ "$USE_COMPOSE" -eq 1 ]]; then
    docker_cmd compose stop "$id" >/dev/null
  else
    stop_local_node "$id"
  fi
}

start_node() {
  local id="$1"
  if [[ "$USE_COMPOSE" -eq 1 ]]; then
    docker_cmd compose start "$id" >/dev/null
  else
    ensure_binary "$ROOT"
    start_local_node "$ROOT" "$id" "127.0.0.1:$(port_for "$id")"
  fi
}

restore_all() {
  start_node node-a || true
  start_node node-b || true
  start_node node-c || true
  wait_health "8080 8081 8082" 80 || true
}

if [[ "$USE_COMPOSE" -eq 1 ]]; then
  if ! curl -sf --max-time 1 http://127.0.0.1:8080/health >/dev/null 2>&1; then
    echo "starting compose cluster..."
    docker_cmd compose up -d --build
    STARTED_CLUSTER=1
  fi
else
  if ! curl -sf --max-time 1 http://127.0.0.1:8080/health >/dev/null 2>&1; then
    ./scripts/run-cluster.sh
    STARTED_CLUSTER=1
  fi
fi
wait_health "8080 8081 8082" 80

# Always try to bring the 3-node cluster back so a failed scenario is not sticky.
trap restore_all EXIT

echo
echo "############################################################"
echo "# Scenario A — full ACK, then kill one replica (expect HIT)"
echo "############################################################"

picked="$(pick_key_owned_by "node-b" "demo:failure-a" 8080)" || {
  echo "could not find a key whose owners[] include node-b" >&2
  exit 1
}
KEY="${picked%%$'\t'*}"
OWNERS="${picked#*$'\t'}"
echo "key=$KEY owners=[$OWNERS]  (owners[] only; not the cluster node list)"

echo
echo "=== A1. SET via node-a ==="
SETA="$(curl -sS --max-time 3 -X PUT http://127.0.0.1:8080/v1/set \
  -H 'Content-Type: application/json' \
  -d "{\"key\":\"$KEY\",\"value\":\"still-here\",\"ttl_ms\":60000}")"
echo "$SETA"
ACKA="$(printf '%s' "$SETA" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("acked",0))')"
FAILEDA="$(printf '%s' "$SETA" | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin).get("failed") or []))')"
if [[ "$ACKA" -lt 1 ]]; then
  echo "SET failed entirely (acked=$ACKA). Success requires acked≥1." >&2
  exit 1
fi
if [[ ",$FAILEDA," == *",node-a,"* && ",$FAILEDA," == *",node-c,"* ]]; then
  echo "SET only landed on node-b; killing node-b would miss (not scenario A)." >&2
  exit 1
fi

echo
echo "=== A2. GET all live nodes (before kill) ==="
for p in 8080 8081 8082; do
  echo -n "  :$p "
  curl -sS --max-time 2 "http://127.0.0.1:$p/v1/get?key=$KEY" || echo "(down)"
  echo
done

echo
echo "=== A3. kill node-b (replica stays on the ring; peers skip it ~2s) ==="
kill_node node-b
sleep 0.3

echo
echo "=== A4. GET node-a and node-c (expect HIT if a surviving owner acked) ==="
hit=0
for p in 8080 8082; do
  echo -n "  :$p "
  body="$(curl -sS --max-time 2 -o /tmp/ringcache-get-a.json -w '%{http_code}' "http://127.0.0.1:$p/v1/get?key=$KEY" || true)"
  echo "$(cat /tmp/ringcache-get-a.json 2>/dev/null)  HTTP $body"
  if grep -q '"found":true' /tmp/ringcache-get-a.json 2>/dev/null; then
    hit=1
  fi
done

echo
echo "=== A5. GET node-b (expect connect failure) ==="
if curl -sf --max-time 1 "http://127.0.0.1:8081/health"; then
  echo "node-b still healthy (unexpected)" >&2
  exit 1
fi
echo "node-b unreachable (expected)"

if [[ "$hit" -ne 1 ]]; then
  echo "RESULT A: miss after one death (partial write or both owners included the dead node)" >&2
  exit 1
fi
echo "RESULT A: read succeeded after one replica death — not healing, just R=2"

echo
echo "=== A6. restore node-b ==="
start_node node-b
wait_health "8080 8081 8082" 80

echo
echo "############################################################"
echo "# Scenario B — SET while owner down (acked=1), then kill it"
echo "#              (expect MISS). acked≥1 is not durability."
echo "############################################################"

picked="$(pick_key_owned_by "node-a,node-b" "demo:failure-b" 8080)" || {
  echo "could not find a key owned by both node-a and node-b" >&2
  exit 1
}
KEYB="${picked%%$'\t'*}"
OWNERSB="${picked#*$'\t'}"
echo "key=$KEYB owners=[$OWNERSB]"

echo
echo "=== B1. kill node-b first, then SET via node-a ==="
kill_node node-b
sleep 0.2
SETB="$(curl -sS --max-time 3 -X PUT http://127.0.0.1:8080/v1/set \
  -H 'Content-Type: application/json' \
  -d "{\"key\":\"$KEYB\",\"value\":\"only-on-survivor\",\"ttl_ms\":60000}")"
echo "$SETB"
ACKB="$(printf '%s' "$SETB" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("acked",0))')"
FAILEDB="$(printf '%s' "$SETB" | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin).get("failed") or []))')"
if [[ "$ACKB" -lt 1 ]]; then
  echo "SET acked=0; scenario B needs a partial success" >&2
  exit 1
fi
if [[ ",$FAILEDB," != *",node-b,"* ]]; then
  echo "expected node-b in failed[], got failed=[$FAILEDB]" >&2
  exit 1
fi
echo "SET acked=$ACKB failed=[$FAILEDB] — HTTP 200 because acked≥1"

echo
echo "=== B2. kill the surviving owner node-a (the copy we just wrote) ==="
kill_node node-a
sleep 0.3

echo
echo "=== B3. GET via node-c (not an owner; both owners are down) ==="
echo -n "  :8082 "
body="$(curl -sS --max-time 2 -o /tmp/ringcache-get-b.json -w '%{http_code}' "http://127.0.0.1:8082/v1/get?key=$KEYB" || true)"
echo "$(cat /tmp/ringcache-get-b.json 2>/dev/null)  HTTP $body"
if grep -q '"found":true' /tmp/ringcache-get-b.json 2>/dev/null; then
  echo "RESULT B: unexpected hit — both owners should be gone" >&2
  exit 1
fi
echo "RESULT B: miss (404/503). Killing the only replica that acked the write loses the key."

echo
echo "=== B4. restore node-a and node-b ==="
start_node node-a
start_node node-b
wait_health "8080 8081 8082" 80

echo
echo "DONE. Consistency recap:"
echo "  success = acked≥1 (not linearizable, not a quorum)"
echo "  R=2 sync fan-out, last-writer-wins, no persistence"
if [[ "$STARTED_CLUSTER" -eq 1 && "$USE_COMPOSE" -eq 0 ]]; then
  echo "  cluster still running; make stop when finished"
fi

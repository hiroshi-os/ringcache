#!/usr/bin/env bash
# Shared helpers for demo scripts. Source this file; do not execute it.
# Requires: curl, python3.

RINGCACHE_PEERS_DEFAULT="${RINGCACHE_PEERS_DEFAULT:-node-a=http://127.0.0.1:8080,node-b=http://127.0.0.1:8081,node-c=http://127.0.0.1:8082}"

docker_cmd() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    docker "$@"
  elif command -v docker >/dev/null 2>&1 && sudo docker info >/dev/null 2>&1; then
    sudo docker "$@"
  else
    echo "docker is not usable" >&2
    return 1
  fi
}

port_for() {
  case "$1" in
    node-a) echo 8080 ;;
    node-b) echo 8081 ;;
    node-c) echo 8082 ;;
    node-d) echo 8083 ;;
    *) echo "unknown node $1" >&2; return 1 ;;
  esac
}

wait_health() {
  local ports="${1:-8080 8081 8082}"
  local tries="${2:-50}"
  local i p
  for i in $(seq 1 "$tries"); do
    local ok=1
    for p in $ports; do
      if ! curl -sf --max-time 1 "http://127.0.0.1:${p}/health" >/dev/null; then
        ok=0
        break
      fi
    done
    if [[ "$ok" -eq 1 ]]; then
      return 0
    fi
    sleep 0.1
  done
  echo "nodes not healthy on ports: $ports" >&2
  return 1
}

# Print comma-separated owners for key. Parses the owners[] field only —
# grepping the whole /ring body matches the cluster node list and is wrong.
owners_of() {
  local key="$1"
  local port="${2:-8080}"
  curl -sf --max-time 2 "http://127.0.0.1:${port}/ring?key=${key}" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(",".join(d.get("owners") or []))
'
}

# Find a key whose owner set includes every id in WANT (comma-separated).
# Prints: KEY<TAB>OWNERS
pick_key_owned_by() {
  local want="$1"
  local prefix="${2:-demo:key}"
  local port="${3:-8080}"
  python3 - "$want" "$prefix" "$port" <<'PY'
import json, sys, urllib.request
want = [x for x in sys.argv[1].split(",") if x]
prefix = sys.argv[2]
port = sys.argv[3]
need = set(want)
for i in range(1, 801):
    key = f"{prefix}:{i}"
    url = f"http://127.0.0.1:{port}/ring?key={key}"
    try:
        with urllib.request.urlopen(url, timeout=2) as r:
            d = json.load(r)
    except Exception:
        continue
    owners = d.get("owners") or []
    if need.issubset(owners):
        print(f"{key}\t{','.join(owners)}")
        sys.exit(0)
sys.exit(1)
PY
}

json_get() {
  python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(sys.argv[1], ""))' "$1"
}

ensure_binary() {
  local root="$1"
  mkdir -p "$root/bin"
  if [[ ! -x "$root/bin/ringcache" ]]; then
    (cd "$root" && go build -o bin/ringcache ./cmd/ringcache)
  fi
}

start_local_node() {
  local root="$1" id="$2" listen="$3"
  local letter="${id##*-}"
  local pidf="/tmp/ringcache-${letter}.pid"
  local logf="/tmp/ringcache-${letter}.log"
  if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    return 0
  fi
  local peers="${4:-$RINGCACHE_PEERS_DEFAULT}"
  "$root/bin/ringcache" \
    -id "$id" \
    -listen "$listen" \
    -replicas 2 \
    -capacity 10000 \
    -vnodes 150 \
    -replica-timeout-ms 200 \
    -peers "$peers" \
    >"$logf" 2>&1 &
  echo $! >"$pidf"
}

stop_local_node() {
  local id="$1"
  local letter="${id##*-}"
  local pidf="/tmp/ringcache-${letter}.pid"
  if [[ -f "$pidf" ]]; then
    local pid
    pid="$(cat "$pidf")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$pidf"
  fi
}

members_join() {
  local port="$1" id="$2" url="$3"
  curl -sf --max-time 2 -X POST "http://127.0.0.1:${port}/admin/members" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"${id}\",\"url\":\"${url}\"}"
}

members_leave() {
  local port="$1" id="$2"
  curl -sf --max-time 2 -X DELETE "http://127.0.0.1:${port}/admin/members?id=${id}"
}

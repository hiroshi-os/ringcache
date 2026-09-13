#!/usr/bin/env bash
# Join/leave remapping on a live cluster.
#
# Membership is NOT gossip. This script tells every live node about node-d
# via POST /admin/members (and DELETE on leave). The ring itself remaps
# about 1/N primaries — same math as internal/ring tests.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib.sh
source "$ROOT/scripts/lib.sh"
cd "$ROOT"

SAMPLES="${SAMPLES:-2000}"
PEERS4="${RINGCACHE_PEERS_DEFAULT},node-d=http://127.0.0.1:8083"

if ! curl -sf --max-time 1 http://127.0.0.1:8080/health >/dev/null 2>&1; then
  ./scripts/run-cluster.sh
fi
wait_health "8080 8081 8082" 80
ensure_binary "$ROOT"

cleanup() {
  members_leave 8080 node-d >/dev/null 2>&1 || true
  members_leave 8081 node-d >/dev/null 2>&1 || true
  members_leave 8082 node-d >/dev/null 2>&1 || true
  stop_local_node node-d
}
trap cleanup EXIT

echo "=== 1. sample $SAMPLES primaries on the 3-node ring (via node-a /ring?key=) ==="
python3 - "$SAMPLES" /tmp/ringcache-primaries-before.txt <<'PY'
import json, sys, urllib.request
n = int(sys.argv[1])
out = sys.argv[2]
primaries = []
for i in range(n):
    key = f"rebalance:{i}"
    with urllib.request.urlopen(f"http://127.0.0.1:8080/ring?key={key}", timeout=2) as r:
        d = json.load(r)
    owners = d.get("owners") or []
    primaries.append(owners[0] if owners else "")
open(out, "w").write("\n".join(primaries))
print(f"wrote {n} primaries")
PY

BEFORE_NODES="$(curl -sf http://127.0.0.1:8080/admin/members | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin).get("nodes") or []))')"
echo "node-a members: [$BEFORE_NODES]"

echo
echo "=== 2. start node-d on :8083 and join it on node-a/b/c (no gossip) ==="
start_local_node "$ROOT" node-d "127.0.0.1:8083" "$PEERS4"
wait_health "8083" 50
for p in 8080 8081 8082; do
  members_join "$p" node-d "http://127.0.0.1:8083" >/dev/null
done
AFTER_NODES="$(curl -sf http://127.0.0.1:8080/admin/members | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin).get("nodes") or []))')"
echo "node-a members after join: [$AFTER_NODES]"
if [[ "$AFTER_NODES" != *node-d* ]]; then
  echo "join did not stick on node-a" >&2
  exit 1
fi

echo
echo "=== 3. resample primaries — consistent hashing should move ~1/4 ==="
python3 - "$SAMPLES" /tmp/ringcache-primaries-before.txt /tmp/ringcache-primaries-after.txt <<'PY'
import json, sys, urllib.request
n = int(sys.argv[1])
before = open(sys.argv[2]).read().splitlines()
after = []
for i in range(n):
    key = f"rebalance:{i}"
    with urllib.request.urlopen(f"http://127.0.0.1:8080/ring?key={key}", timeout=2) as r:
        d = json.load(r)
    owners = d.get("owners") or []
    after.append(owners[0] if owners else "")
open(sys.argv[3], "w").write("\n".join(after))
changed = sum(1 for a, b in zip(before, after) if a != b)
frac = changed / n
print(f"primaries remapped: {changed}/{n} = {frac:.3f}")
# 3 → 4 nodes: expect ~1/4. Wide band so a noisy hash still passes.
if frac < 0.10 or frac > 0.45:
    raise SystemExit(f"remap fraction {frac:.3f} outside [0.10, 0.45]")
print("within expected band [0.10, 0.45] for N=3 → N=4")
PY

echo
echo "=== 4. leave node-d on a/b/c, stop process, expect primaries to revert ==="
for p in 8080 8081 8082; do
  members_leave "$p" node-d >/dev/null
done
stop_local_node node-d
python3 - "$SAMPLES" /tmp/ringcache-primaries-before.txt <<'PY'
import json, sys, urllib.request
n = int(sys.argv[1])
before = open(sys.argv[2]).read().splitlines()
reverted = 0
for i in range(n):
    key = f"rebalance:{i}"
    with urllib.request.urlopen(f"http://127.0.0.1:8080/ring?key={key}", timeout=2) as r:
        d = json.load(r)
    owners = d.get("owners") or []
    primary = owners[0] if owners else ""
    if i < len(before) and primary == before[i]:
        reverted += 1
frac = reverted / n
print(f"primaries restored: {reverted}/{n} = {frac:.3f}")
if frac < 0.99:
    raise SystemExit("leave did not restore the 3-node placement")
print("leave restored the original 3-node placement")
PY

echo
echo "DONE. Join/leave remaps ~1/N primaries. Keys are not migrated;"
echo "a newly responsible node starts empty (no persistence, no hinted handoff)."

#!/usr/bin/env bash
# Start a 3-node cluster + demo API, then hammer it until 429s appear.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

./scripts/run-cluster.sh
mkdir -p bin
go build -o bin/ratelimit-demo ./examples/ratelimit-demo

pkill -f 'bin/ratelimit-demo' 2>/dev/null || true
./bin/ratelimit-demo -listen 127.0.0.1:9090 -rate 5 -burst 5 > /tmp/ratelimit-demo.log 2>&1 &
echo $! > /tmp/ratelimit-demo.pid
sleep 0.3

echo "=== hammer X-API-Key=alice (rate=5 burst=5) ==="
ok=0
denied=0
for i in $(seq 1 30); do
  code="$(curl -sS -o /tmp/rl-body -w '%{http_code}' -H 'X-API-Key: alice' http://127.0.0.1:9090/api/hello || true)"
  if [[ "$code" == "200" ]]; then ok=$((ok+1)); fi
  if [[ "$code" == "429" ]]; then denied=$((denied+1)); fi
done
echo "200=$ok 429=$denied"
echo "fail_open counter:"
curl -sS http://127.0.0.1:9090/debug/vars | python3 -c 'import json,sys; d=json.load(sys.stdin); print("ratelimit_fail_open_total=", d.get("ratelimit_fail_open_total"))'

if [[ "$denied" -lt 1 ]]; then
  echo "expected some 429s" >&2
  exit 1
fi
echo "demo ok"

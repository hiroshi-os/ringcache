#!/usr/bin/env bash
# Real HTTP bench against a live 3-node cluster. Prints measured numbers only.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ADDRS="${RINGCACHE_ADDRS:-http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082}"
N="${N:-4000}"
C="${C:-32}"

if ! curl -sf http://127.0.0.1:8080/health >/dev/null 2>&1; then
  echo "starting local cluster..."
  ./scripts/run-cluster.sh
fi

mkdir -p bin
if [[ ! -x bin/ringcache-bench ]]; then
  go build -o bin/ringcache-bench ./cmd/bench
fi

echo "=== hardware ==="
uname -a
nproc
grep -m1 'model name' /proc/cpuinfo 2>/dev/null || true
echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "go=$(go version)"
echo "=== bench ==="
./bin/ringcache-bench -addrs "$ADDRS" -n "$N" -c "$C"

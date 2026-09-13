#!/usr/bin/env bash
set -u
for n in a b c d; do
  f="/tmp/ringcache-${n}.pid"
  if [[ -f "$f" ]]; then
    pid="$(cat "$f")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$f"
  fi
done
# Fallback if started outside the pid files.
pkill -f '/bin/ringcache' 2>/dev/null || true
echo "cluster stopped"

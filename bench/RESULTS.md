# Rate-limit accuracy measurements

Every number below was produced by a command run on this machine.
Do not invent figures.

## Hardware

| Field | Value |
| --- | --- |
| CPU | AMD Ryzen 5 7530U with Radeon Graphics (6 cores / 12 logical) |
| RAM | ~23.3 GiB |
| OS | Microsoft Windows NT 10.0.26200.0 (windows/amd64) |
| Go | go1.25.5 windows/amd64 |

## Commands

Cluster: three `bin/ringcache.exe` processes on `127.0.0.1:8080-8082` (`R=2`, `V=150`).

```text
bin/ratebench.exe -target 100 -burst 20 -mult 2 -duration 60s -baseline -timeout 50ms -key rb-base
bin/ratebench.exe -target 100 -burst 20 -mult 2 -duration 60s -timeout 50ms -key rb-2x
bin/ratebench.exe -target 100 -burst 20 -mult 5 -duration 60s -timeout 50ms -key rb-5x
bin/ratebench.exe -target 100 -burst 20 -mult 2 -duration 30s -kill-after 10s -timeout 50ms -key rb-kill
```

Harness: async offer (128 workers) through `pkg/ratelimit` middleware against a live cluster.
Commit at measurement time: recorded in the table (working tree may have advanced for docs).

---

## Results (2026-09-26T15:16:41Z UTC)

Target allow rate **100 req/s**, burst **20**.

| Scenario | Offer | Allow rate | Error vs target | p50 | p99 | Samples | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline (no limiter) | 200/s | 198.13/s | n/a (uncapped) | ~0 | 0.60ms | 11888 | middleware bypassed |
| limited 2× | 200/s | **99.93/s** | **-0.07%** | 1.57ms | **7.05ms** | 11905 | clean enforcement |
| limited 5× | 500/s | 362.20/s | **+262.20%** | 159.7ms | 1.34s | 22679 | primary saturated; fail-open lets excess through |
| limited 5× (timeout 200ms) | 500/s | 196.85/s | **+96.85%** | 280.4ms | 885.9ms | 19986 | still overshoots; overload + fail-open |
| kill primary @t+10s (30s run) | 200/s | 160.17/s | +60.17% | 7.97ms | 297.4ms | 5765 | see fail-open window below |

**p99 added latency (2× run):** limited p99 7.05ms − baseline p99 0.60ms ≈ **6.45ms**.

### Fail-open / primary kill

- Killed primary **node-c** at t+10s (membership is static — id stays on the ring).
- Measured fail-open window (kill → ≥2 survivors still healthy): **311.16ms** to detect; after that every take to the dead primary errors and middleware **fails open**, so the limit is not enforced until the process returns or membership is updated.
- Allow rate for the whole 30s window rose to 160/s (+60%) because post-kill traffic was largely fail-opened (plus a fresh empty bucket is irrelevant while the primary is unreachable).

### Honesty about the 5× number

At 5× offer on this laptop the ringcache primary cannot service takes fast enough. Client timeouts / 503s trigger **fail-open**, which deliberately allows the request. The +96%…+262% error is therefore **not** “the bucket math is wrong”; it is “fail-open under overload.” The 2× run is the fair accuracy check (−0.07%).

Raw capture: `bench/ratebench-raw.txt`.

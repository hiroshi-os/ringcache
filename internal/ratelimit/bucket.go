// Package ratelimit implements a process-local token bucket used by the
// ringcache /v1/ratelimit/take endpoint.
//
// Approximate limiting is intentional: bucket state lives on the key's
// PRIMARY owner only (no replication). On failover the new primary starts
// empty, so counts reset and the limit can briefly be exceeded. That is
// acceptable for soft API quotas; do not use this for hard financial caps.
package ratelimit

import (
	"math"
	"time"
)

// Result is the outcome of one Take.
type Result struct {
	Allowed      bool
	Remaining    float64
	RetryAfterMs int64
}

// Bucket is one token bucket. Not safe for concurrent use by itself;
// Store serializes access per key.
type Bucket struct {
	tokens     float64
	lastRefill time.Time
	rate       float64 // tokens per second
	burst      float64
}

// NewBucket starts full (tokens = burst) at now.
func NewBucket(rate, burst float64, now time.Time) *Bucket {
	if rate < 0 {
		rate = 0
	}
	if burst < 0 {
		burst = 0
	}
	return &Bucket{
		tokens:     burst,
		lastRefill: now,
		rate:       rate,
		burst:      burst,
	}
}

// Take refills lazily from lastRefill→now, caps at burst, then tries to
// consume cost. cost > burst is always denied (even a full bucket cannot
// satisfy it). retry_after_ms is ceil of the wait until tokens >= cost
// (or until burst if cost > burst — still always denied thereafter).
func (b *Bucket) Take(now time.Time, cost float64) Result {
	if cost < 0 {
		cost = 0
	}
	b.refill(now)

	if cost > b.burst {
		return Result{
			Allowed:      false,
			Remaining:    b.tokens,
			RetryAfterMs: retryAfterMs(b.tokens, cost, b.rate, b.burst),
		}
	}
	if b.tokens >= cost {
		b.tokens -= cost
		return Result{Allowed: true, Remaining: b.tokens, RetryAfterMs: 0}
	}
	return Result{
		Allowed:      false,
		Remaining:    b.tokens,
		RetryAfterMs: retryAfterMs(b.tokens, cost, b.rate, b.burst),
	}
}

func (b *Bucket) refill(now time.Time) {
	if b.rate <= 0 {
		b.lastRefill = now
		return
	}
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens = math.Min(b.burst, b.tokens+elapsed*b.rate)
	b.lastRefill = now
}

func retryAfterMs(tokens, cost, rate, burst float64) int64 {
	need := cost
	if cost > burst {
		need = burst + 1e-9 // still unreachable; report time to full burst
		if tokens >= burst {
			// Already full and still cannot pay cost>burst.
			return math.MaxInt32
		}
		need = burst
	}
	deficit := need - tokens
	if deficit <= 0 {
		return 0
	}
	if rate <= 0 {
		return math.MaxInt32
	}
	sec := deficit / rate
	ms := int64(math.Ceil(sec * 1000))
	if ms < 1 {
		ms = 1
	}
	return ms
}

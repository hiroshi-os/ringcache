package ratelimit

import (
	"math"
	"testing"
	"time"
)

func TestRefillRateAndBurstCap(t *testing.T) {
	clk := &FakeClock{T: time.Unix(0, 0)}
	b := NewBucket(10, 20, clk.Now()) // 10 tok/s, burst 20
	// Drain 15.
	r := b.Take(clk.Now(), 15)
	if !r.Allowed || math.Abs(r.Remaining-5) > 1e-9 {
		t.Fatalf("drain: %+v", r)
	}
	// After 1s: +10 → 15, still under burst.
	clk.Advance(time.Second)
	r = b.Take(clk.Now(), 0)
	if math.Abs(r.Remaining-15) > 1e-6 {
		t.Fatalf("after 1s want 15 got %v", r.Remaining)
	}
	// After another 2s: +20 would be 35 → capped at 20.
	clk.Advance(2 * time.Second)
	r = b.Take(clk.Now(), 0)
	if math.Abs(r.Remaining-20) > 1e-6 {
		t.Fatalf("burst cap: want 20 got %v", r.Remaining)
	}
}

func TestCostGreaterThanOne(t *testing.T) {
	clk := &FakeClock{T: time.Unix(0, 0)}
	b := NewBucket(5, 10, clk.Now())
	r := b.Take(clk.Now(), 3)
	if !r.Allowed || math.Abs(r.Remaining-7) > 1e-9 {
		t.Fatalf("%+v", r)
	}
	r = b.Take(clk.Now(), 3)
	if !r.Allowed || math.Abs(r.Remaining-4) > 1e-9 {
		t.Fatalf("%+v", r)
	}
}

func TestCostGreaterThanBurstAlwaysDenied(t *testing.T) {
	clk := &FakeClock{T: time.Unix(0, 0)}
	b := NewBucket(100, 10, clk.Now()) // full=10
	r := b.Take(clk.Now(), 11)
	if r.Allowed {
		t.Fatalf("cost>burst must deny: %+v", r)
	}
	if r.Remaining != 10 {
		t.Fatalf("should not consume: remaining=%v", r.Remaining)
	}
	if r.RetryAfterMs <= 0 {
		t.Fatalf("expected positive retry_after, got %d", r.RetryAfterMs)
	}
	// Even after refill time, still denied.
	clk.Advance(time.Hour)
	r = b.Take(clk.Now(), 11)
	if r.Allowed {
		t.Fatalf("still must deny: %+v", r)
	}
}

func TestRetryAfterCorrectness(t *testing.T) {
	clk := &FakeClock{T: time.Unix(0, 0)}
	b := NewBucket(10, 20, clk.Now()) // 10/s
	r := b.Take(clk.Now(), 20)        // empty
	if !r.Allowed {
		t.Fatal("first take of full burst should allow")
	}
	r = b.Take(clk.Now(), 5) // need 5 tokens at 10/s → 500ms
	if r.Allowed {
		t.Fatal("expected deny")
	}
	if r.RetryAfterMs != 500 {
		t.Fatalf("retry_after_ms=%d want 500", r.RetryAfterMs)
	}
	clk.Advance(500 * time.Millisecond)
	r = b.Take(clk.Now(), 5)
	if !r.Allowed {
		t.Fatalf("after wait should allow: %+v", r)
	}
}

func TestStoreIdleTTL(t *testing.T) {
	clk := &FakeClock{T: time.Unix(1000, 0)}
	s := NewStore(8, 2*time.Second, clk)
	defer s.Close()
	_ = s.Take("k", 10, 10, 1)
	if s.Len() != 1 {
		t.Fatalf("len=%d", s.Len())
	}
	clk.Advance(3 * time.Second)
	s.sweep()
	if s.Len() != 0 {
		t.Fatalf("idle bucket should be evicted, len=%d", s.Len())
	}
}

func TestStoreCapacityEvicts(t *testing.T) {
	clk := &FakeClock{T: time.Unix(0, 0)}
	s := NewStore(2, time.Minute, clk)
	defer s.Close()
	_ = s.Take("a", 1, 1, 0)
	_ = s.Take("b", 1, 1, 0)
	_ = s.Take("c", 1, 1, 0)
	if s.Len() != 2 {
		t.Fatalf("len=%d", s.Len())
	}
}

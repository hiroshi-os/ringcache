package ratelimit

import "time"

// Clock is injectable so bucket math is deterministic in tests.
type Clock interface {
	Now() time.Time
}

// RealClock uses time.Now.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a mutable clock for unit tests.
type FakeClock struct {
	T time.Time
}

func (c *FakeClock) Now() time.Time { return c.T }

func (c *FakeClock) Advance(d time.Duration) { c.T = c.T.Add(d) }

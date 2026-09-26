package ratelimit

import (
	"sync"
	"time"
)

// Store holds per-key buckets with idle TTL eviction.
// Capacity is a max key count; when full, the least-recently-used key is dropped.
type Store struct {
	mu       sync.Mutex
	capacity int
	idleTTL  time.Duration
	clock    Clock
	items    map[string]*entry
	order    []string // rough LRU: move-to-end on access
	stop     chan struct{}
	stopped  chan struct{}
	once     sync.Once
}

type entry struct {
	bucket   *Bucket
	lastUsed time.Time
}

// NewStore creates a bucket store. capacity < 1 → 1024; idleTTL <= 0 → 60s.
func NewStore(capacity int, idleTTL time.Duration, clock Clock) *Store {
	if capacity < 1 {
		capacity = 1024
	}
	if idleTTL <= 0 {
		idleTTL = 60 * time.Second
	}
	if clock == nil {
		clock = RealClock{}
	}
	s := &Store{
		capacity: capacity,
		idleTTL:  idleTTL,
		clock:    clock,
		items:    make(map[string]*entry),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go s.janitor()
	return s
}

// Close stops the janitor. Idempotent.
func (s *Store) Close() {
	s.once.Do(func() {
		close(s.stop)
		<-s.stopped
	})
}

// Take applies rate/burst/cost to key. Rate and burst are taken from the
// request each time so a client can change limits; an existing bucket is
// resized (burst clamp) when they change.
func (s *Store) Take(key string, rate, burst, cost float64) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	e, ok := s.items[key]
	if !ok {
		s.evictIfNeededLocked()
		e = &entry{bucket: NewBucket(rate, burst, now), lastUsed: now}
		s.items[key] = e
		s.order = append(s.order, key)
	} else {
		// Update rate/burst if the client changed them.
		if e.bucket.rate != rate || e.bucket.burst != burst {
			e.bucket.refill(now)
			e.bucket.rate = rate
			e.bucket.burst = burst
			if e.bucket.tokens > burst {
				e.bucket.tokens = burst
			}
		}
		s.touchLocked(key)
	}
	e.lastUsed = now
	return e.bucket.Take(now, cost)
}

// Len is the number of live buckets.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func (s *Store) touchLocked(key string) {
	for i, k := range s.order {
		if k == key {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.order = append(s.order, key)
}

func (s *Store) evictIfNeededLocked() {
	for len(s.items) >= s.capacity && len(s.order) > 0 {
		victim := s.order[0]
		s.order = s.order[1:]
		delete(s.items, victim)
	}
}

func (s *Store) janitor() {
	defer close(s.stopped)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	kept := s.order[:0]
	for _, key := range s.order {
		e, ok := s.items[key]
		if !ok {
			continue
		}
		if now.Sub(e.lastUsed) > s.idleTTL {
			delete(s.items, key)
			continue
		}
		kept = append(kept, key)
	}
	s.order = kept
}

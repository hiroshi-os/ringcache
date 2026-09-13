// Package store is a process-local in-memory cache with LRU eviction and TTL.
//
// Capacity is a max key count (not bytes). Expired entries are removed lazily
// on read and by a background janitor; they still occupy a slot until then, so
// a burst of short-TTL writes can evict live keys before the janitor runs.
//
// Concurrent writers use last-writer-wins on WrittenAt (coordinator-assigned
// unix-nano). An older replica write is ignored so a delayed retry cannot
// clobber a newer value.
package store

import (
	"container/list"
	"sync"
	"time"
)

// Entry is a stored value plus metadata returned to callers.
type Entry struct {
	Value     string
	ExpiresAt time.Time // zero means no TTL
	WrittenAt int64
}

type item struct {
	key       string
	value     string
	expiresAt time.Time
	writtenAt int64
}

// Stats is a point-in-time snapshot (not atomic across fields).
type Stats struct {
	Keys      int    `json:"keys"`
	Capacity  int    `json:"capacity"`
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Sets      uint64 `json:"sets"`
	Deletes   uint64 `json:"deletes"`
	Evictions uint64 `json:"evictions"`
	Expired   uint64 `json:"expired"`
	Skipped   uint64 `json:"skipped_stale_writes"`
}

// Store is a mutex-protected LRU+TTL map.
type Store struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List // front = most recently used
	items    map[string]*list.Element
	hits     uint64
	misses   uint64
	sets     uint64
	deletes  uint64
	evicts   uint64
	expired  uint64
	skipped  uint64
	stop     chan struct{}
	stopped  chan struct{}
}

// New creates a store. capacity < 1 means 1.
func New(capacity int) *Store {
	if capacity < 1 {
		capacity = 1
	}
	s := &Store{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go s.janitor()
	return s
}

// Close stops the janitor. Safe to call once.
func (s *Store) Close() {
	select {
	case <-s.stop:
		return
	default:
		close(s.stop)
	}
	<-s.stopped
}

// Get returns (entry, true) on a live hit. Expired keys are deleted and miss.
func (s *Store) Get(key string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		s.misses++
		return Entry{}, false
	}
	it := el.Value.(*item)
	if expired(it, time.Now()) {
		s.removeElem(el)
		s.expired++
		s.misses++
		return Entry{}, false
	}
	s.ll.MoveToFront(el)
	s.hits++
	return Entry{Value: it.value, ExpiresAt: it.expiresAt, WrittenAt: it.writtenAt}, true
}

// Set inserts or updates key. Returns false if the write was ignored as stale
// (existing WrittenAt is strictly newer). ttl<=0 means no expiration.
func (s *Store) Set(key, value string, ttl time.Duration, writtenAt int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var exp time.Time
	if ttl > 0 {
		exp = now.Add(ttl)
	}
	if el, ok := s.items[key]; ok {
		it := el.Value.(*item)
		if it.writtenAt > writtenAt {
			s.skipped++
			return false
		}
		it.value = value
		it.expiresAt = exp
		it.writtenAt = writtenAt
		s.ll.MoveToFront(el)
		s.sets++
		return true
	}
	for len(s.items) >= s.capacity {
		s.evictLRU()
	}
	it := &item{key: key, value: value, expiresAt: exp, writtenAt: writtenAt}
	s.items[key] = s.ll.PushFront(it)
	s.sets++
	return true
}

// Delete removes key. Returns whether it was present (expired still counts).
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.items[key]
	if !ok {
		return false
	}
	s.removeElem(el)
	s.deletes++
	return true
}

// Len is the number of keys currently held, including not-yet-swept expired.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// Stats returns counters plus current occupancy.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		Keys:      len(s.items),
		Capacity:  s.capacity,
		Hits:      s.hits,
		Misses:    s.misses,
		Sets:      s.sets,
		Deletes:   s.deletes,
		Evictions: s.evicts,
		Expired:   s.expired,
		Skipped:   s.skipped,
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
		case now := <-t.C:
			s.sweep(now)
		}
	}
}

func (s *Store) sweep(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for el := s.ll.Back(); el != nil; {
		prev := el.Prev()
		it := el.Value.(*item)
		if expired(it, now) {
			s.removeElem(el)
			s.expired++
		}
		el = prev
	}
}

func (s *Store) evictLRU() {
	el := s.ll.Back()
	if el == nil {
		return
	}
	s.removeElem(el)
	s.evicts++
}

func (s *Store) removeElem(el *list.Element) {
	it := el.Value.(*item)
	delete(s.items, it.key)
	s.ll.Remove(el)
}

func expired(it *item, now time.Time) bool {
	return !it.expiresAt.IsZero() && !now.Before(it.expiresAt)
}

package store

import (
	"strconv"
	"testing"
	"time"
)

func TestSetGetDelete(t *testing.T) {
	s := New(8)
	defer s.Close()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss")
	}
	if !s.Set("k", "v", 0, 1) {
		t.Fatal("set")
	}
	e, ok := s.Get("k")
	if !ok || e.Value != "v" || e.WrittenAt != 1 {
		t.Fatalf("get: %#v ok=%v", e, ok)
	}
	if !s.Delete("k") || s.Delete("k") {
		t.Fatal("delete once")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("deleted key still present")
	}
}

func TestLRUEvictionOrder(t *testing.T) {
	s := New(2)
	defer s.Close()
	s.Set("a", "1", 0, 1)
	s.Set("b", "2", 0, 2)
	// Touch a so b is the LRU victim.
	if _, ok := s.Get("a"); !ok {
		t.Fatal("get a")
	}
	s.Set("c", "3", 0, 3)
	if _, ok := s.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if e, ok := s.Get("a"); !ok || e.Value != "1" {
		t.Fatal("a should remain")
	}
	if e, ok := s.Get("c"); !ok || e.Value != "3" {
		t.Fatal("c should remain")
	}
	st := s.Stats()
	if st.Evictions != 1 {
		t.Fatalf("evictions=%d", st.Evictions)
	}
	if st.Keys != 2 {
		t.Fatalf("keys=%d", st.Keys)
	}
}

func TestOverwriteDoesNotGrow(t *testing.T) {
	s := New(2)
	defer s.Close()
	s.Set("a", "1", 0, 1)
	s.Set("a", "2", 0, 2)
	if s.Len() != 1 {
		t.Fatalf("len=%d", s.Len())
	}
	if e, ok := s.Get("a"); !ok || e.Value != "2" {
		t.Fatalf("value=%#v ok=%v", e, ok)
	}
}

func TestTTLExpiresOnGet(t *testing.T) {
	s := New(4)
	defer s.Close()
	s.Set("soon", "x", 15*time.Millisecond, 1)
	if _, ok := s.Get("soon"); !ok {
		t.Fatal("should be live immediately")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("soon"); ok {
		t.Fatal("should have expired")
	}
	st := s.Stats()
	if st.Expired < 1 {
		t.Fatalf("expired counter=%d", st.Expired)
	}
	if st.Misses < 1 {
		t.Fatalf("misses=%d", st.Misses)
	}
}

func TestTTLZeroMeansNoExpiry(t *testing.T) {
	s := New(2)
	defer s.Close()
	s.Set("perm", "y", 0, 1)
	time.Sleep(20 * time.Millisecond)
	if _, ok := s.Get("perm"); !ok {
		t.Fatal("ttl=0 must not expire")
	}
}

func TestLastWriterWins(t *testing.T) {
	s := New(2)
	defer s.Close()
	if !s.Set("k", "new", 0, 100) {
		t.Fatal("newer write")
	}
	if s.Set("k", "old", 0, 50) {
		t.Fatal("stale write should be ignored")
	}
	e, ok := s.Get("k")
	if !ok || e.Value != "new" || e.WrittenAt != 100 {
		t.Fatalf("got %#v ok=%v", e, ok)
	}
	if s.Stats().Skipped != 1 {
		t.Fatalf("skipped=%d", s.Stats().Skipped)
	}
}

func TestCapacityOne(t *testing.T) {
	s := New(1)
	defer s.Close()
	s.Set("a", "1", 0, 1)
	s.Set("b", "2", 0, 2)
	if s.Len() != 1 {
		t.Fatalf("len=%d", s.Len())
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("a should be evicted")
	}
	if e, ok := s.Get("b"); !ok || e.Value != "2" {
		t.Fatal("b should remain")
	}
}

func TestSweepRemovesExpired(t *testing.T) {
	s := New(8)
	defer s.Close()
	s.Set("x", "1", 5*time.Millisecond, 1)
	time.Sleep(10 * time.Millisecond)
	s.sweep(time.Now())
	if s.Len() != 0 {
		t.Fatalf("janitor/sweep left %d keys", s.Len())
	}
}

func TestHitsAndMisses(t *testing.T) {
	s := New(4)
	defer s.Close()
	s.Set("a", "1", 0, 1)
	s.Get("a")
	s.Get("missing")
	st := s.Stats()
	if st.Hits != 1 || st.Misses != 1 || st.Sets != 1 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestCloseIdempotent(t *testing.T) {
	s := New(2)
	s.Close()
	s.Close()
}

func TestConcurrentSetGet(t *testing.T) {
	s := New(256)
	defer s.Close()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			s.Set("k"+strconv.Itoa(i%16), "v", 0, int64(i+1))
		}
		close(done)
	}()
	for i := 0; i < 200; i++ {
		s.Get("k" + strconv.Itoa(i%16))
	}
	<-done
}

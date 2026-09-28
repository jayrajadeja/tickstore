package store

import (
	"errors"
	"math/rand"
	"sync"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

var errMismatch = errors.New("cache/store result mismatch")

// seedMany appends n ticks with non-decreasing timestamps (some ties) so both
// the sparse index and duplicate-TS handling are exercised.
func seedMany(t *testing.T, s *Store, sym string, n int) {
	t.Helper()
	a, err := s.OpenAppender(sym)
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	ts := int64(0)
	r := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		if r.Intn(3) != 0 { // ~2/3 advance, ~1/3 tie
			ts++
		}
		tk := tick.Tick{TS: ts, Price: 100 + int64(i%50), Qty: uint64(i%7) + 1, Side: tick.Side(i % 2)}
		if err := a.Append(tk); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func ticksEqual(a, b []tick.Tick) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCacheParityWithStore: over the same directory, Cache returns byte-identical
// results to a plain Store for random Range and Last queries.
func TestCacheParityWithStore(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	seedMany(t, s, "SYNTH", 5000)

	c := NewCached(dir)
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 300; i++ {
		from := int64(r.Intn(4000))
		to := from + int64(r.Intn(1500))
		want, err := s.Range("SYNTH", from, to)
		if err != nil {
			t.Fatalf("Store.Range: %v", err)
		}
		got, err := c.Range("SYNTH", from, to)
		if err != nil {
			t.Fatalf("Cache.Range: %v", err)
		}
		if !ticksEqual(want, got) {
			t.Fatalf("Range(%d,%d) mismatch: store %d ticks, cache %d ticks", from, to, len(want), len(got))
		}
	}
	for i := 0; i < 100; i++ {
		n := r.Intn(200)
		want, err := s.Last("SYNTH", n)
		if err != nil {
			t.Fatalf("Store.Last: %v", err)
		}
		got, err := c.Last("SYNTH", n)
		if err != nil {
			t.Fatalf("Cache.Last: %v", err)
		}
		if !ticksEqual(want, got) {
			t.Fatalf("Last(%d) mismatch", n)
		}
	}
}

// TestCacheMissingThenCreated: a symbol with no log returns empty; after the log
// is created the cache picks it up on the next request.
func TestCacheMissingThenCreated(t *testing.T) {
	dir := t.TempDir()
	c := NewCached(dir)

	got, err := c.Range("NEW", 0, 100)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing Range = %v, %v; want empty, nil", got, err)
	}
	got, err = c.Last("NEW", 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing Last = %v, %v; want empty, nil", got, err)
	}

	seedMany(t, New(dir), "NEW", 300)

	got, err = c.Last("NEW", 5)
	if err != nil {
		t.Fatalf("Last after create: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("Last after create = %d ticks, want 5", len(got))
	}
}

// TestCacheSeesGrowth: after more records are appended to an already-cached log,
// the Stat gate detects the growth and subsequent reads include the new data.
func TestCacheSeesGrowth(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	seedMany(t, s, "G", 500)

	c := NewCached(dir)
	before, err := c.Last("G", 1)
	if err != nil {
		t.Fatalf("Last before: %v", err)
	}
	if len(before) != 1 {
		t.Fatalf("want 1 tick before growth")
	}

	// Append 500 more with strictly larger timestamps.
	a, err := s.OpenAppender("G")
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	lastTS := before[0].TS
	for i := 0; i < 500; i++ {
		lastTS++
		if err := a.Append(tick.Tick{TS: lastTS, Price: 200, Qty: 1, Side: tick.Buy}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	after, err := c.Last("G", 1)
	if err != nil {
		t.Fatalf("Last after: %v", err)
	}
	if after[0].TS != lastTS {
		t.Fatalf("cache did not see growth: last TS %d, want %d", after[0].TS, lastTS)
	}
	// Full parity with a fresh Store after growth.
	want, _ := s.Range("G", 0, lastTS)
	got, _ := c.Range("G", 0, lastTS)
	if !ticksEqual(want, got) {
		t.Fatalf("post-growth Range parity failed: store %d, cache %d", len(want), len(got))
	}
}

// TestCacheConcurrentReads exercises the shared fd + index under many goroutines
// (run with -race). Results must match the plain Store.
func TestCacheConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	seedMany(t, s, "C", 3000)
	c := NewCached(dir)

	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 200; i++ {
				from := int64(r.Intn(2500))
				to := from + int64(r.Intn(500))
				want, _ := s.Range("C", from, to)
				got, err := c.Range("C", from, to)
				if err != nil {
					errCh <- err
					return
				}
				if !ticksEqual(want, got) {
					errCh <- errMismatch
					return
				}
			}
		}(int64(g))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent read: %v", err)
	}
}

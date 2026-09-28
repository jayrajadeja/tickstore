package store

import (
	"os"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

// ingestN appends n ticks (TS = i, Price = 100+i, Qty = 1) into symbol.
func ingestN(t testing.TB, s *Store, symbol string, n int) {
	t.Helper()
	a, err := s.OpenAppender(symbol)
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	for i := 0; i < n; i++ {
		side := tick.Buy
		if i%2 == 1 {
			side = tick.Sell
		}
		if err := a.Append(tick.Tick{TS: int64(i), Price: int64(100 + i), Qty: 1, Side: side}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAppenderWritesConsistentIndex(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	ingestN(t, s, "SYM", 300) // strides at 0, 128, 256

	entries, err := loadIndex(s.idxPath("SYM"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != expectedIndexLen(300) || len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	wantIdx := []int64{0, 128, 256}
	for i, e := range entries {
		if e.RecordIndex != wantIdx[i] {
			t.Fatalf("entry %d RecordIndex = %d, want %d", i, e.RecordIndex, wantIdx[i])
		}
		if e.TS != wantIdx[i] { // TS == i in ingestN
			t.Fatalf("entry %d TS = %d, want %d", i, e.TS, wantIdx[i])
		}
	}
}

func TestAppenderReopenExtendsIndex(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	ingestN(t, s, "SYM", 300)

	// Append 100 more (total 400 → new checkpoint at 384).
	a, err := s.OpenAppender("SYM")
	if err != nil {
		t.Fatal(err)
	}
	for i := 300; i < 400; i++ {
		if err := a.Append(tick.Tick{TS: int64(i), Price: int64(100 + i), Qty: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	entries, _ := loadIndex(s.idxPath("SYM"))
	if len(entries) != expectedIndexLen(400) || len(entries) != 4 {
		t.Fatalf("entries = %d, want 4", len(entries))
	}
	if entries[3].RecordIndex != 384 || entries[3].TS != 384 {
		t.Fatalf("entry 3 = %+v, want {384,384}", entries[3])
	}
}

func TestOpenAppenderRebuildsDeletedIndex(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	ingestN(t, s, "SYM", 300)
	good, _ := loadIndex(s.idxPath("SYM"))

	// Delete the index; reopening + closing (no appends) must rebuild it identically.
	if err := os.Remove(s.idxPath("SYM")); err != nil {
		t.Fatal(err)
	}
	a, err := s.OpenAppender("SYM")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	rebuilt, _ := loadIndex(s.idxPath("SYM"))
	if len(rebuilt) != len(good) {
		t.Fatalf("rebuilt len = %d, want %d", len(rebuilt), len(good))
	}
	for i := range good {
		if rebuilt[i] != good[i] {
			t.Fatalf("rebuilt[%d] = %+v, want %+v", i, rebuilt[i], good[i])
		}
	}
}

func TestOpenAppenderRebuildsCorruptIndex(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	ingestN(t, s, "SYM", 300)

	// Truncate the index to a partial/inconsistent state.
	if err := os.Truncate(s.idxPath("SYM"), idxHeaderSize+idxEntrySize+3); err != nil {
		t.Fatal(err)
	}
	a, err := s.OpenAppender("SYM")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	entries, _ := loadIndex(s.idxPath("SYM"))
	if len(entries) != 3 {
		t.Fatalf("entries after rebuild = %d, want 3", len(entries))
	}
}

package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func appendAll(t *testing.T, s *Store, sym string, ticks []tick.Tick) {
	t.Helper()
	a, err := s.OpenAppender(sym)
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	for _, tk := range ticks {
		if err := a.Append(tk); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLastReturnsFinalN(t *testing.T) {
	s := New(t.TempDir())
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
		{TS: 3, Price: 102, Qty: 1, Side: tick.Buy},
	}
	appendAll(t, s, "X", ticks)

	got, err := s.Last("X", 2)
	if err != nil {
		t.Fatalf("Last: %v", err)
	}
	want := ticks[1:]
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Last(2) = %+v, want %+v", got, want)
	}
}

func TestLastClampsToLength(t *testing.T) {
	s := New(t.TempDir())
	ticks := []tick.Tick{{TS: 1, Price: 100, Qty: 5, Side: tick.Buy}}
	appendAll(t, s, "X", ticks)

	got, err := s.Last("X", 10)
	if err != nil {
		t.Fatalf("Last: %v", err)
	}
	if len(got) != 1 || got[0] != ticks[0] {
		t.Fatalf("Last(10) = %+v, want %+v", got, ticks)
	}
}

func TestLastMissingSymbolEmpty(t *testing.T) {
	got, err := New(t.TempDir()).Last("NOPE", 5)
	if err != nil {
		t.Fatalf("Last missing: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Last missing = %+v, want empty", got)
	}
}

func TestLastIgnoresTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
	}
	appendAll(t, s, "X", ticks)

	// Corrupt: append a stray partial record (fewer than RecordSize bytes).
	f, err := os.OpenFile(filepath.Join(dir, "X.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	if _, err := f.Write([]byte{0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	f.Close()

	got, err := s.Last("X", 10)
	if err != nil {
		t.Fatalf("Last: %v", err)
	}
	if len(got) != 2 || got[1] != ticks[1] {
		t.Fatalf("Last ignoring tail = %+v, want the two whole records", got)
	}
}

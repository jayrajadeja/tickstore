package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func TestAppenderWritesHeaderOnce(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)

	a, err := s.OpenAppender("BTC-USD")
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	if err := a.Append(tick.Tick{TS: 1, Price: 100, Qty: 5, Side: tick.Buy}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and append again — header must NOT be rewritten.
	a2, err := s.OpenAppender("BTC-USD")
	if err != nil {
		t.Fatalf("reopen OpenAppender: %v", err)
	}
	if err := a2.Append(tick.Tick{TS: 2, Price: 101, Qty: 1, Side: tick.Sell}); err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := a2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "BTC-USD.log"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	want := int64(headerSize + 2*tick.RecordSize)
	if info.Size() != want {
		t.Fatalf("file size = %d, want %d (one header + two records)", info.Size(), want)
	}
}

func TestValidateHeaderRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "X.log")
	// 8-byte bogus header + no records.
	if err := os.WriteFile(path, []byte("BADMAGIC"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := New(dir).OpenAppender("X"); err != ErrBadMagic {
		t.Fatalf("err = %v, want ErrBadMagic", err)
	}
}

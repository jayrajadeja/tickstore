package store

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexEntryGolden(t *testing.T) {
	e := indexEntry{TS: 1, RecordIndex: 128}
	got := encodeIndexEntry(e)
	// TS=1 LE (8) + RecordIndex=128=0x80 LE (8)
	want, _ := hex.DecodeString("01000000000000008000000000000000")
	if !bytes.Equal(got[:], want) {
		t.Fatalf("encode = %x, want %x", got[:], want)
	}
	if dec := decodeIndexEntry(got[:]); dec != e {
		t.Fatalf("decode = %+v, want %+v", dec, e)
	}
}

func TestExpectedIndexLen(t *testing.T) {
	cases := map[int64]int{0: 0, 1: 1, 128: 1, 129: 2, 256: 2, 257: 3}
	for count, want := range cases {
		if got := expectedIndexLen(count); got != want {
			t.Errorf("expectedIndexLen(%d) = %d, want %d", count, got, want)
		}
	}
}

func TestWriteLoadIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "SYM.idx")
	entries := []indexEntry{{1, 0}, {50, 128}, {90, 256}}
	if err := writeIndex(path, entries); err != nil {
		t.Fatal(err)
	}
	got, err := loadIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("len = %d, want %d", len(got), len(entries))
	}
	for i := range entries {
		if got[i] != entries[i] {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], entries[i])
		}
	}
}

func TestLoadIndexMissing(t *testing.T) {
	got, err := loadIndex(filepath.Join(t.TempDir(), "nope.idx"))
	if err != nil || got != nil {
		t.Fatalf("missing = %v,%v, want nil,nil", got, err)
	}
}

func TestLoadIndexBadHeader(t *testing.T) {
	dir := t.TempDir()
	badMagic := filepath.Join(dir, "m.idx")
	h := append([]byte("XXXXXX"), 0x01, 0x00)
	os.WriteFile(badMagic, h, 0o644)
	if _, err := loadIndex(badMagic); !errors.Is(err, ErrBadIdxMagic) {
		t.Fatalf("bad magic err = %v, want ErrBadIdxMagic", err)
	}
	badVer := filepath.Join(dir, "v.idx")
	os.WriteFile(badVer, append([]byte("TCKIDX"), 0x02, 0x00), 0o644)
	if _, err := loadIndex(badVer); !errors.Is(err, ErrBadIdxVersion) {
		t.Fatalf("bad version err = %v, want ErrBadIdxVersion", err)
	}
}

func TestLoadIndexPartialTailIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.idx")
	if err := writeIndex(path, []indexEntry{{1, 0}, {9, 128}}); err != nil {
		t.Fatal(err)
	}
	// append 5 stray bytes (a partial entry)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{1, 2, 3, 4, 5})
	f.Close()
	got, err := loadIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (partial tail ignored)", len(got))
	}
}

package store

import (
	"io"
	"os"
	"reflect"
	"testing"
)

type countingReaderAt struct {
	ra io.ReaderAt
	n  *int
}

func (c countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	*c.n++
	return c.ra.ReadAt(p, off)
}

// TestRangeParityIndexedVsFallback is the core guarantee: the indexed path and
// the no-index fallback return byte-identical results for every query.
func TestRangeParityIndexedVsFallback(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	const n = 500
	ingestN(t, s, "SYM", n) // TS = 0..499

	type qr struct{ from, to int64 }
	queries := []qr{
		{-5, 1000},  // full
		{0, 0},      // first only
		{499, 499},  // last only
		{127, 129},  // across stride boundary 128
		{128, 128},  // exact stride multiple
		{256, 300},  // interior block
		{384, 500},  // last block onward
		{1000, 2000}, // past end → empty
		{-100, -1},  // before start → empty
		{200, 100},  // from > to → empty
		{250, 250},  // single interior
	}
	idxPath := s.idxPath("SYM")
	for _, q := range queries {
		indexed, err := s.Range("SYM", q.from, q.to)
		if err != nil {
			t.Fatalf("indexed Range(%d,%d): %v", q.from, q.to, err)
		}
		// Force fallback by hiding the index.
		saved, _ := os.ReadFile(idxPath)
		os.Remove(idxPath)
		fallback, err := s.Range("SYM", q.from, q.to)
		if err != nil {
			t.Fatalf("fallback Range(%d,%d): %v", q.from, q.to, err)
		}
		os.WriteFile(idxPath, saved, 0o644)

		if !reflect.DeepEqual(indexed, fallback) {
			t.Fatalf("parity mismatch for (%d,%d):\n indexed=%v\n fallback=%v", q.from, q.to, indexed, fallback)
		}
	}
}

// TestIndexedLowerBoundFewerSeeks asserts the headline metric: the indexed
// lower-bound issues far fewer ReadAt calls than the log binary search.
func TestIndexedLowerBoundFewerSeeks(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	const n = 4000
	ingestN(t, s, "SYM", n)

	f, err := s.openForRead("SYM")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	entries, _ := loadIndex(s.idxPath("SYM"))
	if len(entries) == 0 {
		t.Fatal("no index entries")
	}
	from := int64(3000)

	var scanN, idxN int
	scanLo, _ := scanLowerBound(countingReaderAt{f, &scanN}, n, from)
	idxLo, _ := indexedLowerBound(countingReaderAt{f, &idxN}, entries, n, from)
	if scanLo != idxLo {
		t.Fatalf("lower bounds differ: scan=%d idx=%d", scanLo, idxLo)
	}
	if idxN >= scanN {
		t.Fatalf("indexed seeks (%d) not fewer than scan seeks (%d)", idxN, scanN)
	}
	if idxN != 1 {
		t.Fatalf("indexed lower-bound should be 1 block read, got %d", idxN)
	}
	t.Logf("seeks: scan=%d indexed=%d (n=%d)", scanN, idxN, n)
}

// TestRangeInconsistentIndexFallsBack: a short/stale index (length != the log's
// expected entry count) is ignored on read, so results stay correct via fallback.
func TestRangeInconsistentIndexFallsBack(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	const n = 500
	ingestN(t, s, "SYM", n)

	// Overwrite the index with only its first entry ({0,0}) — deliberately stale.
	if err := writeIndex(s.idxPath("SYM"), []indexEntry{{TS: 0, RecordIndex: 0}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Range("SYM", 400, 410)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 11 || got[0].TS != 400 || got[10].TS != 410 {
		t.Fatalf("stale-index range wrong: len=%d first/last=%v/%v", len(got), got[0].TS, got[len(got)-1].TS)
	}
}

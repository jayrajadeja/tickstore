package store

import (
	"io"
	"sort"

	"github.com/jayrajadeja/tickstore/tick"
)

// Range returns all ticks with from <= TS <= to (logical time), ascending. It
// locates the first record whose TS is >= from — using the sparse index when
// present (one in-memory search + one bounded block read) or an in-log binary
// search as a fallback — then scans forward until TS > to. The index only ever
// supplies a lower bound; results are always identical to a pure log scan.
// Missing log => empty slice.
func (s *Store) Range(symbol string, from, to int64) ([]tick.Tick, error) {
	if from > to {
		return nil, nil
	}
	f, err := s.openForRead(symbol)
	if err != nil || f == nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	count, _ := recordCount(info.Size())

	entries, _ := loadIndex(s.idxPath(symbol)) // a bad/missing index simply falls back
	return rangeFrom(f, count, entries, from, to)
}

// rangeFrom is the shared Range core over an already-opened log: it locates the
// first record with TS >= from (via a consistent sparse index, else an in-log
// binary search) and scans forward until TS > to. Callers supply the record
// count and any loaded index; both the plain Store and the resident Cache use it
// so their results are byte-for-byte identical. from <= to is assumed.
func rangeFrom(ra io.ReaderAt, count int64, entries []indexEntry, from, to int64) ([]tick.Tick, error) {
	if count == 0 {
		return nil, nil
	}
	var lo int64
	var err error
	// Only trust an index that is consistent with the current log; a short/stale
	// or corrupt-length index is ignored (fallback stays correct, and the index is
	// rebuilt on the next OpenAppender or `reindex`).
	if len(entries) > 0 && len(entries) == expectedIndexLen(count) {
		lo, err = indexedLowerBound(ra, entries, count, from)
	} else {
		lo, err = scanLowerBound(ra, count, from)
	}
	if err != nil {
		return nil, err
	}

	var out []tick.Tick
	for i := lo; i < count; i++ {
		tk, err := readRecordAt(ra, i)
		if err != nil {
			return nil, err
		}
		if tk.TS > to {
			break
		}
		out = append(out, tk)
	}
	return out, nil
}

// scanLowerBound binary-searches the log itself for the first record with
// TS >= from (the no-index fallback: ~log2(count) scattered reads).
func scanLowerBound(ra io.ReaderAt, count, from int64) (int64, error) {
	lo, hi := int64(0), count
	for lo < hi {
		mid := lo + (hi-lo)/2
		tk, err := readRecordAt(ra, mid)
		if err != nil {
			return 0, err
		}
		if tk.TS >= from {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

// indexedLowerBound uses the sparse index to jump to the block that must contain
// the first record with TS >= from, reads that one block, and refines within it.
// It selects the last checkpoint with TS < from as the block start; the next
// checkpoint therefore has TS >= from, so the first record >= from — even across
// a run of duplicate timestamps equal to from — is within stride+1 records. This
// keeps results byte-identical to scanLowerBound's lower_bound over duplicates.
func indexedLowerBound(ra io.ReaderAt, entries []indexEntry, count, from int64) (int64, error) {
	// Last entry with TS < from (−1 if from precedes/equals the first checkpoint).
	i := sort.Search(len(entries), func(k int) bool { return entries[k].TS >= from }) - 1
	var base int64
	if i >= 0 {
		base = entries[i].RecordIndex
	}
	end := base + indexStride + 1 // inclusive of the next checkpoint record
	if end > count {
		end = count
	}
	block, err := readBlock(ra, base, end-base)
	if err != nil {
		return 0, err
	}
	for j, tk := range block {
		if tk.TS >= from {
			return base + int64(j), nil
		}
	}
	return count, nil // no record >= from (from is past the end)
}

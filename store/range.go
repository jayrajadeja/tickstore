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
	if count == 0 {
		return nil, nil
	}

	entries, _ := loadIndex(s.idxPath(symbol)) // a bad/missing index simply falls back

	var lo int64
	// Only trust an index that is consistent with the current log; a short/stale
	// or corrupt-length index is ignored (fallback stays correct, and the index is
	// rebuilt on the next OpenAppender or `reindex`).
	if len(entries) > 0 && len(entries) == expectedIndexLen(count) {
		lo, err = indexedLowerBound(f, entries, count, from)
	} else {
		lo, err = scanLowerBound(f, count, from)
	}
	if err != nil {
		return nil, err
	}

	var out []tick.Tick
	for i := lo; i < count; i++ {
		tk, err := readRecordAt(f, i)
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
// The checkpoint at or before `from` bounds the block; the next checkpoint
// (TS > from) bounds its far edge, so the target is within stride+1 records.
func indexedLowerBound(ra io.ReaderAt, entries []indexEntry, count, from int64) (int64, error) {
	// Largest entry with TS <= from (−1 if from precedes the first checkpoint).
	i := sort.Search(len(entries), func(k int) bool { return entries[k].TS > from }) - 1
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

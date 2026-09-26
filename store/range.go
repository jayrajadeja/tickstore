package store

import (
	"github.com/jayrajadeja/tickstore/tick"
)

// Range returns all ticks with from <= TS <= to (logical time), ascending. It
// binary-searches the sorted fixed-width log for the first record whose TS is
// >= from, then scans forward until TS > to. Missing log => empty slice.
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

	// First index with TS >= from (half-open [lo, hi)).
	lo, hi := int64(0), count
	for lo < hi {
		mid := (lo + hi) / 2
		tk, err := readRecordAt(f, mid)
		if err != nil {
			return nil, err
		}
		if tk.TS >= from {
			hi = mid
		} else {
			lo = mid + 1
		}
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

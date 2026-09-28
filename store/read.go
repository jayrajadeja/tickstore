package store

import (
	"os"

	"github.com/jayrajadeja/tickstore/tick"
)

// recordCount returns the number of whole records after the header and whether
// a partial (truncated) trailing record is present.
func recordCount(size int64) (n int64, truncated bool) {
	body := size - headerSize
	if body <= 0 {
		return 0, false
	}
	return body / tick.RecordSize, body%tick.RecordSize != 0
}

// readRecordAt reads and decodes the i-th record (0-based) from f.
func readRecordAt(f *os.File, i int64) (tick.Tick, error) {
	var buf [tick.RecordSize]byte
	off := int64(headerSize) + i*tick.RecordSize
	if _, err := f.ReadAt(buf[:], off); err != nil {
		return tick.Tick{}, err
	}
	return tick.Decode(buf[:])
}

// openForRead opens the log read-only and validates its header. A missing file
// yields (nil, nil) so callers can return an empty result.
func (s *Store) openForRead(symbol string) (*os.File, error) {
	f, err := os.Open(s.path(symbol))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if err := validateHeader(f); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// Last returns the final n ticks (fewer if the log is shorter), ascending. A
// missing log returns an empty slice. A truncated trailing record is ignored.
func (s *Store) Last(symbol string, n int) ([]tick.Tick, error) {
	if n <= 0 {
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
	start := count - int64(n)
	if start < 0 {
		start = 0
	}
	out := make([]tick.Tick, 0, count-start)
	for i := start; i < count; i++ {
		tk, err := readRecordAt(f, i)
		if err != nil {
			return nil, err
		}
		out = append(out, tk)
	}
	return out, nil
}

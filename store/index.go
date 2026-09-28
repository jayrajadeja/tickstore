package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// The sparse index is a disposable, rebuildable sidecar (SYMBOL.idx) that lets a
// Range locate its starting record without a cold O(log N) binary search over the
// log. Entry i describes log record i*indexStride. The log is always the source of
// truth; the index only ever supplies a lower bound that reads refine forward.

const (
	indexStride  = 128 // one index entry every this many records
	idxHeaderSize = 8
	idxEntrySize  = 16
	idxVersion    = uint16(1)
)

var idxMagic = []byte("TCKIDX") // 6 bytes

// Sentinel errors for a malformed index. On any of these a reader treats the
// index as absent and falls back to the log.
var (
	ErrBadIdxMagic   = errors.New("store: bad index magic")
	ErrBadIdxVersion = errors.New("store: unsupported index version")
)

// indexEntry maps a checkpoint record's logical TS to its record index in the log.
type indexEntry struct {
	TS          int64
	RecordIndex int64
}

func encodeIndexEntry(e indexEntry) [idxEntrySize]byte {
	var b [idxEntrySize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(e.TS))
	binary.LittleEndian.PutUint64(b[8:16], uint64(e.RecordIndex))
	return b
}

func decodeIndexEntry(buf []byte) indexEntry {
	return indexEntry{
		TS:          int64(binary.LittleEndian.Uint64(buf[0:8])),
		RecordIndex: int64(binary.LittleEndian.Uint64(buf[8:16])),
	}
}

// expectedIndexLen returns how many entries a consistent index has for a log of
// count records (checkpoints at 0, stride, 2*stride, … < count).
func expectedIndexLen(count int64) int {
	if count <= 0 {
		return 0
	}
	return int((count-1)/indexStride) + 1
}

// buildIndex samples every indexStride-th record's TS to reconstruct the index.
func buildIndex(ra io.ReaderAt, count int64) ([]indexEntry, error) {
	entries := make([]indexEntry, 0, expectedIndexLen(count))
	for j := int64(0); j < count; j += indexStride {
		tk, err := readRecordAt(ra, j)
		if err != nil {
			return nil, err
		}
		entries = append(entries, indexEntry{TS: tk.TS, RecordIndex: j})
	}
	return entries, nil
}

// writeIndex writes entries to path atomically (temp file + rename).
func writeIndex(path string, entries []indexEntry) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	var h [idxHeaderSize]byte
	copy(h[0:6], idxMagic)
	binary.LittleEndian.PutUint16(h[6:8], idxVersion)
	if _, err := w.Write(h[:]); err != nil {
		f.Close()
		return err
	}
	for _, e := range entries {
		b := encodeIndexEntry(e)
		if _, err := w.Write(b[:]); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadIndex reads and validates an index file. A missing file yields (nil, nil).
// A trailing partial entry is ignored, mirroring the log's truncated-tail handling.
func loadIndex(path string) ([]indexEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) < idxHeaderSize {
		return nil, ErrBadIdxMagic
	}
	if !bytes.Equal(data[0:6], idxMagic) {
		return nil, ErrBadIdxMagic
	}
	if binary.LittleEndian.Uint16(data[6:8]) != idxVersion {
		return nil, ErrBadIdxVersion
	}
	body := data[idxHeaderSize:]
	n := len(body) / idxEntrySize
	entries := make([]indexEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = decodeIndexEntry(body[i*idxEntrySize : (i+1)*idxEntrySize])
	}
	return entries, nil
}

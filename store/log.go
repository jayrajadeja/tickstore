// Package store implements an append-only, log-structured tick store: one
// fixed-width, header-prefixed log file per symbol.
package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/jayrajadeja/tickstore/tick"
)

const (
	headerSize = 8
	version    = uint16(1)
)

var magic = []byte("TCKLOG") // 6 bytes

// Sentinel errors.
var (
	ErrBadMagic   = errors.New("store: bad log magic")
	ErrBadVersion = errors.New("store: unsupported log version")
)

// Store owns a directory of per-symbol logs.
type Store struct {
	dir string
}

// New returns a Store rooted at dir.
func New(dir string) *Store { return &Store{dir: dir} }

// Reader is the read-only query surface shared by the plain Store and the
// resident Cache, so callers (e.g. the HTTP server) can hold either.
type Reader interface {
	Range(symbol string, from, to int64) ([]tick.Tick, error)
	Last(symbol string, n int) ([]tick.Tick, error)
}

var _ Reader = (*Store)(nil)

func (s *Store) path(symbol string) string {
	return filepath.Join(s.dir, symbol+".log")
}

func (s *Store) idxPath(symbol string) string {
	return filepath.Join(s.dir, symbol+".idx")
}

func writeHeader(w *bufio.Writer) error {
	var h [headerSize]byte
	copy(h[0:6], magic)
	binary.LittleEndian.PutUint16(h[6:8], version)
	_, err := w.Write(h[:])
	return err
}

// validateHeader reads and checks the 8-byte header at the start of f without
// changing f's write offset (uses ReadAt).
func validateHeader(f *os.File) error {
	var h [headerSize]byte
	if _, err := f.ReadAt(h[:], 0); err != nil {
		return err
	}
	if !bytes.Equal(h[0:6], magic) {
		return ErrBadMagic
	}
	if binary.LittleEndian.Uint16(h[6:8]) != version {
		return ErrBadVersion
	}
	return nil
}

// Appender appends ticks to one symbol's log with a buffered writer. Opened
// with O_APPEND so every write lands at end-of-file.
type Appender struct {
	f       *os.File
	w       *bufio.Writer
	buf     [tick.RecordSize]byte
	idxPath string
	next    int64        // record index of the next record to append
	entries []indexEntry // in-memory sparse index, persisted on Close
}

// OpenAppender opens (creating if needed) the log for symbol. A new file gets
// its header; an existing file has its header validated. The sparse index is
// loaded and, if missing or inconsistent with the log, rebuilt in memory.
func (s *Store) OpenAppender(symbol string) (*Appender, error) {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.path(symbol), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := bufio.NewWriter(f)
	if info.Size() == 0 {
		if err := writeHeader(w); err != nil {
			f.Close()
			return nil, err
		}
	} else if err := validateHeader(f); err != nil {
		f.Close()
		return nil, err
	}
	a := &Appender{f: f, w: w, idxPath: s.idxPath(symbol)}
	a.next, _ = recordCount(info.Size())
	a.entries, err = loadIndex(a.idxPath)
	if err != nil || len(a.entries) != expectedIndexLen(a.next) {
		if a.entries, err = buildIndex(f, a.next); err != nil {
			f.Close()
			return nil, err
		}
	}
	return a, nil
}

// Append encodes and buffers one tick, recording a sparse-index checkpoint at
// every indexStride-th record.
func (a *Appender) Append(t tick.Tick) error {
	if a.next%indexStride == 0 {
		a.entries = append(a.entries, indexEntry{TS: t.TS, RecordIndex: a.next})
	}
	if err := t.EncodeInto(a.buf[:]); err != nil {
		return err
	}
	if _, err := a.w.Write(a.buf[:]); err != nil {
		return err
	}
	a.next++
	return nil
}

// Close flushes buffered records (so the log is durable before the index that
// points into it), persists the sparse index atomically, and closes the file.
func (a *Appender) Close() error {
	if err := a.w.Flush(); err != nil {
		a.f.Close()
		return err
	}
	if err := writeIndex(a.idxPath, a.entries); err != nil {
		a.f.Close()
		return err
	}
	return a.f.Close()
}

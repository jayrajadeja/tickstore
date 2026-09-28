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

func (s *Store) path(symbol string) string {
	return filepath.Join(s.dir, symbol+".log")
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
	f   *os.File
	w   *bufio.Writer
	buf [tick.RecordSize]byte
}

// OpenAppender opens (creating if needed) the log for symbol. A new file gets
// its header; an existing file has its header validated.
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
	return &Appender{f: f, w: w}, nil
}

// Append encodes and buffers one tick.
func (a *Appender) Append(t tick.Tick) error {
	if err := t.EncodeInto(a.buf[:]); err != nil {
		return err
	}
	_, err := a.w.Write(a.buf[:])
	return err
}

// Close flushes buffered records and closes the file.
func (a *Appender) Close() error {
	if err := a.w.Flush(); err != nil {
		a.f.Close()
		return err
	}
	return a.f.Close()
}

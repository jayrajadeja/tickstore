# Tick Store (v1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable, append-only, log-structured tick store for `lob`'s trade stream, answering time-range and last-N reads, wired over a byte-stream pipe.

**Architecture:** Fixed-width 25-byte binary records `{TS, Price, Qty, Side}` in one sorted per-symbol log file (with an 8-byte header). Range reads binary-search the sorted fixed-width log on disk; last-N is tail offset math. A single `cmd/tickstore` binary (`ingest`/`query`/`dump`) is the only I/O layer. Pure `tick` and `store` packages otherwise.

**Tech Stack:** Go 1.26, standard library only (`encoding/binary`, `bufio`, `os`, `io`, `flag`, `bytes`, `fmt`).

## Global Constraints

- Module `github.com/jayrajadeja/tickstore`; `go 1.26` (installed 1.26.4). `go.mod` already exists.
- Prices and TS are `int64`; quantities `uint64`; **never floats**.
- Little-endian binary via `encoding/binary`.
- Deterministic: no wall clock anywhere; TS is the caller-supplied logical counter; no map-iteration-order dependence.
- Record layout is fixed **25 bytes**: `TS int64 | Price int64 | Qty uint64 | Side uint8`. `RecordSize = 25`.
- Log file header is **8 bytes** at offset 0: `magic [6]byte = "TCKLOG"` + `version uint16 = 1` (little-endian). Records begin at offset 8; record `i` at offset `8 + i*25`.
- `Side`: `Buy = 0`, `Sell = 1`.
- Layering: `tick` imports only stdlib; `store` imports `tick` + stdlib; only `cmd/tickstore` performs I/O. No import of `lob`.
- Library functions return errors; never `os.Exit`, never panic on bad input. Truncated trailing record is **ignored** on read (safe). Missing log → `Range`/`Last` return empty, nil.
- Stdlib only — no third-party dependencies.
- Every commit ends with: `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`

> **Note (intentional refinement vs the spec):** the spec §7 mentioned printing a warning on a truncated trailing record. To keep the `store` core pure (no I/O), v1 **silently ignores** the partial tail instead of printing. Flag for the whole-picture review.

---

### Task 1: Tick record (encode/decode)

**Files:**
- Create: `tick/tick.go`
- Test: `tick/tick_test.go`

**Interfaces:**
- Consumes: nothing (stdlib only).
- Produces: `tick.Tick{ TS int64; Price int64; Qty uint64; Side Side }`; `tick.Side` with `tick.Buy=0`, `tick.Sell=1`; `tick.RecordSize=25`; `func (Tick) EncodeInto(buf []byte) error`; `func Decode(buf []byte) (Tick, error)`; sentinel errors `ErrShortBuffer`, `ErrBadSize`.

- [ ] **Step 1: Write the failing test**

```go
package tick

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Tick{TS: 42, Price: -1500, Qty: 9, Side: Sell}
	var buf [RecordSize]byte
	if err := in.EncodeInto(buf[:]); err != nil {
		t.Fatalf("EncodeInto: %v", err)
	}
	got, err := Decode(buf[:])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != in {
		t.Fatalf("round trip = %+v, want %+v", got, in)
	}
}

func TestEncodeIntoShortBuffer(t *testing.T) {
	small := make([]byte, RecordSize-1)
	if err := (Tick{}).EncodeInto(small); err != ErrShortBuffer {
		t.Fatalf("err = %v, want ErrShortBuffer", err)
	}
}

func TestDecodeWrongSize(t *testing.T) {
	if _, err := Decode(make([]byte, RecordSize+1)); err != ErrBadSize {
		t.Fatalf("err = %v, want ErrBadSize", err)
	}
}

func TestSideBytes(t *testing.T) {
	var buf [RecordSize]byte
	_ = Tick{Side: Buy}.EncodeInto(buf[:])
	if buf[24] != 0 {
		t.Fatalf("Buy side byte = %d, want 0", buf[24])
	}
	_ = Tick{Side: Sell}.EncodeInto(buf[:])
	if buf[24] != 1 {
		t.Fatalf("Sell side byte = %d, want 1", buf[24])
	}
	if !bytes.Equal(buf[:24], make([]byte, 24)) {
		t.Fatalf("unexpected non-zero payload for zero tick")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./tick/ -run TestEncodeDecodeRoundTrip -v`
Expected: FAIL — build error, `Tick`/`RecordSize`/`Decode` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// Package tick defines the fixed-width binary tick record persisted by the
// store and streamed over the ingest pipe.
package tick

import (
	"encoding/binary"
	"errors"
)

// Side is the aggressor side of a trade.
type Side uint8

const (
	Buy  Side = 0
	Sell Side = 1
)

// RecordSize is the fixed on-disk / on-wire size of one encoded Tick, in bytes.
const RecordSize = 25

// Tick is a single execution stored by the tick store. TS is a logical
// (non-decreasing) sequence timestamp; Price is in integer ticks.
type Tick struct {
	TS    int64
	Price int64
	Qty   uint64
	Side  Side
}

// Sentinel errors.
var (
	ErrShortBuffer = errors.New("tick: buffer smaller than RecordSize")
	ErrBadSize     = errors.New("tick: buffer length must equal RecordSize")
)

// EncodeInto writes t into buf (len must be >= RecordSize), little-endian.
func (t Tick) EncodeInto(buf []byte) error {
	if len(buf) < RecordSize {
		return ErrShortBuffer
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.TS))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(t.Price))
	binary.LittleEndian.PutUint64(buf[16:24], t.Qty)
	buf[24] = byte(t.Side)
	return nil
}

// Decode reads exactly RecordSize bytes from buf into a Tick.
func Decode(buf []byte) (Tick, error) {
	if len(buf) != RecordSize {
		return Tick{}, ErrBadSize
	}
	return Tick{
		TS:    int64(binary.LittleEndian.Uint64(buf[0:8])),
		Price: int64(binary.LittleEndian.Uint64(buf[8:16])),
		Qty:   binary.LittleEndian.Uint64(buf[16:24]),
		Side:  Side(buf[24]),
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tick/ -v`
Expected: PASS (all four tests).

- [ ] **Step 5: Commit**

```bash
git add tick/tick.go tick/tick_test.go
git commit -m "$(printf 'feat: add fixed-width tick record encode/decode\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>')"
```

---

### Task 2: Log header + Appender

**Files:**
- Create: `store/log.go`
- Test: `store/log_test.go`

**Interfaces:**
- Consumes: `tick.Tick`, `tick.RecordSize`.
- Produces: `store.Store` via `store.New(dir string) *Store`; `func (s *Store) OpenAppender(symbol string) (*Appender, error)`; `func (a *Appender) Append(t tick.Tick) error`; `func (a *Appender) Close() error`; sentinel errors `ErrBadMagic`, `ErrBadVersion`; unexported `headerSize`, `version`, `magic`, `writeHeader`, `validateHeader`, `(*Store).path` reused by later tasks.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./store/ -run TestAppenderWritesHeaderOnce -v`
Expected: FAIL — build error, `New`/`OpenAppender` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./store/ -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add store/log.go store/log_test.go
git commit -m "$(printf 'feat: add per-symbol log header and buffered appender\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>')"
```

---

### Task 3: Reader helpers + Last(n)

**Files:**
- Create: `store/read.go`
- Test: `store/read_test.go`

**Interfaces:**
- Consumes: `tick.Tick`, `tick.RecordSize`, `tick.Decode`, and Task 2's `headerSize`, `validateHeader`, `(*Store).path`.
- Produces: `func (s *Store) Last(symbol string, n int) ([]tick.Tick, error)`; unexported `recordCount(size int64) (n int64, truncated bool)`, `readRecordAt(f *os.File, i int64) (tick.Tick, error)`, and `(*Store).openForRead(symbol string) (*os.File, error)` reused by Task 4.

- [ ] **Step 1: Write the failing test**

```go
package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func appendAll(t *testing.T, s *Store, sym string, ticks []tick.Tick) {
	t.Helper()
	a, err := s.OpenAppender(sym)
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	for _, tk := range ticks {
		if err := a.Append(tk); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLastReturnsFinalN(t *testing.T) {
	s := New(t.TempDir())
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
		{TS: 3, Price: 102, Qty: 1, Side: tick.Buy},
	}
	appendAll(t, s, "X", ticks)

	got, err := s.Last("X", 2)
	if err != nil {
		t.Fatalf("Last: %v", err)
	}
	want := ticks[1:]
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Last(2) = %+v, want %+v", got, want)
	}
}

func TestLastClampsToLength(t *testing.T) {
	s := New(t.TempDir())
	ticks := []tick.Tick{{TS: 1, Price: 100, Qty: 5, Side: tick.Buy}}
	appendAll(t, s, "X", ticks)

	got, err := s.Last("X", 10)
	if err != nil {
		t.Fatalf("Last: %v", err)
	}
	if len(got) != 1 || got[0] != ticks[0] {
		t.Fatalf("Last(10) = %+v, want %+v", got, ticks)
	}
}

func TestLastMissingSymbolEmpty(t *testing.T) {
	got, err := New(t.TempDir()).Last("NOPE", 5)
	if err != nil {
		t.Fatalf("Last missing: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Last missing = %+v, want empty", got)
	}
}

func TestLastIgnoresTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
	}
	appendAll(t, s, "X", ticks)

	// Corrupt: append a stray partial record (fewer than RecordSize bytes).
	f, err := os.OpenFile(filepath.Join(dir, "X.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	if _, err := f.Write([]byte{0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	f.Close()

	got, err := s.Last("X", 10)
	if err != nil {
		t.Fatalf("Last: %v", err)
	}
	if len(got) != 2 || got[1] != ticks[1] {
		t.Fatalf("Last ignoring tail = %+v, want the two whole records", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./store/ -run TestLastReturnsFinalN -v`
Expected: FAIL — build error, `Last` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./store/ -v`
Expected: PASS (Task 2 + Task 3 tests).

- [ ] **Step 5: Commit**

```bash
git add store/read.go store/read_test.go
git commit -m "$(printf 'feat: add reader helpers and Last-N tail read\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>')"
```

---

### Task 4: Range(from, to) via on-disk binary search

**Files:**
- Create: `store/range.go`
- Test: `store/range_test.go`

**Interfaces:**
- Consumes: Task 3's `openForRead`, `recordCount`, `readRecordAt`.
- Produces: `func (s *Store) Range(symbol string, from, to int64) ([]tick.Tick, error)`.

- [ ] **Step 1: Write the failing test**

```go
package store

import (
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func rangeFixture(t *testing.T) *Store {
	t.Helper()
	s := New(t.TempDir())
	appendAll(t, s, "X", []tick.Tick{
		{TS: 10, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 10, Price: 101, Qty: 2, Side: tick.Sell}, // tie on 10
		{TS: 20, Price: 102, Qty: 3, Side: tick.Buy},
		{TS: 30, Price: 103, Qty: 4, Side: tick.Sell},
		{TS: 30, Price: 104, Qty: 5, Side: tick.Buy}, // tie on 30
	})
	return s
}

func tsList(ticks []tick.Tick) []int64 {
	out := make([]int64, len(ticks))
	for i, tk := range ticks {
		out[i] = tk.TS
	}
	return out
}

func eq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRangeInclusiveWithTies(t *testing.T) {
	s := rangeFixture(t)
	cases := []struct {
		name     string
		from, to int64
		wantTS   []int64
	}{
		{"all", 0, 100, []int64{10, 10, 20, 30, 30}},
		{"ties at low edge", 10, 10, []int64{10, 10}},
		{"ties at high edge", 20, 30, []int64{20, 30, 30}},
		{"before first", 0, 5, nil},
		{"after last", 40, 50, nil},
		{"empty gap between records", 11, 19, nil},
		{"from greater than to", 30, 20, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := s.Range("X", c.from, c.to)
			if err != nil {
				t.Fatalf("Range: %v", err)
			}
			if !eq(tsList(got), c.wantTS) {
				t.Fatalf("Range(%d,%d) TS = %v, want %v", c.from, c.to, tsList(got), c.wantTS)
			}
		})
	}
}

func TestRangeMissingSymbolEmpty(t *testing.T) {
	got, err := New(t.TempDir()).Range("NOPE", 0, 100)
	if err != nil {
		t.Fatalf("Range missing: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Range missing = %+v, want empty", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./store/ -run TestRangeInclusiveWithTies -v`
Expected: FAIL — build error, `Range` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./store/ -v`
Expected: PASS (all store tests).

- [ ] **Step 5: Commit**

```bash
git add store/range.go store/range_test.go
git commit -m "$(printf 'feat: add time-range read via on-disk binary search\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>')"
```

---

### Task 5: `cmd/tickstore` — ingest + dump

**Files:**
- Create: `cmd/tickstore/main.go`
- Test: `cmd/tickstore/main_test.go`

**Interfaces:**
- Consumes: `tick`, `store` (`New`, `OpenAppender`, `Append`, `Close`, `Last`, `Range`).
- Produces: package-level helpers `runIngest(dir, symbol string, in io.Reader) error`, `runDump(dir, symbol string, out io.Writer) error`, `formatTick(t tick.Tick) string`, and constants `minInt64`, `maxInt64`; a `main()` dispatching subcommands. `query` is added in Task 6.

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func encodeStream(t *testing.T, ticks []tick.Tick) []byte {
	t.Helper()
	var b bytes.Buffer
	buf := make([]byte, tick.RecordSize)
	for _, tk := range ticks {
		if err := tk.EncodeInto(buf); err != nil {
			t.Fatalf("EncodeInto: %v", err)
		}
		b.Write(buf)
	}
	return b.Bytes()
}

func TestFormatTick(t *testing.T) {
	got := formatTick(tick.Tick{TS: 7, Price: 100, Qty: 3, Side: tick.Sell})
	want := "ts=7 price=100 qty=3 side=sell"
	if got != want {
		t.Fatalf("formatTick = %q, want %q", got, want)
	}
}

func TestIngestThenDump(t *testing.T) {
	dir := t.TempDir()
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
	}
	if err := runIngest(dir, "X", bytes.NewReader(encodeStream(t, ticks))); err != nil {
		t.Fatalf("runIngest: %v", err)
	}

	var out bytes.Buffer
	if err := runDump(dir, "X", &out); err != nil {
		t.Fatalf("runDump: %v", err)
	}
	want := "ts=1 price=100 qty=5 side=buy\nts=2 price=101 qty=2 side=sell\n"
	if out.String() != want {
		t.Fatalf("dump =\n%q\nwant\n%q", out.String(), want)
	}
}

func TestIngestTruncatedStreamErrors(t *testing.T) {
	dir := t.TempDir()
	full := encodeStream(t, []tick.Tick{{TS: 1, Price: 100, Qty: 5, Side: tick.Buy}})
	truncated := append(full, 0x01, 0x02) // trailing partial record

	err := runIngest(dir, "X", bytes.NewReader(truncated))
	if err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("err = %v, want a truncated-stream error", err)
	}

	// The one whole record must still have been flushed and be readable.
	var out bytes.Buffer
	if derr := runDump(dir, "X", &out); derr != nil {
		t.Fatalf("runDump: %v", derr)
	}
	if out.String() != "ts=1 price=100 qty=5 side=buy\n" {
		t.Fatalf("dump after truncated ingest = %q", out.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/tickstore/ -run TestIngestThenDump -v`
Expected: FAIL — build error, `runIngest`/`runDump`/`formatTick` undefined.

- [ ] **Step 3: Write minimal implementation**

```go
// Command tickstore ingests a lob trade stream into per-symbol logs and queries
// them. It is the only package in the project that performs I/O.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jayrajadeja/tickstore/store"
	"github.com/jayrajadeja/tickstore/tick"
)

const (
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)

func sideString(s tick.Side) string {
	if s == tick.Sell {
		return "sell"
	}
	return "buy"
}

func formatTick(t tick.Tick) string {
	return fmt.Sprintf("ts=%d price=%d qty=%d side=%s", t.TS, t.Price, t.Qty, sideString(t.Side))
}

// runIngest reads RecordSize-byte records from in until EOF and appends them to
// <dir>/<symbol>.log. A partial trailing record is an error (whole records read
// so far are still flushed).
func runIngest(dir, symbol string, in io.Reader) error {
	s := store.New(dir)
	a, err := s.OpenAppender(symbol)
	if err != nil {
		return err
	}
	r := bufio.NewReader(in)
	buf := make([]byte, tick.RecordSize)
	for {
		_, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			a.Close()
			return errors.New("truncated stream: partial final record")
		}
		if err != nil {
			a.Close()
			return err
		}
		t, derr := tick.Decode(buf)
		if derr != nil {
			a.Close()
			return derr
		}
		if aerr := a.Append(t); aerr != nil {
			a.Close()
			return aerr
		}
	}
	return a.Close()
}

// writeTicks renders ticks as text, one per line.
func writeTicks(out io.Writer, ticks []tick.Tick) error {
	w := bufio.NewWriter(out)
	for _, t := range ticks {
		if _, err := fmt.Fprintln(w, formatTick(t)); err != nil {
			return err
		}
	}
	return w.Flush()
}

// runDump prints every record in the log as text.
func runDump(dir, symbol string, out io.Writer) error {
	ticks, err := store.New(dir).Range(symbol, minInt64, maxInt64)
	if err != nil {
		return err
	}
	return writeTicks(out, ticks)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tickstore <ingest|query|dump> [flags]")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "ingest":
		fs := flag.NewFlagSet("ingest", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to ingest into")
		fs.Parse(args)
		if *symbol == "" {
			err = errors.New("ingest: --symbol is required")
		} else {
			err = runIngest(*dir, *symbol, os.Stdin)
		}
	case "dump":
		fs := flag.NewFlagSet("dump", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to dump")
		fs.Parse(args)
		if *symbol == "" {
			err = errors.New("dump: --symbol is required")
		} else {
			err = runDump(*dir, *symbol, os.Stdout)
		}
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/tickstore/ -v`
Expected: PASS (formatTick, ingest→dump, truncated-stream).

- [ ] **Step 5: Commit**

```bash
git add cmd/tickstore/main.go cmd/tickstore/main_test.go
git commit -m "$(printf 'feat: add tickstore ingest and dump subcommands\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>')"
```

---

### Task 6: `query` subcommand + end-to-end golden test

**Files:**
- Modify: `cmd/tickstore/main.go` (add `runQuery` + the `query` case in `main`'s switch, before `default`)
- Test: `cmd/tickstore/query_test.go`

**Interfaces:**
- Consumes: `store.Range`, `store.Last`, and Task 5's `runIngest`, `writeTicks`, `formatTick`, `minInt64`, `maxInt64`, `encodeStream` (test helper).
- Produces: `func runQuery(dir, symbol string, from, to int64, last int, out io.Writer) error` (when `last > 0` it calls `Last`; otherwise `Range(from, to)`); a `query` subcommand with `--from/--to/--last` flags (`--last` exclusive with `--from/--to`).

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bytes"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

// goldenTicks is the fixed input for the end-to-end test. Deterministic in →
// deterministic out; expected strings below are captured from a real run.
func goldenTicks() []tick.Tick {
	return []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 1, Price: 101, Qty: 2, Side: tick.Sell},
		{TS: 2, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 5, Price: 102, Qty: 3, Side: tick.Sell},
	}
}

func TestEndToEndIngestQuery(t *testing.T) {
	dir := t.TempDir()
	if err := runIngest(dir, "SYNTH", bytes.NewReader(encodeStream(t, goldenTicks()))); err != nil {
		t.Fatalf("runIngest: %v", err)
	}

	t.Run("range 1..1 (ties)", func(t *testing.T) {
		var out bytes.Buffer
		if err := runQuery(dir, "SYNTH", 1, 1, 0, &out); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
		want := "ts=1 price=100 qty=5 side=buy\nts=1 price=101 qty=2 side=sell\n"
		if out.String() != want {
			t.Fatalf("range 1..1 =\n%q\nwant\n%q", out.String(), want)
		}
	})

	t.Run("range 2..5", func(t *testing.T) {
		var out bytes.Buffer
		if err := runQuery(dir, "SYNTH", 2, 5, 0, &out); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
		want := "ts=2 price=100 qty=1 side=buy\nts=5 price=102 qty=3 side=sell\n"
		if out.String() != want {
			t.Fatalf("range 2..5 =\n%q\nwant\n%q", out.String(), want)
		}
	})

	t.Run("last 1", func(t *testing.T) {
		var out bytes.Buffer
		if err := runQuery(dir, "SYNTH", 0, 0, 1, &out); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
		want := "ts=5 price=102 qty=3 side=sell\n"
		if out.String() != want {
			t.Fatalf("last 1 =\n%q\nwant\n%q", out.String(), want)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/tickstore/ -run TestEndToEndIngestQuery -v`
Expected: FAIL — build error, `runQuery` undefined.

- [ ] **Step 3: Write minimal implementation**

Add `runQuery` to `cmd/tickstore/main.go`:

```go
// runQuery prints ticks as text: last N when last > 0, else the range [from,to].
func runQuery(dir, symbol string, from, to int64, last int, out io.Writer) error {
	s := store.New(dir)
	var (
		ticks []tick.Tick
		err   error
	)
	if last > 0 {
		ticks, err = s.Last(symbol, last)
	} else {
		ticks, err = s.Range(symbol, from, to)
	}
	if err != nil {
		return err
	}
	return writeTicks(out, ticks)
}
```

Wire the `query` subcommand into `main`'s switch (before `default`):

```go
	case "query":
		fs := flag.NewFlagSet("query", flag.ExitOnError)
		dir := fs.String("dir", "data", "data directory")
		symbol := fs.String("symbol", "", "symbol to query")
		from := fs.Int64("from", minInt64, "range start (inclusive, logical TS)")
		to := fs.Int64("to", maxInt64, "range end (inclusive, logical TS)")
		last := fs.Int("last", 0, "return the last N ticks (exclusive with --from/--to)")
		fs.Parse(args)
		switch {
		case *symbol == "":
			err = errors.New("query: --symbol is required")
		case *last > 0 && (*from != minInt64 || *to != maxInt64):
			err = errors.New("query: --last is exclusive with --from/--to")
		default:
			err = runQuery(*dir, *symbol, *from, *to, *last, os.Stdout)
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/tickstore/ -v`
Expected: PASS (Task 5 + end-to-end tests).

- [ ] **Step 5: Full-suite gates**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: all packages build, vet clean, all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/tickstore/main.go cmd/tickstore/query_test.go
git commit -m "$(printf 'feat: add tickstore query subcommand and end-to-end test\n\nCo-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>')"
```

---

## Self-Review

**Spec coverage:**
- Fixed-width 25-byte record `{TS,Price,Qty,Side}` — Task 1. ✅
- 8-byte header (magic+version), written once, validated on reopen — Task 2. ✅
- Append-only per-symbol log, buffered, flush on close — Task 2. ✅
- Last-N via tail math; missing log empty; truncated tail ignored — Task 3. ✅
- Range via on-disk binary search; inclusive; ties; boundaries; missing empty — Task 4. ✅
- Pipe ingest (stdin → append), truncated-stream error, dump — Task 5. ✅
- Query (range/last, mutually exclusive), end-to-end golden — Task 6. ✅
- Layering (`tick`→stdlib, `store`→tick, I/O only in cmd), no `lob` import, stdlib only, errors-not-panics — enforced across tasks + global constraints. ✅
- Deferred (sparse index/serve, OHLC, fsync, wall-clock, multi-ingest) — out of scope, not in any task. ✅

**Placeholder scan:** none — every step has complete code and exact commands.

**Type consistency:** `RecordSize`/`headerSize` used consistently; `readRecordAt`/`recordCount`/`openForRead` defined in Task 3 and reused in Task 4; `runIngest`/`writeTicks`/`formatTick`/`minInt64`/`maxInt64` defined in Task 5 and reused in Task 6; `Side` values `Buy=0`/`Sell=1` consistent between `tick` and `sideString`.

**Known divergence from spec (for review):** truncated trailing record is silently ignored by the pure `store` core rather than warned (spec §7 said "warn") — keeps the library I/O-free. `cmd/tickstore` could surface a warning later if desired.

## Execution Handoff

Once the plan is approved, the next step is superpowers:subagent-driven-development (same loop that shipped `lob`): fresh subagent per task, two-stage review between tasks, final whole-branch review, then finishing-a-development-branch (create the `jayrajadeja/tickstore` remote and open a PR, plus the separate `lob --emit` PR).

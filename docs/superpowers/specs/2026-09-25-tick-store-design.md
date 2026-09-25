# Tick Store (v1) — Design Spec

**Project:** `tickstore` — project **B** in the A → B → C series
(lob matching engine → **tick store** → storage engine).

**Goal:** A durable, append-only, log-structured store for the trade ("tick")
stream produced by the `lob` matching engine, with two reads: **time-range scan**
and **last-N**. Decoupled from `lob` via a byte-stream pipe.

**Non-goals (v1):** OHLC/candles, market orders, wall-clock timestamps, an
in-memory sparse index, a long-lived query server, concurrent readers/writers,
compression, networking/RabbitMQ/AWS. Each is a deliberate future increment.

---

## 1. Context & the itch

`lob` emits a deterministic stream of trades. B stores that tape durably and
answers "give me the trades in this range" and "give me the last N trades." The
learning target is a **log-structured storage engine**: fixed-width binary
records, an append-only log, and reads that exploit the log's structure — the
foundation for project C.

This spec is deliberately scoped tight (as `lob` v1 was): a pure core plus a
single I/O command. Set = **"Tape"** (range scan + last-N).

## 2. Re-audit decisions (why this differs from the first sketch)

The initial design was re-audited before writing. Four decisions changed:

1. **Logical time, not wall-clock.** `lob`'s `TS` is a **monotonic logical
   sequence counter** (determinism forbids reading the clock). So a "range"
   query is over logical TS, and there is no notion of seconds. All "time" in
   this spec means **logical TS**. Real wall-clock arrival stamps are a future
   increment (and would trade away pure determinism).

2. **Range reads use on-disk binary search; no in-memory index in v1.** Because
   the log is **fixed-width and sorted by TS**, a range read binary-searches the
   log directly on disk in O(log n) `ReadAt`s — no scan, less code. An in-memory
   sparse index only pays off in a long-lived process serving many reads; our
   command is a short-lived one-shot, so the index would be rebuilt (O(file)) on
   every invocation for no benefit. The **sparse index is deferred** to the
   version that adds a query server — where it earns its keep (its own post).

3. **Records store `{TS, Price, Qty, Side}`, not order IDs.** A tape/replay
   consumer never uses the order-book-internal `MakerID`/`TakerID`; real
   time-&-sales data omits them. The genuinely useful field is the **aggressor
   side** (did a buy or a sell hit the book). So the record drops the two IDs and
   adds `Side`.

4. **Each log file carries a small header** (magic + version) so the on-disk
   format can evolve.

## 3. Architecture

Append-only, log-structured, one **log file per symbol**. Pure core + a single
I/O command, mirroring `lob`.

```
tick/            Tick record + fixed-width binary encode/decode   (pure)
store/           log header, append writer, range/last-N reads    (imports tick)
cmd/tickstore/   the ONLY I/O layer: ingest | query | dump
```

Dependency arrows point one way: `cmd/tickstore` → `store` → `tick`. `tick` and
`store` contain zero I/O beyond the file handles `store` is handed. `tickstore`
does **not** import `lob` — the two are decoupled; the wire format (§6) is the
only contract between them.

## 4. On-disk layout

Per symbol: `<dir>/<SYMBOL>.log`.

### 4.1 File header (8 bytes, once, at offset 0)

```
magic  [6]byte  = "TCKLOG"
version uint16  = 1            (little-endian)
```

Records begin at byte offset **8**. On open, a missing file is created with this
header; an existing file's magic and version are validated (mismatch → error).

### 4.2 Record (fixed 25 bytes, little-endian)

```
TS     int64    8      logical sequence timestamp (non-decreasing)
Price  int64    8      execution price in integer ticks
Qty    uint64   8      executed quantity
Side   uint8    1      aggressor side: 0 = Buy, 1 = Sell
                ----
                25 bytes
```

Record `i` (0-based) lives at byte offset `8 + i*25`. Record count =
`(fileSize - 8) / 25`. **Symbol is not in the record** — it is the file name —
so records stay fixed-width and multi-symbol is free.

### 4.3 Invariants

- **Sorted by TS:** `lob` emits trades in non-decreasing `TS`, so appends are
  monotonic and each log is sorted by TS. Ties (a sweep emits several trades at
  the same `TS`) are allowed; reads handle equal keys by scanning.
- **Append-only:** records are only appended; existing bytes are never rewritten.

## 5. Packages & interfaces

### 5.1 `tick`

```go
type Side uint8
const (
    Buy  Side = 0
    Sell Side = 1
)

type Tick struct {
    TS    int64
    Price int64
    Qty   uint64
    Side  Side
}

const RecordSize = 25

// EncodeInto writes t as RecordSize little-endian bytes into buf (len >= 25).
func (t Tick) EncodeInto(buf []byte) error
// Decode reads exactly RecordSize bytes into a Tick.
func Decode(buf []byte) (Tick, error)   // errors if len(buf) != RecordSize
```

### 5.2 `store`

```go
// Store owns a directory of per-symbol logs.
func New(dir string) *Store

// Appender appends to one symbol's log with a buffered writer.
func (s *Store) OpenAppender(symbol string) (*Appender, error) // creates + writes header if new; validates header if existing
func (a *Appender) Append(t tick.Tick) error
func (a *Appender) Close() error                                // flushes the buffer

// Range returns all ticks with from <= TS <= to (logical time), ascending.
// Missing log => empty slice, nil error. Uses on-disk binary search.
func (s *Store) Range(symbol string, from, to int64) ([]tick.Tick, error)

// Last returns the final n ticks (fewer if the log is shorter), ascending.
// Missing log => empty slice, nil error. Uses tail offset math (no scan).
func (s *Store) Last(symbol string, n int) ([]tick.Tick, error)
```

- **Range** binary-searches for the first record with `TS >= from` (each probe is
  a `ReadAt` of 25 bytes at `8 + mid*25`, decode, compare), then reads forward
  emitting while `TS <= to`.
- **Last** computes `count = (fileSize-8)/25`, seeks to `8 + max(0, count-n)*25`,
  reads the remaining records.

### 5.3 `cmd/tickstore`

One binary, three subcommands (the only package doing I/O: `os`, `bufio`,
`flag`, `fmt`):

```
tickstore ingest --dir data --symbol BTC-USD
    Read 25-byte records from stdin until EOF; append to <dir>/<sym>.log.

tickstore query  --dir data --symbol BTC-USD --from T1 --to T2
tickstore query  --dir data --symbol BTC-USD --last N
    Print matching ticks to stdout as text (one per line).

tickstore dump   --dir data --symbol BTC-USD
    Print every record in the log as text.
```

`--from/--to` and `--last` are mutually exclusive on `query`.

## 6. Data flow & the `lob` contract

```
lob-replay --emit --symbol BTC-USD  |  tickstore ingest --symbol BTC-USD --dir data
        (raw 25-byte records)            (header-on-create, then append)

tickstore query --symbol BTC-USD --dir data --from 100 --to 500   # range
tickstore query --symbol BTC-USD --dir data --last 20             # tape
tickstore dump  --symbol BTC-USD --dir data                       # whole log
```

**Wire format:** a bare stream of the §4.2 25-byte records — **no file header on
the wire** (the header is a disk artifact `ingest` writes when creating a log),
and no delimiters (fixed size self-frames). One symbol per ingest stream in v1;
`ingest --symbol` names the target log.

**External dependency (separate `lob` PR, out of scope for this spec):** `lob`
gains a `cmd/replay --emit` mode that writes this 25-byte stream to stdout
(default output stays the human summary), which requires `lob`'s emitted trade to
carry the **aggressor side** (the taker's side, known at match time). This spec
defines the byte contract; the `lob` change conforms to it.

## 7. Error handling & durability

- Library functions **return errors; never `os.Exit`, never panic on bad input.**
  `cmd/tickstore` surfaces errors to stderr and exits non-zero.
- **Truncated tail:** if `(fileSize - 8)` is not a multiple of 25 (a partial or
  crashed write), the trailing partial record is **ignored** on read and a
  warning is printed; whole records remain readable.
- **Truncated ingest stream:** if stdin ends mid-record (bytes read since the last
  full record is nonzero and < 25), `ingest` reports an error and exits non-zero
  after flushing the whole records it did read.
- **Bad header:** wrong magic or unknown version → error, no read attempted.
- **Missing/empty log** on `query`/`dump`: `Range`/`Last` return an empty result
  with no error; `dump` prints nothing.
- **Durability:** buffered writes, flushed on `Appender.Close()`. Per-record
  `fsync` (durability vs throughput) is a future increment.
- Mid-file corruption (not the tail) is out of scope; the append-only invariant
  is assumed to hold.

## 8. Testing (TDD)

**`tick` unit:**
- `EncodeInto` → `Decode` round-trip reproduces the exact `Tick`.
- `Decode` rejects a buffer whose length != `RecordSize`.
- `EncodeInto` rejects a short buffer.
- `Side` encodes/decodes 0=Buy, 1=Sell.

**`store` unit:**
- Append N ticks, then `Last(k)` returns the final k in order; `Last(n > N)`
  returns all N.
- `Range` boundaries: `from` before the first TS; `to` after the last; a window
  fully inside; `from == to` on an existing TS; an empty window between two
  records; **ties** (several records share a TS at the range edge — all returned).
- Header is written exactly once on create and validated on reopen; reopening
  with a wrong magic/version errors.
- Truncated tail (append a stray partial record's bytes) is ignored on read with
  a warning; preceding whole records still read.
- Missing symbol → `Range`/`Last` return empty, nil.

**End-to-end golden (deterministic, self-contained):**
- A fixed, in-repo byte fixture (a captured stream of 25-byte records) is piped
  through `ingest`, then `query --from/--to` and `--last` assert the **exact**
  expected ticks. Because the format and reads are deterministic, the golden
  values are captured from a real `ingest`+`query` run, never hand-written.
- The full cross-repo pipe (`lob-replay --emit | tickstore ingest`) is documented
  in the README as an integration smoke test (depends on the separate `lob` PR),
  not a committed unit test.

**Gates:** `go build ./...`, `go vet ./...`, `go test ./...` all green.

## 9. Global constraints

- Module `github.com/jayrajadeja/tickstore`; Go 1.26 (installed 1.26.4).
- Prices and TS are `int64`; quantities `uint64`; **never floats**.
- Little-endian binary encoding via `encoding/binary`.
- Deterministic: no wall clock anywhere; TS is the caller-supplied logical
  counter; no dependence on map-iteration order.
- Layering: `tick` imports only stdlib; `store` imports `tick` + stdlib; only
  `cmd/tickstore` performs I/O. No import of `lob`.
- **Stdlib only** — no third-party dependencies.
- Every commit ends with:
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`

## 10. Future increments (each its own spec/plan/post)

- **Sparse in-memory index + `tickstore serve`** (long-lived query mode where the
  index earns its keep; compare against on-disk binary search).
- **OHLC / candle aggregation** (Set 3 — the first "finance math").
- **Persisted index snapshot** (skip the startup scan).
- **`fsync` durability modes**; crash-consistency tests.
- **Symbol-in-stream multi-ingest**; concurrent reader/writer; compression;
  wall-clock arrival stamps; network / RabbitMQ / AWS deployment.

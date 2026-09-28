# tickstore v2 — persistent sparse index (Project D)

**Status:** approved design (brainstormed autonomously under autopilot; author away)
**Date:** 2026-09-28
**Series:** A `lob` → B `tickstore` → C `candle` → **D: B's storage-engine upgrade**

## Purpose

`tickstore`'s `Range` already binary-searches the fixed-width, sorted log — it does
**not** scan the whole file. But a cold binary search issues **O(log N) scattered
`pread` syscalls**, each a seek into a potentially huge log. This project adds a
**persistent sparse index** — a small `SYMBOL.idx` sidecar holding `{TS, recordIndex}`
every K records — so a `Range` becomes *one tiny in-memory index search + one bounded
block read* instead of ~log₂N scattered seeks.

It is the canonical pattern (Kafka `.index`, LevelDB, Postgres BRIN) and the
prerequisite for a future variable-width/compressed block format (where offsets can
no longer be computed arithmetically and an index becomes mandatory).

**Honest scope of the win:** fewer cold `pread` syscalls per query and the
groundwork for compression — *not* a big-O leap (both old and new are sub-linear).
The benchmark's headline metric is **ReadAt count per Range**, which drops from
~log₂N to ~1–2.

## Non-goals (v2, deferred)

- **Block compression / variable-width records** (the index's eventual payoff).
- **Segmented / rolling logs** (multi-file, segment min/max skipping) — a different
  upgrade; this one keeps the single log file per symbol.
- **Incremental per-append `fsync` durability** of the index — the index is a
  derived, rebuildable artifact; correctness never depends on it.
- **mmap reads, concurrency, a live index server.**
- Configurable stride via CLI (a `const` for v2; note it as future work).

## Core principle: the index is an optimisation, never a source of truth

The log remains the sole authority. The index only ever provides a **lower-bound
record position**; the actual answer is always refined against the real log by
scanning forward. Consequences:

- A **missing** `.idx` → fall back to today's in-log binary search. Correct, just
  slower. Old logs written by v1 keep working untouched.
- A **stale/short** `.idx` (crash between log append and index write, so its length
  ≠ `expectedIndexLen(count)`) → ignored on read; the fallback in-log binary search
  is used, which is always correct. The index is rebuilt on the next `OpenAppender`
  or `reindex`.
- The index is **rebuilt from the log** whenever it is missing or inconsistent
  (on `OpenAppender`, or explicitly via `reindex`). It is disposable.

## On-disk format: `SYMBOL.idx`

Mirrors the log's framing:

- **Header — 8 bytes:** `"TCKIDX"` (6) + `version uint16` LE (`= 1`).
- **Entries — 16 bytes each, little-endian:** `{ TS int64 [0:8] | RecordIndex int64 [8:16] }`.
  Entry `i` describes log record number `i*K`: `TS` = that record's TS, `RecordIndex`
  = `i*K`. The log byte offset is derivable: `headerSize + RecordIndex*RecordSize`.

`K` (`indexStride`) is a package constant, **128 records** (≈3.2 KB blocks). Entries
inherit the log's non-decreasing-TS ordering, so the index is itself binary-searchable
by TS. A trailing partial entry (file size not `8 + 16·m`) is ignored, exactly as the
log ignores a truncated trailing record.

## Components

### `store/index.go` (new) — index format + build/load (pure-ish)
- `type indexEntry struct { TS, RecordIndex int64 }`.
- `const indexStride = 128`; `const idxHeaderSize = 8`; `idxMagic = "TCKIDX"`; `idxVersion = 1`.
- `encodeIndexEntry(e) [16]byte` / `decodeIndexEntry(buf) indexEntry`.
- `writeIndex(path string, entries []indexEntry) error` — atomic: write `path+".tmp"`
  (header + entries), `Close`, `os.Rename` over `path`.
- `loadIndex(path string) ([]indexEntry, error)` — validate header (`ErrBadMagic`/
  `ErrBadVersion`), read whole entries, ignore a partial tail; missing file → `(nil, nil)`.
- `buildIndex(ra io.ReaderAt, count int64) ([]indexEntry, error)` — sample records
  `0, K, 2K, … < count`, reading each TS; returns entries. Used by rebuild and `reindex`.
- `expectedIndexLen(count int64) int` — `count>0 ? (count-1)/K + 1 : 0`; used to detect
  a consistent existing index.

### `store/log.go` (modified) — Appender maintains the index in memory
- `Appender` gains: `dir, symbol string`, `next int64` (index of the next record to
  write), `entries []indexEntry`.
- `OpenAppender`: after header handling, set `next = recordCount(size)`; then
  `entries, _ = loadIndex(idxPath)`; if `len(entries) != expectedIndexLen(next)` (or
  load errored) rebuild via `buildIndex` over the just-opened file. (Rebuild reads
  `next/K` records — cheap, one-time.)
- `Append`: if `next % indexStride == 0`, append `{t.TS, next}` to `entries`; then
  `next++`. (No file IO per append.)
- `Close`: flush the log writer first (records must be durable before the index that
  points at them), then `writeIndex(idxPath, entries)` atomically. An index write
  error is returned but the log is already safe.
- `s.idxPath(symbol)` helper: `symbol + ".idx"` in `s.dir`.

### `store/read.go` + `store/range.go` (modified) — index-aware Range with fallback
- Refactor the raw read helpers to take `io.ReaderAt` (an `*os.File` satisfies it),
  isolating the seek logic and making `pread` count observable in tests/benchmarks:
  - `readRecordAt(ra io.ReaderAt, i int64) (tick.Tick, error)` (unchanged behavior).
  - `readBlock(ra io.ReaderAt, start, n int64) ([]tick.Tick, error)` — one `ReadAt`
    of `n` records into a `[]byte`, decoded in memory (the bounded block read).
- `Range` opens the log, `Stat`s for `count`, then tries `loadIndex`:
  - **Index present & non-empty:** binary-search `entries` for the greatest entry with
    `TS <= from` → `base` (record index; `base=0` if `from <= entries[0].TS`). The
    upper edge of the candidate window is the next entry's `RecordIndex` (or `count`).
    `readBlock` that window (≤ `K+1` records) once; linear-scan it for the first record
    with `TS >= from`; then continue forward — reading subsequent whole blocks as
    needed — collecting until `TS > to`.
  - **Index absent:** existing in-log binary search (unchanged code path).
  - Both paths must return **identical** results for every query (a property test).
- `Last` is unchanged — it is already O(1) offset math and needs no index.

### `cmd/tickstore/main.go` (modified) — `reindex` subcommand
- `tickstore reindex --dir D --symbol S`: open the log read-only, `buildIndex`,
  `writeIndex`. Reports entries written. Also refreshes a stale index. Wired into the
  existing `switch`, matching the `ingest/query/dump` style.
- `ingest` already produces an index for free now (via `Appender.Close`).

## Data flow

```
ingest:   stdin → Appender.Append (buffer log + collect entries) → Close
                                                   ├─► flush SYMBOL.log
                                                   └─► atomic write SYMBOL.idx
range:    loadIndex(SYMBOL.idx) ──► binary-search entries (in memory)
                                     │  base record index (lower bound)
                                     ▼
          readBlock(SYMBOL.log, base, ≤K)  ──► refine + forward-scan ──► []Tick
          (no index → in-log binary search, unchanged)
```

## Error handling

| Condition | Behavior |
|---|---|
| `.idx` missing | fallback to in-log binary search (correct) |
| `.idx` bad magic/version | `ErrBadMagic`/`ErrBadVersion` → treat as missing (fallback) + log to stderr on reindex |
| `.idx` trailing partial entry | ignore the partial tail |
| `.idx` shorter/stale vs log (length ≠ `expectedIndexLen`) | ignored on read (fallback stays correct); rebuilt on next `OpenAppender`/`reindex` |
| `writeIndex` fails on Close | log already flushed+durable; return the error |
| `reindex` on missing log | error (no log to index) |

## Testing

- **Format (`index_test.go`):** golden 16-byte entry; header round-trip; `writeIndex`
  then `loadIndex` round-trips; partial-tail truncation ignored; bad magic/version.
- **Build:** `buildIndex` over a known N-record log yields entries at `0,K,2K,…` with
  the right TS and `expectedIndexLen(N)` count.
- **Appender:** ingest N (N spanning several strides); reopen store; `loadIndex`
  consistent with the log; append more, reopen, still consistent; simulate crash by
  truncating/deleting `.idx` → next `OpenAppender` rebuilds an identical index.
- **Range parity (the core guarantee):** for a generated log and many `(from,to)`
  pairs — including boundaries at exact stride multiples, `from` before first / after
  last, empty range, single-record — assert **indexed Range == fallback Range** (delete
  `.idx` to force fallback) byte-for-byte.
- **Seek reduction:** wrap the log in a counting `io.ReaderAt`; assert an indexed
  `Range` issues far fewer `ReadAt` calls than the fallback on a large log
  (the headline metric).
- **Stale index self-correction:** hand-write a short/partial `.idx`, assert `Range`
  still returns the correct full result.
- **CLI:** `reindex` builds a loadable index; `query` results unchanged before/after
  `reindex`; `ingest` produces an index whose entry count is `expectedIndexLen`.
- **Benchmark (`range_bench_test.go`):** large temp log (e.g. 1–5 M records);
  `BenchmarkRangeIndexed` vs `BenchmarkRangeFallback`, reporting ns/op and a custom
  `preads/op` counter.

## Success criteria

- `go build ./... && go vet ./... && go test ./...` green.
- Indexed and fallback `Range` are provably identical across a wide query sweep.
- Indexed `Range` issues O(1–2) block `ReadAt`s where the fallback issues ~log₂N.
- Old v1 logs (no `.idx`) still query correctly; `reindex` upgrades them.
- The index is fully rebuildable and never a correctness dependency.
- Deterministic: same log → same `.idx` bytes.

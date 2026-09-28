# tickstore v2 sparse index — implementation plan

Spec: `docs/superpowers/specs/2026-09-28-sparse-index-design.md`

TDD throughout (RED → GREEN). Full gate `go build ./... && go vet ./... &&
go test ./...` green after every task. stdlib only. Deterministic. Personal git
identity; `Co-authored-by: Copilot` trailer on every commit. The index is always an
optimisation — every task must preserve "results identical to the log."

## Task 1 — `store/index.go`: index format + build/load

**Files:** `store/index.go`, `store/index_test.go`

- Consts: `indexStride = 128`, `idxHeaderSize = 8`, `idxEntrySize = 16`,
  `idxMagic = []byte("TCKIDX")`, `idxVersion = uint16(1)`.
- Sentinels: `ErrBadIdxMagic`, `ErrBadIdxVersion`.
- `type indexEntry struct { TS, RecordIndex int64 }`.
- `encodeIndexEntry(e indexEntry) [idxEntrySize]byte` / `decodeIndexEntry([]byte) indexEntry` (LE).
- `expectedIndexLen(count int64) int` — `count<=0 → 0`; else `int((count-1)/indexStride)+1`.
- `buildIndex(ra io.ReaderAt, count int64) ([]indexEntry, error)` — for `j := 0;
  j < count; j += indexStride` read record `j`'s TS via `readRecordAt`, append
  `{TS, j}`.
- `writeIndex(path string, entries []indexEntry) error` — write `path+".tmp"`
  (header + entries) via a buffered writer, `Sync`+`Close`, then `os.Rename` over
  `path` (atomic).
- `loadIndex(path string) ([]indexEntry, error)` — missing file → `(nil, nil)`;
  validate 8-byte header; read `(size-8)/16` whole entries (ignore partial tail).

**Tests (RED first):**
- golden entry bytes: `{TS:1, RecordIndex:128}` → known 16 hex bytes; decode back.
- `expectedIndexLen`: 0→0, 1→1, 128→1, 129→2, 256→2, 257→3.
- `writeIndex`→`loadIndex` round-trip on a few entries.
- `loadIndex` missing → nil,nil; bad magic → `ErrBadIdxMagic`; version 2 →
  `ErrBadIdxVersion`; extra 5 trailing bytes → ignored.

**Gate:** `go test ./store/`.

## Task 2 — `store/log.go`: Appender maintains + persists the index

**Files:** `store/log.go`, `store/log_test.go` (extend)

- `s.idxPath(symbol)` helper.
- `Appender` fields: add `idxPath string`, `next int64`, `entries []indexEntry`.
- `OpenAppender`: after header setup, `a.next = recordCount(size)`;
  `a.entries, err = loadIndex(idxPath)`; if `err != nil || len(a.entries) !=
  expectedIndexLen(a.next)` then `a.entries, err = buildIndex(f, a.next)` (rebuild).
- `Append`: `if a.next % indexStride == 0 { a.entries = append(a.entries,
  indexEntry{t.TS, a.next}) }`; encode+write; `a.next++`.
- `Close`: `w.Flush()` first (return on error), then `writeIndex(a.idxPath,
  a.entries)`; close file; surface a non-nil error from either.

**Tests (RED first):**
- ingest 300 ticks (strides at 0,128,256) → `loadIndex` has 3 entries at
  RecordIndex 0/128/256 with the matching TS; `expectedIndexLen(300)==3`.
- reopen appender, append 100 more (total 400 → entries at …384) → still consistent.
- delete `.idx`, `OpenAppender` (no new appends), `Close` → index rebuilt identical
  to the pre-delete bytes.
- corrupt `.idx` (truncate to a partial entry), `OpenAppender` → rebuild yields the
  correct consistent index.

**Gate:** `go test ./store/`.

## Task 3 — `store/read.go`+`range.go`: index-aware Range + reindex CLI

**Files:** `store/read.go`, `store/range.go`, `store/range_test.go` (extend),
`cmd/tickstore/main.go`, `cmd/tickstore/main_test.go` (extend)

- Refactor raw reads onto `io.ReaderAt`:
  - `readRecordAt(ra io.ReaderAt, i int64)` (signature change; callers pass the file).
  - `readBlock(ra io.ReaderAt, start, n int64) ([]tick.Tick, error)` — single
    `ReadAt` of `n*RecordSize` bytes, decode each.
- `Range`: open log, `count`; `entries, _ := loadIndex(idxPath)`.
  - if `len(entries) > 0`: binary-search `entries` for greatest `TS <= from` → `base`;
    window upper bound = next entry's RecordIndex or `count`; `readBlock(base ..upper)`;
    linear-scan for first `TS >= from`; forward-scan (reading further blocks) collecting
    until `TS > to`.
  - else: existing in-log binary search (keep as `rangeScan`/current code).
- `cmd`: add `case "reindex"` — flags `--dir`,`--symbol`; open log read-only, `count`,
  `buildIndex`, `writeIndex`; print entries written; `--symbol` required.

**Tests (RED first):**
- **parity sweep:** generate a log (e.g. gen 2000 actions), build index; for many
  `(from,to)` incl. stride-boundary TSs, `from` < first, `from` > last, empty
  (`from>to`), single-TS, full-range — assert `Range` == the same query with `.idx`
  removed. Byte-identical slices.
- **seek count:** counting `io.ReaderAt` wrapper; indexed `Range` over a large log
  issues `<` fallback's `ReadAt` count (assert a strict reduction).
- **stale idx:** write a 1-entry `.idx` for a many-record log by hand; `Range` still
  returns the correct full result (self-correction).
- **CLI:** `reindex` then `loadIndex` non-empty; `query` output identical before vs
  after `reindex`; `reindex` on missing log errors.

**Gate:** `go build ./... && go vet ./... && go test ./...`.

## Task 4 — benchmark + e2e

**Files:** `store/range_bench_test.go`

- `BenchmarkRangeIndexed` vs `BenchmarkRangeFallback` on a large temp log
  (1–5 M records), each running a mid-log `Range`. Report ns/op and a `preads/op`
  custom metric via a counting `ReaderAt`.
- Manual e2e (scripted, not committed): `lob-replay --emit | tickstore ingest`
  produces `SYNTH.log` **and** `SYNTH.idx`; `tickstore query` results match a
  `--idx`-removed run; `candle --log SYNTH.log` still reconciles (index is orthogonal
  to the record stream candle reads).

**Gate:** `go test ./... && go test -bench=Range -benchmem ./store/`.

## After the tasks

- Whole-branch code review (fresh reviewer); fix Critical/Important.
- `finishing-a-development-branch`: push branch, open PR (base main) via
  `as-personal gh pr create --body-file`, disclosing model + plugins, headlining the
  seek-count benchmark. Leave for the user to merge.
- After merge: doc-vault write-up `services/tickstore/v2-sparse-index-writeup.md`,
  update vault README index + global contents map, push via `as-personal git push`.

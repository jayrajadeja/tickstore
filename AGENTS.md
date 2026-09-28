# AGENTS.md — tickstore

Guidance for AI coding agents working in this repository. Human contributors: see
[`CONTRIBUTING.md`](CONTRIBUTING.md).

**What this is:** an append-only, fixed-width tick store with a persistent sparse
index (`SYMBOL.log` + `SYMBOL.idx`) and a `Range`/`Last`/`Reindex` read API. Pure Go,
standard library only, deterministic. **Project B** in the
`lob → tickstore → candle` series.

## Commands you must run

```bash
go build ./...            # compile
go build -o tickstore ./cmd/tickstore
go vet ./...              # static checks
go test ./...             # full test suite
go test ./store/ -run '^$' -bench BenchmarkRange -benchmem   # index vs fallback reads/op
```

**Never claim a change is done until `go build ./... && go vet ./... && go test ./...`
passes.** Show the output; don't assert.

## Non-negotiable conventions

- **The index is disposable, NEVER the source of truth.** A missing, corrupt, or
  stale/short `.idx` must produce results byte-identical to a pure log scan (the
  in-log binary-search fallback). `Range` trusts the index only when its length
  equals `expectedIndexLen(count)`. Any index change must keep the parity test green.
- **On-disk formats are contracts.**
  - Log: 8-byte header (`TCKLOG` + uint16 version) then N×25-byte records
    `TS int64 | Price int64 | Qty uint64 | Side uint8` (LE).
  - Index: 8-byte header (`TCKIDX` + uint16 version) then 16-byte entries
    `TS int64 | RecordIndex int64` (LE), one checkpoint per 128 records.
  - Bump the version and stay backward-compatible if you must change either.
- **Durability ordering.** The `Appender` flushes the log **then** writes the index
  atomically (temp → `Sync` → rename). Never the reverse — an index must never
  promise records the log lacks.
- **Duplicate timestamps are legal.** `TS` is non-decreasing, not strictly
  increasing. Lower-bound logic must return the *first* record with `TS >= from` even
  across a run of equal timestamps that straddles a checkpoint. There is a regression
  test for exactly this; keep it.
- **Integers only.** `int64` prices/timestamps, `uint64` quantities. No floats.
- **I/O is isolated.** `tick` and `store` are pure (return errors, no `os.Exit`/
  print/panic). Only `cmd/tickstore` touches argv/stdin/stdout/files.
- **The 25-byte record is a cross-repo contract** shared with `lob --emit` and
  `candle`. Don't change it in isolation.
- **Minimal, surgical diffs.** Keep every safety guard; write the failing test first.

## Workflow

- Specs and plans live under `docs/superpowers/` (v1 tick store, v2 sparse index).
  Read the relevant spec before a non-trivial change; add one for new work.
- **Commits:** include the trailer
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`.
  Git identity is machine-level — do not hard-code author info.
- **Pull requests:** open a PR and let a human merge it. **Never self-merge.**

## Layout

| package | job |
|---------|-----|
| `tick/`  | the 25-byte record + encode/decode (pure) |
| `store/` | append-only log, `Appender`, sparse index, `Range`/`Last`/`Reindex` |
| `cmd/tickstore/` | `ingest`/`query`/`dump`/`reindex` CLI — the only I/O layer |

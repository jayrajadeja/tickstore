# Contributing to tickstore

Thanks for helping improve **tickstore** — an append-only, sparse-indexed tick
store for the trade stream.

## Prerequisites

- Go 1.26 or newer (see `go.mod`).
- Standard library only. **Do not add third-party dependencies** without discussion;
  keeping the module dependency-free is a design goal of this series.

## Local checks

Run the full gate from the repository root before opening a pull request:

```bash
go build ./... && go vet ./... && go test ./...
```

If you touched the read path or index, run the benchmarks too:

```bash
go test ./store/ -run '^$' -bench BenchmarkRange -benchmem
```

## Coding expectations

- **Test-first.** Add or update a failing test before the change that makes it pass.
- **The index is an optimization, never the source of truth.** Any change to the
  index must preserve this: a missing, corrupt, or stale index must still yield
  results byte-identical to a pure log scan. Assert the indexed path against the
  fallback path in a test — don't just eyeball it.
- **Preserve the on-disk formats.** The 25-byte log record and the 16-byte index
  entry are contracts; bump the header version if you must change them, and keep the
  reader backward-compatible.
- **Keep the core pure.** `tick` and `store` return errors and never `os.Exit`,
  print, or panic on bad input. All I/O lives in `cmd/tickstore`.
- **Integers, not floats.** Prices/timestamps are `int64`, quantities `uint64`.
- **Durability ordering matters.** Write the log before the index; the crash window
  must only ever produce a *tolerated* failure (a short index), never a fatal one.
- **Minimal, surgical changes.** No speculative features, no unrelated refactors.
- Update documentation when behavior, flags, or a format changes.
- Never commit secrets.

## Pull-request checklist

- `go build ./... && go vet ./... && go test ./...` is green.
- Index/log changes are covered by a parity test (indexed vs. fallback).
- Docs (README, specs) reflect the change.
- No unrelated files are modified.

## Design docs

Specs and implementation plans live under `docs/superpowers/` (v1 tick store,
v2 sparse index). For a non-trivial change, add or update the relevant spec first.

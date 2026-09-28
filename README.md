# tickstore — an append-only, sparse-indexed tick store (Go)

A tiny, honest slice of a time-series database: one append-only, fixed-width log
per symbol, plus a persistent sparse index that turns a cold range lookup into a
single block read. Pure Go, standard library only, no floats.

This is **project B** in a four-part series:

```
A  lob        generate   — match orders, emit a trade stream
B  tickstore  store      — append-only, indexed tick store        (this repo)
C  candle     aggregate  — OHLCV candles over the tick stream
```

The pieces share nothing but a **25-byte trade record** on a Unix pipe:

```bash
lob-replay --emit | tickstore ingest --symbol SYNTH
tickstore query  --symbol SYNTH --from 1000 --to 2000
```

## Build

```bash
go build ./...
go build -o tickstore ./cmd/tickstore
```

Requires Go 1.26+ (see `go.mod`). No third-party dependencies.

## Usage

```bash
# ingest a raw 25-byte record stream (from a file or a pipe) into a symbol's log
lob-replay --emit | tickstore ingest --dir data --symbol SYNTH

# range query over logical time (inclusive), ascending
tickstore query --dir data --symbol SYNTH --from 1000 --to 2000

# the most recent N ticks (exclusive with --from/--to)
tickstore query --dir data --symbol SYNTH --last 50

# dump the whole log
tickstore dump --dir data --symbol SYNTH

# (re)build the sparse index for a log — e.g. to upgrade a v1 log
tickstore reindex --dir data --symbol SYNTH
```

`--dir` defaults to `data`. `--symbol` is required.

## How it works

- **Fixed-width records.** Every tick is exactly **25 bytes**, little-endian:
  `TS int64 | Price int64 | Qty uint64 | Side uint8`. Record `i` lives at offset
  `8 + i*25`, so `Last-N` is pure arithmetic and `Range` is a binary search on disk.
- **The filename is the symbol.** `SYNTH.log` — no per-record symbol field, and
  multi-symbol falls out for free (one sorted log per symbol).
- **8-byte header.** `TCKLOG` + a `uint16` version, validated on every reopen.
- **Persistent sparse index (v2).** A `SYMBOL.idx` sidecar stores one checkpoint
  every 128 records (`TS`, `RecordIndex`). `Range` binary-searches the tiny index in
  memory, then reads one contiguous block — collapsing ~log₂N scattered `pread`s into
  a single sequential read. At 1M records: **~19 reads/op → 1**.
- **The index is disposable, never the source of truth.** Missing, corrupt, or
  stale/short index ⇒ `Range` falls back to the correct in-log binary search; the
  index is rebuilt on the next append or via `reindex`. Results are byte-identical
  either way.
- **Integers, not floats.** Prices/timestamps are `int64`, quantities `uint64`.
- **I/O in one package.** The `tick`/`store` cores are pure; only `cmd/tickstore`
  touches stdin/stdout/argv.

## Layout

| package | job |
|---------|-----|
| `tick/`  | the 25-byte record + `EncodeInto`/`Decode` (pure) |
| `store/` | per-symbol append-only log, `Appender`, sparse index, `Range`/`Last`/`Reindex` |
| `cmd/tickstore/` | `ingest`/`query`/`dump`/`reindex` CLI — the only I/O layer |

## Development

```bash
go build ./... && go vet ./... && go test ./...
go test ./store/ -run '^$' -bench BenchmarkRange -benchmem   # index vs fallback reads/op
```

Design specs and implementation plans live under `docs/superpowers/`
(v1 tick store and v2 sparse index).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Agent contributors: read
[`AGENTS.md`](AGENTS.md).

## License

MIT — see [`LICENSE`](LICENSE).

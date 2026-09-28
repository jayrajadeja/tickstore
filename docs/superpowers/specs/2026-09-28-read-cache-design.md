# tickstore serve read cache — design (Project E.1)

> Increment on top of Project E (`serve`, PR #4). Go, stdlib only. Keeps the same
> read-only HTTP surface; makes the read path resident so repeat queries stop
> paying per-request open + index-reload cost.

## Problem

`serve` reuses `store.Range`/`store.Last`, which are deliberately stateless: each
call does `os.Open` + `validateHeader` + `Stat` + `loadIndex` (a full `os.ReadFile`
of `SYMBOL.idx`, ~125 KB at 1M records) before it reads a single result record.
Under a server that fat is re-paid on every request. serve-v1 froze this as the
baseline on purpose; this project spends the deferred idea: **keep the fd and the
parsed sparse index hot, validated by a cheap Stat.**

Scope, cut hard (YAGNI): no TTL, no LRU eviction (symbol count is tiny), no mmap,
no background refresh, no negative caching, no write path changes. One idea,
measured against the serve-v1 baseline.

## Architecture

A new `store.Cache` type wraps a `*store.Store` and caches, per symbol, the open
read fd + parsed index + record count. A tiny read interface lets the server hold
either the plain store or the cache, so `server` stays transport-only.

```go
type Reader interface {
    Range(symbol string, from, to int64) ([]tick.Tick, error)
    Last(symbol string, n int) ([]tick.Tick, error)
}

type Cache struct {
    s  *Store
    mu sync.RWMutex
    m  map[string]*hot
}

type hot struct {
    f       *os.File
    size    int64
    modUnix int64
    count   int64
    entries []indexEntry
}

func NewCached(dir string) *Cache
```

Both `*Store` and `*Cache` satisfy `Reader`. `server.Handler` takes a `Reader`
(existing tests keep passing `*Store`); `cmd serve` wires in `store.NewCached(dir)`.

## Shared core (refactor)

`Range`'s body — count → choose indexed vs scan lower bound → forward scan to `to`
— is extracted into a package helper:

```go
func rangeFrom(ra io.ReaderAt, count int64, entries []indexEntry, from, to int64) ([]tick.Tick, error)
func lastFrom(ra io.ReaderAt, count int64, n int) ([]tick.Tick, error)
```

`Store.Range`/`Store.Last` and `Cache.Range`/`Cache.Last` both call these, so
results are byte-for-byte identical by construction. No behavior change to the
existing store; this is a pure extraction covered by the current tests.

## Per-request path (cache hit)

1. `RLock`; look up `hot` for the symbol. Miss → fall to the load path.
2. `f.Stat()` (1 syscall). If `size`/`modUnix` match the cached values, the fd and
   `entries` are current: `RUnlock`, then run `rangeFrom`/`lastFrom` directly on the
   hot fd via `ReadAt`.
3. Mismatch or miss → `Lock`; reopen the fd, reload the index, refresh `hot`,
   `Unlock`, then serve.

Dropped vs baseline: the `os.Open`, the header `ReadAt`, and the whole index
`os.ReadFile`. Net cost on a hit: one `Stat` + the record `ReadAt`s.

## Concurrency

- `*os.File.ReadAt` is offset-free and goroutine-safe, and a built `entries` slice
  is never mutated after construction — so concurrent reads share the hot fd and
  slice with no per-read locking.
- The `RWMutex` guards only the map and the `hot` reload. Readers take `RLock`;
  a reload takes `Lock`. Validated under `go test -race`.

## Invalidation (correctness)

The store is append-only, but a separate `ingest` process can grow (or replace) a
log while serve runs. The per-request `Stat` compares `(size, mtime)`:

- **Grown / shrunk / mtime changed** → reopen fd + reload index under `Lock`.
- **Unchanged** → hot data is authoritative.
- **Missing log** → return empty, cache nothing (a later-created log is picked up
  on the next request).

Because `Range` already tolerates a stale/short index by falling back to the log
scan, even a momentarily-behind cache can only ever be *correct*; the Stat gate
just keeps it from being slow.

## Error handling

Mirrors the store: missing log → empty (not error); `from > to` / `n <= 0` → empty;
a bad/corrupt index → treated as absent, fallback scan (unchanged). Reopen/reload
errors propagate to the handler as `500`, same as today.

## Testing

- `store/cache_test.go`
  - **Parity:** random (symbol, from, to) and (symbol, n) queries return identical
    results from `Cache` and a plain `Store` over the same dir.
  - **Grow-while-open:** open a Cache, query, append more records via
    `OpenAppender`, query again — the Stat gate detects growth and the new records
    appear.
  - **Missing then created:** query a symbol with no log (empty), create it, query
    again (data).
  - **Concurrency:** parallel `Range`/`Last` on shared Cache under `-race`.
- `store/cache_bench_test.go` — `BenchmarkCacheRange` vs the existing
  `BenchmarkRange` on a large log, reported as reads/op, to quantify the win over
  serve-v1.
- `server` — unchanged behavior; a smoke assertion that `Handler` accepts a
  `*store.Cache`.

## Out of scope

Eviction, TTL, mmap, negative caching, multi-dir, write-through, metrics. Each is a
separate measurable increment if ever justified.

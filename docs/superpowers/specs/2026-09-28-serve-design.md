# tickstore serve — read-only HTTP query server (Project E)

**Status:** design (drafted autonomously under autopilot; author reviews before/after)
**Date:** 2026-09-28
**Series:** A `lob` (generate) → B `tickstore` (store) → C `candle` (aggregate).
Project E is the first *service*: a long-lived process that answers queries over the
network instead of a one-shot CLI.

## Why build this

Every read so far has been one-shot: the CLI opens the log, answers one query, and
exits. Both the v1 and v2 write-ups named the same next itch — **a long-lived
`serve` mode** — as the point where an in-memory/resident story finally earns its
keep. This project takes the first honest step: turn the existing `store.Range` /
`store.Last` read API into a **concurrent, read-only HTTP server**.

The lesson is the *service boundary*: a wire protocol, request validation, mapping
domain results and errors to HTTP, concurrency, and graceful shutdown — on top of a
core that is already correct and fast.

## Scope (v1, cut hard — YAGNI)

**In:**
- A `tickstore serve --dir <data> --addr <:8080>` subcommand.
- Three endpoints over HTTP/1.1, JSON responses:
  - `GET /healthz` → liveness.
  - `GET /v1/range?symbol=&from=&to=` → ticks with `from <= TS <= to`.
  - `GET /v1/last?symbol=&n=` → the most recent `n` ticks.
- Concurrent request handling (net/http default) and graceful shutdown on
  SIGINT/SIGTERM.

**Out (deferred, on purpose):**
- **Writes over HTTP.** Ingestion stays the CLI/pipe. The server is read-only.
- **A resident index/file cache.** v1 reuses `store.Range`/`store.Last`, which
  reopen the log and reload the (tiny) index per request. Caching open handles and
  parsed indices in memory is the *next* increment, to be measured — it's the part
  the write-ups flagged, and it deserves its own before/after benchmark rather than
  being smuggled in here.
- **Candles over HTTP** (`candle` is a separate module/CLI), auth, TLS, pagination,
  streaming/`follow`, a binary protocol, multi-symbol batch queries.

Keeping v1 to "HTTP in front of the existing reads" makes it small and reviewable,
exactly like v1 tickstore deferred the index itself.

## Architecture

Two units, one dependency arrow, mirroring the rest of the repo:

```
store/            existing pure read API: Range, Last            (unchanged)
   ^
server/           http.Handler over a *store.Store: routing,
                  param parsing, JSON encoding, error mapping    (new, pure-ish:
                                                                  no sockets, no os.Exit)
   ^
cmd/tickstore/    `serve` subcommand: build the handler, run an
                  http.Server, handle signals + graceful shutdown (I/O layer)
```

- **`server` package.** Exposes `func Handler(s *store.Store) http.Handler`. All
  routing, query-parameter parsing, validation, result/error → HTTP mapping, and
  JSON encoding live here. It never binds a socket, never calls `os.Exit`, never
  reads argv — so it is fully testable with `net/http/httptest` and no real network.
- **`cmd/tickstore serve`.** Parses `--dir`/`--addr`, constructs
  `store.New(dir)` and `server.Handler(...)`, wraps it in an `http.Server`, and runs
  `ListenAndServe` with `Shutdown` on SIGINT/SIGTERM. This is the only new I/O.

### Why no locks

`store` reads are stateless: each `Range`/`Last` opens the file, reads, and closes,
sharing no mutable state. net/http serves each request on its own goroutine, so the
handler needs **no synchronization** in v1. This is a direct benefit of the pure
read design and worth stating explicitly. (The moment we add a resident cache in a
later increment, that changes — and that's the point where locking/`sync` shows up.)

## Wire format

JSON, UTF-8. `side` is rendered as the string `"buy"`/`"sell"` (not the raw byte),
matching the CLI's human output. Prices/quantities/timestamps are JSON numbers
(int64/uint64 fit within JSON's safe integer range for this project's synthetic
data; documented as a known limitation rather than solved with string encoding).

**Tick object:**
```json
{ "ts": 100, "price": 250, "qty": 3, "side": "buy" }
```

**`GET /v1/range?symbol=SYNTH&from=1000&to=2000`:**
```json
{ "symbol": "SYNTH", "from": 1000, "to": 2000, "count": 2,
  "ticks": [ { "ts": 1000, ... }, { "ts": 1500, ... } ] }
```

**`GET /v1/last?symbol=SYNTH&n=50`:**
```json
{ "symbol": "SYNTH", "n": 50, "count": 50, "ticks": [ ... ] }
```

**`GET /healthz`:** `200` with body `ok` (text/plain).

**Errors** (JSON, non-2xx):
```json
{ "error": "symbol is required" }
```

## Request → response mapping

| Case | Status | Body |
|------|--------|------|
| valid range/last | `200` | result object above |
| missing/blank `symbol` | `400` | `{"error":"symbol is required"}` |
| `from`/`to`/`n` not an integer | `400` | `{"error":"..."}` |
| `n < 0` (last) | `400` | `{"error":"n must be non-negative"}` |
| `range` given `n`, or `last` given from/to | `400` | clear error |
| unknown symbol / no log | `200` | empty `ticks`, `count: 0` (mirrors `store` read semantics) |
| `from > to` (range) | `200` | empty `ticks` (mirrors `Range`) |
| internal read/corruption error | `500` | `{"error":"..."}` |
| unknown path / method not GET | `404` / `405` | `{"error":"..."}` |

Defaults, mirroring the `query` CLI: `range` without `from` uses `MinInt64`,
without `to` uses `MaxInt64` (so `?symbol=X` alone returns everything). `last`
requires `n`.

## Error handling

- The `server` package returns typed HTTP responses; it never panics on bad input.
  Parameter parsing failures map to `400`; `store` errors map to `500`; a `nil`
  result maps to an empty list, never a null.
- The `cmd` layer logs listen/shutdown lifecycle to stderr and exits non-zero only
  on a genuine `ListenAndServe` failure (not on clean shutdown).

## Testing

- **`server/handler_test.go`** (the bulk): table-driven `httptest` tests over a
  handler backed by a `store.New(t.TempDir())` seeded with `OpenAppender`. Cover:
  range happy path + boundaries, last happy path + `n` larger than the log,
  empty/unknown symbol, all `400` validation branches, `from>to`, unknown
  path/method, and JSON shape (field names, `side` string, empty-list-not-null).
- **`cmd/tickstore` serve test:** start the server on `127.0.0.1:0` (an ephemeral
  port), issue one real `GET /healthz` and one `/v1/last`, then `Shutdown`; assert
  clean start/stop. Keep it to a single smoke test — the logic lives in `server`.
- Full gate: `go build ./... && go vet ./... && go test ./...`.

## Manual end-to-end

```bash
lob-replay --emit | tickstore ingest --dir data --symbol SYNTH
tickstore serve --dir data --addr :8080 &
curl 'localhost:8080/v1/last?symbol=SYNTH&n=5'
curl 'localhost:8080/v1/range?symbol=SYNTH&from=1000&to=2000'
```

Results must match the equivalent `tickstore query` output.

## Open decisions (resolved)

- **HTTP/JSON over a binary protocol** — chosen for curl-ability and content value;
  a binary protocol is a possible later increment, not v1.
- **Read-only** — writes stay on the pipe; a networked writer is a much larger,
  separate design (durability, auth, backpressure).
- **No resident cache in v1** — deferred so its speedup can be measured on its own.

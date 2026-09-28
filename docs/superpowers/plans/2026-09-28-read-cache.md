# tickstore serve read cache — implementation plan (TDD)

Spec: `docs/superpowers/specs/2026-09-28-read-cache-design.md`. Branch
`feature/read-cache`. Each task: red → green → refactor, keep the full gate green
(`go build ./... && go vet ./... && go test ./... && go test -race ./store/...`).

## Task 1 — Extract shared range/last core (pure refactor)

- Add `rangeFrom(ra io.ReaderAt, count int64, entries []indexEntry, from, to int64)`
  and `lastFrom(ra io.ReaderAt, count int64, n int)` in `store/range.go` / `read.go`.
- Reimplement `Store.Range`/`Store.Last` to open+stat+loadIndex, then delegate to
  the helpers. No behavior change.
- Green = existing `store` tests all pass unchanged. Commit.

## Task 2 — `Reader` interface + server takes it

- Add `store.Reader` (Range/Last). Assert `var _ Reader = (*Store)(nil)`.
- Change `server.Handler(s *store.Store)` → `Handler(r store.Reader)`; update field
  type. Existing `server` + cmd tests compile and pass (Store satisfies Reader).
- Commit.

## Task 3 — `store.Cache`: parity first (red → green)

- Write `store/cache_test.go` parity test: same dir, random queries, `Cache` result
  == `Store` result. (Red: Cache doesn't exist.)
- Implement `Cache`, `NewCached`, `hot`, `Range`, `Last`: RLock lookup → `f.Stat`
  → hit uses hot fd+entries via `rangeFrom`/`lastFrom`; miss/stale reloads under
  Lock (reopen fd, loadIndex-or-fallback, refresh hot). Missing log → empty.
- Green. Commit.

## Task 4 — Invalidation + concurrency tests

- Add grow-while-open test (append via `OpenAppender`, cached query sees new data),
  missing-then-created test, and a parallel `-race` test.
- Fix any race/staleness. `go test -race ./store/...` green. Commit.

## Task 5 — Bench the win

- Add `BenchmarkCacheRange` mirroring `BenchmarkRange` (1M records) reporting
  reads/op; confirm the cache removes the per-request index ReadFile + open.
- Record the numbers in the PR body. Commit.

## Task 6 — Wire `cmd serve` to the cache

- `runServe` builds `store.NewCached(dir)` and passes it to `server.Handler`.
- `serve_test.go` smoke test still passes. Manual e2e: parity vs CLI unchanged.
- Commit.

## Task 7 — Gate, review, PR

- Full gate incl. `-race`. Fresh code-review agent on `git diff main...HEAD`; fix
  Critical/Important (3x reassess). Open PR base main (disclose model + plugins,
  include bench numbers, note it's read-only + Stat-validated). Never self-merge.
- Post-merge: sync/delete branch, doc-vault write-up, README/index/global-map
  updates, README cache note.

package store

import "testing"

// These benchmarks measure the per-request cost of a repeated Range on a large
// log. Store pays open + Stat + a full index ReadFile on every call; Cache pays
// one Stat and reuses the hot fd + parsed index. The ns/op gap is the win the
// resident cache buys over the serve-v1 baseline.

func benchRepeatRange(b *testing.B, r Reader, n int) {
	b.Helper()
	from := int64(n * 3 / 4)
	to := from + 64
	// Warm any resident state so we measure steady-state, not first-load.
	if _, err := r.Range("SYM", from, to); err != nil {
		b.Fatalf("warmup: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Range("SYM", from, to); err != nil {
			b.Fatalf("Range: %v", err)
		}
	}
}

func BenchmarkStoreRangeRepeat1M(b *testing.B) {
	dir := b.TempDir()
	ingestN(b, New(dir), "SYM", 1_000_000)
	benchRepeatRange(b, New(dir), 1_000_000)
}

func BenchmarkCacheRangeRepeat1M(b *testing.B) {
	dir := b.TempDir()
	ingestN(b, New(dir), "SYM", 1_000_000)
	c := NewCached(dir)
	defer c.Close()
	benchRepeatRange(b, c, 1_000_000)
}

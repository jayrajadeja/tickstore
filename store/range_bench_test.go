package store

import (
	"os"
	"testing"
)

// benchLowerBound reports ReadAt calls/op (the cost that dominates a cold query:
// each ReadAt is a pread/seek). The indexed path should stay flat (~1 block read)
// while the fallback grows as log2(N).
func benchLowerBound(b *testing.B, n int, indexed bool) {
	b.Helper()
	dir := b.TempDir()
	s := New(dir)
	ingestN(b, s, "SYM", n)

	f, err := os.Open(s.path("SYM"))
	if err != nil {
		b.Fatalf("open log: %v", err)
	}
	defer f.Close()

	entries, err := loadIndex(s.idxPath("SYM"))
	if err != nil {
		b.Fatalf("loadIndex: %v", err)
	}
	from := int64(n * 3 / 4) // a lower bound deep in the log

	reads := 0
	cra := countingReaderAt{f, &reads}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var e error
		if indexed {
			_, e = indexedLowerBound(cra, entries, int64(n), from)
		} else {
			_, e = scanLowerBound(cra, int64(n), from)
		}
		if e != nil {
			b.Fatalf("lowerBound: %v", e)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
}

func BenchmarkRangeFallback1M(b *testing.B) { benchLowerBound(b, 1_000_000, false) }
func BenchmarkRangeIndexed1M(b *testing.B)  { benchLowerBound(b, 1_000_000, true) }

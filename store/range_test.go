package store

import (
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func rangeFixture(t *testing.T) *Store {
	t.Helper()
	s := New(t.TempDir())
	appendAll(t, s, "X", []tick.Tick{
		{TS: 10, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 10, Price: 101, Qty: 2, Side: tick.Sell}, // tie on 10
		{TS: 20, Price: 102, Qty: 3, Side: tick.Buy},
		{TS: 30, Price: 103, Qty: 4, Side: tick.Sell},
		{TS: 30, Price: 104, Qty: 5, Side: tick.Buy}, // tie on 30
	})
	return s
}

func tsList(ticks []tick.Tick) []int64 {
	out := make([]int64, len(ticks))
	for i, tk := range ticks {
		out[i] = tk.TS
	}
	return out
}

func eq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRangeInclusiveWithTies(t *testing.T) {
	s := rangeFixture(t)
	cases := []struct {
		name     string
		from, to int64
		wantTS   []int64
	}{
		{"all", 0, 100, []int64{10, 10, 20, 30, 30}},
		{"ties at low edge", 10, 10, []int64{10, 10}},
		{"ties at high edge", 20, 30, []int64{20, 30, 30}},
		{"before first", 0, 5, nil},
		{"after last", 40, 50, nil},
		{"empty gap between records", 11, 19, nil},
		{"from greater than to", 30, 20, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := s.Range("X", c.from, c.to)
			if err != nil {
				t.Fatalf("Range: %v", err)
			}
			if !eq(tsList(got), c.wantTS) {
				t.Fatalf("Range(%d,%d) TS = %v, want %v", c.from, c.to, tsList(got), c.wantTS)
			}
		})
	}
}

func TestRangeMissingSymbolEmpty(t *testing.T) {
	got, err := New(t.TempDir()).Range("NOPE", 0, 100)
	if err != nil {
		t.Fatalf("Range missing: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Range missing = %+v, want empty", got)
	}
}

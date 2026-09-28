package main

import (
	"bytes"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

// goldenTicks is the fixed input for the end-to-end test. Deterministic in →
// deterministic out; expected strings below are captured from a real run.
func goldenTicks() []tick.Tick {
	return []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 1, Price: 101, Qty: 2, Side: tick.Sell},
		{TS: 2, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 5, Price: 102, Qty: 3, Side: tick.Sell},
	}
}

func TestEndToEndIngestQuery(t *testing.T) {
	dir := t.TempDir()
	if err := runIngest(dir, "SYNTH", bytes.NewReader(encodeStream(t, goldenTicks()))); err != nil {
		t.Fatalf("runIngest: %v", err)
	}

	t.Run("range 1..1 (ties)", func(t *testing.T) {
		var out bytes.Buffer
		if err := runQuery(dir, "SYNTH", 1, 1, 0, &out); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
		want := "ts=1 price=100 qty=5 side=buy\nts=1 price=101 qty=2 side=sell\n"
		if out.String() != want {
			t.Fatalf("range 1..1 =\n%q\nwant\n%q", out.String(), want)
		}
	})

	t.Run("range 2..5", func(t *testing.T) {
		var out bytes.Buffer
		if err := runQuery(dir, "SYNTH", 2, 5, 0, &out); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
		want := "ts=2 price=100 qty=1 side=buy\nts=5 price=102 qty=3 side=sell\n"
		if out.String() != want {
			t.Fatalf("range 2..5 =\n%q\nwant\n%q", out.String(), want)
		}
	})

	t.Run("last 1", func(t *testing.T) {
		var out bytes.Buffer
		if err := runQuery(dir, "SYNTH", 0, 0, 1, &out); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
		want := "ts=5 price=102 qty=3 side=sell\n"
		if out.String() != want {
			t.Fatalf("last 1 =\n%q\nwant\n%q", out.String(), want)
		}
	})
}

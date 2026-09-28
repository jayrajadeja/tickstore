package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

func encodeStream(t *testing.T, ticks []tick.Tick) []byte {
	t.Helper()
	var b bytes.Buffer
	buf := make([]byte, tick.RecordSize)
	for _, tk := range ticks {
		if err := tk.EncodeInto(buf); err != nil {
			t.Fatalf("EncodeInto: %v", err)
		}
		b.Write(buf)
	}
	return b.Bytes()
}

func TestFormatTick(t *testing.T) {
	got := formatTick(tick.Tick{TS: 7, Price: 100, Qty: 3, Side: tick.Sell})
	want := "ts=7 price=100 qty=3 side=sell"
	if got != want {
		t.Fatalf("formatTick = %q, want %q", got, want)
	}
}

func TestIngestThenDump(t *testing.T) {
	dir := t.TempDir()
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
	}
	if err := runIngest(dir, "X", bytes.NewReader(encodeStream(t, ticks))); err != nil {
		t.Fatalf("runIngest: %v", err)
	}

	var out bytes.Buffer
	if err := runDump(dir, "X", &out); err != nil {
		t.Fatalf("runDump: %v", err)
	}
	want := "ts=1 price=100 qty=5 side=buy\nts=2 price=101 qty=2 side=sell\n"
	if out.String() != want {
		t.Fatalf("dump =\n%q\nwant\n%q", out.String(), want)
	}
}

func TestIngestTruncatedStreamErrors(t *testing.T) {
	dir := t.TempDir()
	full := encodeStream(t, []tick.Tick{{TS: 1, Price: 100, Qty: 5, Side: tick.Buy}})
	truncated := append(full, 0x01, 0x02) // trailing partial record

	err := runIngest(dir, "X", bytes.NewReader(truncated))
	if err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("err = %v, want a truncated-stream error", err)
	}

	// The one whole record must still have been flushed and be readable.
	var out bytes.Buffer
	if derr := runDump(dir, "X", &out); derr != nil {
		t.Fatalf("runDump: %v", derr)
	}
	if out.String() != "ts=1 price=100 qty=5 side=buy\n" {
		t.Fatalf("dump after truncated ingest = %q", out.String())
	}
}

func TestReindexRebuildsAndQueryUnchanged(t *testing.T) {
	dir := t.TempDir()
	ticks := make([]tick.Tick, 300)
	for i := range ticks {
		ticks[i] = tick.Tick{TS: int64(i), Price: int64(100 + i), Qty: 1, Side: tick.Buy}
	}
	if err := runIngest(dir, "X", bytes.NewReader(encodeStream(t, ticks))); err != nil {
		t.Fatalf("runIngest: %v", err)
	}

	// Query result before reindex.
	var before bytes.Buffer
	if err := runQuery(dir, "X", 150, 160, 0, &before); err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	// Delete the index, then reindex it back.
	if err := os.Remove(filepath.Join(dir, "X.idx")); err != nil {
		t.Fatalf("remove idx: %v", err)
	}
	var rout bytes.Buffer
	if err := runReindex(dir, "X", &rout); err != nil {
		t.Fatalf("runReindex: %v", err)
	}
	if !strings.Contains(rout.String(), "3 index entries") {
		t.Fatalf("reindex output = %q, want '3 index entries'", rout.String())
	}

	// Query result after reindex must be identical.
	var after bytes.Buffer
	if err := runQuery(dir, "X", 150, 160, 0, &after); err != nil {
		t.Fatalf("runQuery after: %v", err)
	}
	if before.String() != after.String() {
		t.Fatalf("query differs after reindex:\n before=%q\n after=%q", before.String(), after.String())
	}
}

func TestReindexMissingLogErrors(t *testing.T) {
	if err := runReindex(t.TempDir(), "NOPE", &bytes.Buffer{}); err == nil {
		t.Fatal("expected error reindexing a missing log")
	}
}

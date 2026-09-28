package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/jayrajadeja/tickstore/tick"
)

// TestServeSmoke starts the real HTTP server on an ephemeral port, hits /healthz
// and /v1/last against it, then cancels and confirms a clean shutdown.
func TestServeSmoke(t *testing.T) {
	dir := t.TempDir()
	ticks := []tick.Tick{
		{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
		{TS: 3, Price: 102, Qty: 1, Side: tick.Buy},
	}
	if err := runIngest(dir, "X", bytes.NewReader(encodeStream(t, ticks))); err != nil {
		t.Fatalf("runIngest: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	errc := make(chan error, 1)
	go func() { errc <- runServe(dir, "127.0.0.1:0", ctx, ready) }()
	addr := <-ready
	base := "http://" + addr

	// health
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("health = %d %q", resp.StatusCode, body)
	}

	// last
	resp, err = http.Get(base + "/v1/last?symbol=X&n=2")
	if err != nil {
		t.Fatalf("GET /v1/last: %v", err)
	}
	var last struct {
		Count int `json:"count"`
		Ticks []struct {
			TS   int64  `json:"ts"`
			Side string `json:"side"`
		} `json:"ticks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&last); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if last.Count != 2 || last.Ticks[1].TS != 3 || last.Ticks[1].Side != "buy" {
		t.Fatalf("last response wrong: %+v", last)
	}

	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("runServe returned error: %v", err)
	}
}

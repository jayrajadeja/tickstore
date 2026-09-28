// Package server exposes the tickstore read API (Range/Last) over HTTP as JSON.
// It is transport-only: it binds no socket, never calls os.Exit, and never reads
// argv, so it is fully testable with net/http/httptest. Process concerns (listening,
// signals, shutdown) live in cmd/tickstore.
package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/jayrajadeja/tickstore/store"
	"github.com/jayrajadeja/tickstore/tick"
)

const (
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)

// tickDTO is the JSON shape of a tick on the wire. Side is a human string.
type tickDTO struct {
	TS    int64  `json:"ts"`
	Price int64  `json:"price"`
	Qty   uint64 `json:"qty"`
	Side  string `json:"side"`
}

type rangeResponse struct {
	Symbol string    `json:"symbol"`
	From   int64     `json:"from"`
	To     int64     `json:"to"`
	Count  int       `json:"count"`
	Ticks  []tickDTO `json:"ticks"`
}

type lastResponse struct {
	Symbol string    `json:"symbol"`
	N      int       `json:"n"`
	Count  int       `json:"count"`
	Ticks  []tickDTO `json:"ticks"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func sideString(s tick.Side) string {
	if s == tick.Sell {
		return "sell"
	}
	return "buy"
}

// toDTOs maps store ticks to wire DTOs, always returning a non-nil slice so the
// JSON is an empty array `[]` rather than `null`.
func toDTOs(ticks []tick.Tick) []tickDTO {
	out := make([]tickDTO, 0, len(ticks))
	for _, t := range ticks {
		out = append(out, tickDTO{TS: t.TS, Price: t.Price, Qty: t.Qty, Side: sideString(t.Side)})
	}
	return out
}

// Handler returns the HTTP handler for the read API backed by s.
func Handler(s *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleNotFound) // catch-all: JSON 404 for unknown paths
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/v1/range", func(w http.ResponseWriter, r *http.Request) { handleRange(w, r, s) })
	mux.HandleFunc("/v1/last", func(w http.ResponseWriter, r *http.Request) { handleLast(w, r, s) })
	return mux
}

func handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeErr(w, http.StatusNotFound, "not found")
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func handleRange(w http.ResponseWriter, r *http.Request, s *store.Store) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		writeErr(w, http.StatusBadRequest, "symbol is required")
		return
	}
	if q.Has("n") {
		writeErr(w, http.StatusBadRequest, "n is not valid for range; use last")
		return
	}
	from, ok := parseInt64(w, q, "from", minInt64)
	if !ok {
		return
	}
	to, ok := parseInt64(w, q, "to", maxInt64)
	if !ok {
		return
	}

	ticks, err := s.Range(symbol, from, to)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	dtos := toDTOs(ticks)
	writeJSON(w, http.StatusOK, rangeResponse{Symbol: symbol, From: from, To: to, Count: len(dtos), Ticks: dtos})
}

func handleLast(w http.ResponseWriter, r *http.Request, s *store.Store) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		writeErr(w, http.StatusBadRequest, "symbol is required")
		return
	}
	if q.Has("from") || q.Has("to") {
		writeErr(w, http.StatusBadRequest, "from/to are not valid for last; use range")
		return
	}
	if !q.Has("n") {
		writeErr(w, http.StatusBadRequest, "n is required")
		return
	}
	n, ok := parseInt(w, q, "n")
	if !ok {
		return
	}
	if n < 0 {
		writeErr(w, http.StatusBadRequest, "n must be non-negative")
		return
	}

	ticks, err := s.Last(symbol, n)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	dtos := toDTOs(ticks)
	writeJSON(w, http.StatusOK, lastResponse{Symbol: symbol, N: n, Count: len(dtos), Ticks: dtos})
}

// parseInt64 reads an int64 query param, using def when absent/empty. On a
// malformed value it writes a 400 and returns ok=false.
func parseInt64(w http.ResponseWriter, q url.Values, key string, def int64) (int64, bool) {
	v := q.Get(key)
	if v == "" {
		return def, true
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, key+" must be an integer")
		return 0, false
	}
	return n, true
}

// parseInt reads an int query param (assumed present). On a malformed value it
// writes a 400 and returns ok=false.
func parseInt(w http.ResponseWriter, q url.Values, key string) (int, bool) {
	n, err := strconv.Atoi(q.Get(key))
	if err != nil {
		writeErr(w, http.StatusBadRequest, key+" must be an integer")
		return 0, false
	}
	return n, true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

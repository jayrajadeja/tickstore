package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jayrajadeja/tickstore/store"
	"github.com/jayrajadeja/tickstore/tick"
)

// seedStore returns a store with n ticks (TS=0..n-1) under symbol.
func seedStore(t *testing.T, symbol string, n int) *store.Store {
	t.Helper()
	s := store.New(t.TempDir())
	a, err := s.OpenAppender(symbol)
	if err != nil {
		t.Fatalf("OpenAppender: %v", err)
	}
	for i := 0; i < n; i++ {
		side := tick.Buy
		if i%2 == 1 {
			side = tick.Sell
		}
		if err := a.Append(tick.Tick{TS: int64(i), Price: int64(100 + i), Qty: 1, Side: side}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return s
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestToDTOEmptyIsArrayNotNull(t *testing.T) {
	b, err := json.Marshal(rangeResponse{Symbol: "X", Ticks: toDTOs(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"ticks":[]`) {
		t.Fatalf("empty ticks must encode as [], got %s", b)
	}
}

func TestToDTOSideStrings(t *testing.T) {
	got := toDTOs([]tick.Tick{{Side: tick.Buy}, {Side: tick.Sell}})
	if got[0].Side != "buy" || got[1].Side != "sell" {
		t.Fatalf("side mapping wrong: %+v", got)
	}
}

func TestHealth(t *testing.T) {
	h := Handler(store.New(t.TempDir()))
	rec := get(t, h, "/healthz")
	if rec.Code != 200 || rec.Body.String() != "ok" {
		t.Fatalf("health = %d %q", rec.Code, rec.Body.String())
	}
}

func TestHealthNonGet405(t *testing.T) {
	h := Handler(store.New(t.TempDir()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz = %d, want 405", rec.Code)
	}
}

func TestUnknownPath404JSON(t *testing.T) {
	h := Handler(store.New(t.TempDir()))
	rec := get(t, h, "/v1/nope")
	if rec.Code != 404 {
		t.Fatalf("unknown path = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("404 content-type = %q, want json", ct)
	}
	var e errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error == "" {
		t.Fatalf("404 body not a JSON error: %s (%v)", rec.Body.String(), err)
	}
}

func TestRangeHappyPath(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 100))
	rec := get(t, h, "/v1/range?symbol=SYM&from=10&to=12")
	if rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
	var resp rangeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Symbol != "SYM" || resp.From != 10 || resp.To != 12 || resp.Count != 3 {
		t.Fatalf("resp meta wrong: %+v", resp)
	}
	if resp.Ticks[0].TS != 10 || resp.Ticks[2].TS != 12 {
		t.Fatalf("ticks wrong: %+v", resp.Ticks)
	}
	if resp.Ticks[0].Side != "buy" || resp.Ticks[1].Side != "sell" {
		t.Fatalf("side wrong: %+v", resp.Ticks)
	}
}

func TestRangeDefaultsReturnAll(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 40))
	rec := get(t, h, "/v1/range?symbol=SYM")
	var resp rangeResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Count != 40 {
		t.Fatalf("no-bounds range count = %d, want 40", resp.Count)
	}
}

func TestRangeFromGreaterThanToEmpty(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 40))
	rec := get(t, h, "/v1/range?symbol=SYM&from=30&to=10")
	var resp rangeResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec.Code != 200 || resp.Count != 0 || resp.Ticks == nil {
		t.Fatalf("from>to = %d count=%d ticks=%v", rec.Code, resp.Count, resp.Ticks)
	}
}

func TestRangeUnknownSymbolEmpty(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 10))
	rec := get(t, h, "/v1/range?symbol=NOPE")
	var resp rangeResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec.Code != 200 || resp.Count != 0 {
		t.Fatalf("unknown symbol = %d count=%d", rec.Code, resp.Count)
	}
}

func TestRangeValidationErrors(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 10))
	cases := []string{
		"/v1/range",                     // missing symbol
		"/v1/range?symbol=",             // blank symbol
		"/v1/range?symbol=SYM&from=abc", // bad from
		"/v1/range?symbol=SYM&to=xyz",   // bad to
		"/v1/range?symbol=SYM&n=5",      // n not valid for range
	}
	for _, path := range cases {
		if rec := get(t, h, path); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", path, rec.Code)
		}
	}
}

func TestLastHappyPath(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 100))
	rec := get(t, h, "/v1/last?symbol=SYM&n=3")
	var resp lastResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.N != 3 || resp.Count != 3 || resp.Ticks[2].TS != 99 {
		t.Fatalf("last wrong: %+v", resp)
	}
}

func TestLastNLargerThanLog(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 5))
	rec := get(t, h, "/v1/last?symbol=SYM&n=50")
	var resp lastResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Count != 5 {
		t.Fatalf("n>len count = %d, want 5", resp.Count)
	}
}

func TestLastNZeroEmpty(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 5))
	rec := get(t, h, "/v1/last?symbol=SYM&n=0")
	var resp lastResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if rec.Code != 200 || resp.Count != 0 || resp.Ticks == nil {
		t.Fatalf("n=0 = %d count=%d ticks=%v", rec.Code, resp.Count, resp.Ticks)
	}
}

func TestLastValidationErrors(t *testing.T) {
	h := Handler(seedStore(t, "SYM", 10))
	cases := []string{
		"/v1/last?n=3",                   // missing symbol
		"/v1/last?symbol=SYM",            // missing n
		"/v1/last?symbol=SYM&n=abc",      // bad n
		"/v1/last?symbol=SYM&n=-1",       // negative n
		"/v1/last?symbol=SYM&n=3&from=1", // from not valid for last
	}
	for _, path := range cases {
		if rec := get(t, h, path); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", path, rec.Code)
		}
	}
}

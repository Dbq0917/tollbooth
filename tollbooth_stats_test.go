package tollbooth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/didip/tollbooth/v8/limiter"
)

func TestLimitByKeysStatsMatchErrors(t *testing.T) {
	lmt := NewLimiter(2, nil).SetBurst(2)
	keys := []string{"192.0.2.10", "/stats"}
	joinedKey := "192.0.2.10|/stats"

	want := limiter.RequestStats{}
	for i := 0; i < 12; i++ {
		httpError := LimitByKeys(lmt, keys)
		if httpError != nil {
			want.Rejected++
			continue
		}
		want.Allowed++
	}

	if want.Allowed != 2 || want.Rejected != 10 {
		t.Fatalf("unexpected HTTP decision setup: %+v", want)
	}

	if got := lmt.Stats(joinedKey); got != want {
		t.Fatalf("Stats(%q) = %+v, want %+v", joinedKey, got, want)
	}
	if got := lmt.Stats(); got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}
}

func TestLimitHandlerStatsMatchStatuses(t *testing.T) {
	lmt := NewLimiter(2, nil).SetBurst(2)
	lmt.SetIPLookup(limiter.IPLookup{Name: "RemoteAddr"})

	handler := LimitHandler(lmt, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	want := limiter.RequestStats{}
	for i := 0; i < 7; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "192.0.2.7:51234"
		recorder := httptest.NewRecorder()

		handler.ServeHTTP(recorder, req)
		switch recorder.Code {
		case http.StatusOK:
			want.Allowed++
		case http.StatusTooManyRequests:
			want.Rejected++
		default:
			t.Fatalf("unexpected status code: %d", recorder.Code)
		}
	}

	if want.Allowed != 2 || want.Rejected != 5 {
		t.Fatalf("unexpected HTTP status setup: %+v", want)
	}

	if got := lmt.Stats(); got != want {
		t.Fatalf("global Stats() = %+v, want %+v", got, want)
	}
	key := lmt.HottestKey()
	if key == "" {
		t.Fatal("HottestKey() returned empty key after requests")
	}
	if got := lmt.Stats(key); got != want {
		t.Fatalf("Stats(%q) = %+v, want %+v", key, got, want)
	}
}

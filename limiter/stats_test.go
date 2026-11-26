package limiter

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStatsMatchLimitReached(t *testing.T) {
	lmt := New(nil).SetMax(2).SetBurst(2)
	key := "127.0.0.1|stats"

	const requests = 10
	expected := RequestStats{}
	for i := 0; i < requests; i++ {
		if lmt.LimitReached(key) {
			expected.Rejected++
			continue
		}
		expected.Allowed++
	}

	if expected.Allowed != 2 || expected.Rejected != 8 {
		t.Fatalf("unexpected decision setup: %+v", expected)
	}

	assertStatsEqual(t, "key stats", lmt.Stats(key), expected)
	assertStatsEqual(t, "global stats", lmt.Stats(), expected)

	if got := lmt.AllowedRequests(key); got != expected.Allowed {
		t.Fatalf("AllowedRequests(key) = %d, want %d", got, expected.Allowed)
	}
	if got := lmt.RejectedRequests(key); got != expected.Rejected {
		t.Fatalf("RejectedRequests(key) = %d, want %d", got, expected.Rejected)
	}
	if allowed, rejected := lmt.Counts(key); allowed != expected.Allowed || rejected != expected.Rejected {
		t.Fatalf("Counts(key) = (%d, %d), want (%d, %d)", allowed, rejected, expected.Allowed, expected.Rejected)
	}
	if got := lmt.TotalRequests(key); got != requests {
		t.Fatalf("TotalRequests(key) = %d, want %d", got, requests)
	}
	if math.Abs(lmt.RejectionRate(key)-0.8) > 1e-12 {
		t.Fatalf("RejectionRate(key) = %v, want 0.8", lmt.RejectionRate(key))
	}
	if lmt.Tokens(key) != 0 {
		t.Fatalf("Tokens(key) = %d, want 0", lmt.Tokens(key))
	}
}

func TestStatsAggregateAcrossKeys(t *testing.T) {
	lmt := New(nil).SetMax(1).SetBurst(1)
	firstKey := "first"
	secondKey := "second"

	firstDecisions := []bool{false, true, true}
	secondDecisions := []bool{false, true}

	for _, rejected := range firstDecisions {
		if got := lmt.LimitReached(firstKey); got != rejected {
			t.Fatalf("first key decision = %v, want %v", got, rejected)
		}
	}
	for _, rejected := range secondDecisions {
		if got := lmt.LimitReached(secondKey); got != rejected {
			t.Fatalf("second key decision = %v, want %v", got, rejected)
		}
	}

	assertStatsEqual(t, "first stats", lmt.Stats(firstKey), RequestStats{Allowed: 1, Rejected: 2})
	assertStatsEqual(t, "second stats", lmt.Stats(secondKey), RequestStats{Allowed: 1, Rejected: 1})
	assertStatsEqual(t, "global stats", lmt.Stats(), RequestStats{Allowed: 2, Rejected: 3})

	if got := lmt.HottestKey(); got != firstKey {
		t.Fatalf("HottestKey() = %q, want %q", got, firstKey)
	}

	if got := lmt.GetAllowedCount(); got != 2 {
		t.Fatalf("GetAllowedCount() = %d, want 2", got)
	}
	if got := lmt.GetRejectedCount(); got != 3 {
		t.Fatalf("GetRejectedCount() = %d, want 3", got)
	}
}

func TestStatsSurviveTokenBucketExpiry(t *testing.T) {
	lmt := New(&ExpirableOptions{DefaultExpirationTTL: time.Minute}).
		SetMax(1).
		SetBurst(1).
		SetTokenBucketExpirationTTL(25 * time.Millisecond)
	key := "expiring"

	if lmt.LimitReached(key) {
		t.Fatal("first request should be allowed")
	}
	if !lmt.LimitReached(key) {
		t.Fatal("second request should be rejected")
	}

	time.Sleep(75 * time.Millisecond)
	lmt.DeleteExpiredTokenBuckets()

	if _, found := lmt.tokenBuckets.Get(key); found {
		t.Fatal("token bucket should have expired")
	}

	if lmt.LimitReached(key) {
		t.Fatal("first request against a replacement bucket should be allowed")
	}

	want := RequestStats{Allowed: 2, Rejected: 1}
	assertStatsEqual(t, "key stats after expiry", lmt.Stats(key), want)
	assertStatsEqual(t, "global stats after expiry", lmt.Stats(), want)
}

func TestStatsConcurrentDecisions(t *testing.T) {
	lmt := New(nil).SetMax(1).SetBurst(1)
	key := "concurrent"

	const goroutines = 16
	const requestsPerGoroutine = 50

	var allowed int64
	var rejected int64
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				if lmt.LimitReached(key) {
					atomic.AddInt64(&rejected, 1)
				} else {
					atomic.AddInt64(&allowed, 1)
				}
			}
		}()
	}

	wg.Wait()

	want := RequestStats{
		Allowed:  atomic.LoadInt64(&allowed),
		Rejected: atomic.LoadInt64(&rejected),
	}
	assertStatsEqual(t, "concurrent key stats", lmt.Stats(key), want)
	assertStatsEqual(t, "concurrent global stats", lmt.Stats(), want)

	if got, wantTotal := lmt.TotalRequests(key), int64(goroutines*requestsPerGoroutine); got != wantTotal {
		t.Fatalf("TotalRequests(key) = %d, want %d", got, wantTotal)
	}
}

func assertStatsEqual(t *testing.T, label string, got, want RequestStats) {
	t.Helper()

	if got != want {
		t.Fatalf("%s = %+v, want %+v", label, got, want)
	}
}

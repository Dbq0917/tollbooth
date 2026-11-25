package limiter

import (
	"fmt"
	"testing"
	"time"

	tbErrors "github.com/didip/tollbooth/v8/errors"
)

func TestConstructor(t *testing.T) {
	lmt := New(nil).SetMax(1)
	if lmt.GetMax() != 1 {
		t.Errorf("Max field is incorrect. Value: %v", lmt.GetMax())
	}
	if lmt.GetMessage() != "You have reached maximum request limit." {
		t.Errorf("Message field is incorrect. Value: %v", lmt.GetMessage())
	}
	if lmt.GetStatusCode() != 429 {
		t.Errorf("StatusCode field is incorrect. Value: %v", lmt.GetStatusCode())
	}
}

func TestConstructorExpiringBuckets(t *testing.T) {
	lmt := New(&ExpirableOptions{DefaultExpirationTTL: time.Second}).SetMax(1)
	if lmt.GetMax() != 1 {
		t.Errorf("Max field is incorrect. Value: %v", lmt.GetMax())
	}
	if lmt.GetMessage() != "You have reached maximum request limit." {
		t.Errorf("Message field is incorrect. Value: %v", lmt.GetMessage())
	}
	if lmt.GetStatusCode() != 429 {
		t.Errorf("StatusCode field is incorrect. Value: %v", lmt.GetStatusCode())
	}
}

func TestLimitReached(t *testing.T) {
	lmt := New(nil).SetMax(1).SetBurst(1)
	key := "127.0.0.1|/"

	if lmt.LimitReached(key) == true {
		t.Error("First time count should not reached the limit.")
	}

	if lmt.LimitReached(key) == false {
		t.Error("Second time count should return true because it exceeds 1 request per second.")
	}

	<-time.After(1 * time.Second)
	if lmt.LimitReached(key) == true {
		t.Error("Third time count should not reached the limit because the 1 second window has passed.")
	}
}

func TestFloatingLimitReached(t *testing.T) {
	lmt := New(nil).SetMax(0.1).SetBurst(1)
	key := "127.0.0.1|/"

	if lmt.LimitReached(key) == true {
		t.Error("First time count should not reached the limit.")
	}

	if lmt.LimitReached(key) == false {
		t.Error("Second time count should return true because it exceeds 1 request per 10 seconds.")
	}

	<-time.After(7 * time.Second)
	if lmt.LimitReached(key) == false {
		t.Error("Third time count should return true because it exceeds 1 request per 10 seconds.")
	}

	<-time.After(3 * time.Second)
	if lmt.LimitReached(key) == true {
		t.Error("Fourth time count should not reached the limit because the 10 second window has passed.")
	}
}

func TestLimitReachedWithCustomTokenBucketTTL(t *testing.T) {
	lmt := New(&ExpirableOptions{DefaultExpirationTTL: time.Second}).SetMax(1).SetBurst(1)
	key := "127.0.0.1|/"

	if lmt.LimitReached(key) == true {
		t.Error("First time count should not reached the limit.")
	}

	if lmt.LimitReached(key) == false {
		t.Error("Second time count should return true because it exceeds 1 request per second.")
	}

	<-time.After(1 * time.Second)
	if lmt.LimitReached(key) == true {
		t.Error("Third time count should not reached the limit because the 1 second window has passed.")
	}
}

func TestMuchHigherMaxRequests(t *testing.T) {
	numRequests := 1000
	delay := (1 * time.Second) / time.Duration(numRequests)
	lmt := New(nil).SetMax(float64(numRequests)).SetBurst(1)
	key := "127.0.0.1|/"

	for i := 0; i < numRequests; i++ {
		time.Sleep(delay)
		if lmt.LimitReached(key) == true {
			t.Errorf("N(%v) limit should not be reached.", i)
		}
	}

	if lmt.LimitReached(key) == false {
		t.Errorf("N(%v) limit should be reached because it exceeds %v request per second.", numRequests+2, numRequests)
	}

}

func TestMuchHigherMaxRequestsWithCustomTokenBucketTTL(t *testing.T) {
	numRequests := 1000
	delay := (1 * time.Second) / time.Duration(numRequests)
	lmt := New(&ExpirableOptions{DefaultExpirationTTL: time.Minute}).SetMax(float64(numRequests)).SetBurst(1)
	key := "127.0.0.1|/"

	for i := 0; i < numRequests; i++ {
		time.Sleep(delay)
		if lmt.LimitReached(key) == true {
			fmt.Printf("N(%v) limit should not be reached.\n", i)
		}
	}

	if lmt.LimitReached(key) == false {
		t.Errorf("N(%v) limit should be reached because it exceeds %v request per second.", numRequests+1, numRequests)
	}

}

func TestGlobalLimitReached(t *testing.T) {
	lmt := New(nil).SetMax(1000).SetBurst(1000).
		SetGlobalMax(0.001).
		SetGlobalBurst(3)

	for i := 0; i < 3; i++ {
		if lmt.GlobalLimitReached() {
			t.Fatalf("global request %d should be allowed", i+1)
		}
	}
	if !lmt.GlobalLimitReached() {
		t.Fatal("fourth global request should be rejected")
	}
	if tokens := lmt.GlobalTokens(); tokens != 0 {
		t.Error("global bucket should be exhausted, tokens left:", tokens)
	}
}

func TestGlobalLimitDisabledByDefaultAndCanBeDisabled(t *testing.T) {
	lmt := New(nil).SetMax(1).SetBurst(1)
	if lmt.GlobalLimitEnabled() {
		t.Fatal("global limit should be disabled by default")
	}
	if lmt.GlobalLimitReached() {
		t.Fatal("disabled global limit should not reject")
	}

	lmt.SetGlobalMax(1)
	if !lmt.GlobalLimitEnabled() {
		t.Fatal("global limit should be enabled")
	}
	lmt.SetGlobalMax(0)
	if lmt.GlobalLimitEnabled() {
		t.Fatal("zero global max should disable the global limit")
	}
}

func TestGlobalLimitFromExpirableOptions(t *testing.T) {
	lmt := New(&ExpirableOptions{GlobalMax: 0.001, GlobalBurst: 2}).SetMax(1000).SetBurst(1000)

	if got := lmt.GetGlobalMax(); got != 0.001 {
		t.Errorf("GetGlobalMax() = %v, want 0.001", got)
	}
	if got := lmt.GetGlobalBurst(); got != 2 {
		t.Errorf("GetGlobalBurst() = %d, want 2", got)
	}
	if lmt.GlobalLimitReached() || lmt.GlobalLimitReached() {
		t.Fatal("both requests in the global burst should be allowed")
	}
	if !lmt.GlobalLimitReached() {
		t.Fatal("request above the global burst should be rejected")
	}
}

func TestLimitReachedIncludesGlobalLayerAndReason(t *testing.T) {
	lmt := New(nil).SetMax(1000).SetBurst(1000).
		SetGlobalMax(0.001).
		SetGlobalBurst(2)

	for i := 0; i < 2; i++ {
		if lmt.LimitReached(fmt.Sprintf("key-%d", i)) {
			t.Fatalf("global request %d should be allowed", i+1)
		}
	}

	limitReached, reason := lmt.LimitReachedWithReason("key-3")
	if !limitReached {
		t.Fatal("request above the global burst should be rejected")
	}
	if reason != tbErrors.ReasonGlobal {
		t.Errorf("rejection reason = %q, want %q", reason, tbErrors.ReasonGlobal)
	}
}

package limiter

import (
	"fmt"
	"testing"
	"time"
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

func TestInitialTokens(t *testing.T) {
	lmt := New(nil).SetMax(1).SetBurst(5).SetInitialTokens(3)
	key := "127.0.0.1|/"

	for i := 0; i < 3; i++ {
		if lmt.LimitReached(key) {
			t.Errorf("request %d should be allowed by the initial tokens", i+1)
		}
	}
	if !lmt.LimitReached(key) {
		t.Error("request after consuming the initial tokens should be rejected")
	}
}

func TestInitialTokensAreClampedToBurst(t *testing.T) {
	lmt := New(&ExpirableOptions{InitialTokens: 100}).SetMax(1).SetBurst(5)
	key := "127.0.0.1|/"

	for i := 0; i < 5; i++ {
		if lmt.LimitReached(key) {
			t.Errorf("request %d should be allowed by the clamped initial tokens", i+1)
		}
	}
	if !lmt.LimitReached(key) {
		t.Error("request beyond the burst size should be rejected")
	}
}

func TestRefillInterval(t *testing.T) {
	const (
		interval = 75 * time.Millisecond
		refill   = 2
	)
	lmt := New(nil).
		SetMax(2).
		SetBurst(4).
		SetInitialTokens(4).
		SetRefillInterval(interval).
		SetRefillTokens(refill)
	key := "127.0.0.1|/"

	for i := 0; i < 4; i++ {
		if lmt.LimitReached(key) {
			t.Fatalf("request %d should be allowed by the initial tokens", i+1)
		}
	}
	if !lmt.LimitReached(key) {
		t.Fatal("request after consuming the initial tokens should be rejected")
	}

	time.Sleep(interval / 2)
	if !lmt.LimitReached(key) {
		t.Error("request before the first refill interval should be rejected")
	}

	time.Sleep(interval/2 + 10*time.Millisecond)
	for i := 0; i < refill; i++ {
		if lmt.LimitReached(key) {
			t.Errorf("request %d after the refill interval should be allowed", i+1)
		}
	}
	if !lmt.LimitReached(key) {
		t.Error("request beyond the refill batch should be rejected")
	}
}

func TestInitialTokensAreNotRegrantedToActiveBucket(t *testing.T) {
	lmt := New(&ExpirableOptions{
		DefaultExpirationTTL: 25 * time.Millisecond,
		InitialTokens:        3,
	}).SetMax(0.1).SetBurst(3)
	key := "127.0.0.1|/"

	for i := 0; i < 3; i++ {
		if lmt.LimitReached(key) {
			t.Fatalf("request %d should be allowed by the initial tokens", i+1)
		}
	}

	allowed := 0
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !lmt.LimitReached(key) {
			allowed++
		}
		time.Sleep(5 * time.Millisecond)
	}

	if allowed > 0 {
		t.Errorf("active bucket was re-seeded with initial tokens %d times", allowed)
	}
}

func TestInitialTokensAreRegrantedAfterIdleExpiration(t *testing.T) {
	lmt := New(&ExpirableOptions{
		DefaultExpirationTTL: 25 * time.Millisecond,
		InitialTokens:        2,
	}).SetMax(1).SetBurst(2)
	key := "127.0.0.1|/"

	for i := 0; i < 2; i++ {
		if lmt.LimitReached(key) {
			t.Fatalf("request %d should be allowed by the initial tokens", i+1)
		}
	}
	if !lmt.LimitReached(key) {
		t.Fatal("request after consuming the initial tokens should be rejected")
	}

	time.Sleep(40 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if lmt.LimitReached(key) {
			t.Errorf("request %d after the idle expiration should be allowed by the initial tokens", i+1)
		}
	}
}

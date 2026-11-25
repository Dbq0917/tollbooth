// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rate provides a rate limiter.
package rate

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// Informally, in any large enough time interval, the Limiter limits the
// rate to r tokens per second, with a maximum burst size of b events.
// As a special case, if r == Inf (the infinite rate), b is ignored.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// The zero value is a valid Limiter, but it will reject all events.
// Use NewLimiter to create non-zero Limiters.
//
// Limiter has three main methods, Allow, Reserve, and Wait.
// Most callers should use Wait.
//
// Each of the three methods consumes a single token.
// They differ in their behavior when no token is available.
// If no token is available, Allow returns false.
// If no token is available, Reserve returns a reservation for a future token
// and the amount of time the caller must wait before using it.
// If no token is available, Wait blocks until one can be obtained
// or its associated context.Context is canceled.
//
// The methods AllowN, ReserveN, and WaitN consume n tokens.
type Limiter struct {
	mu     sync.Mutex
	limit  Limit
	burst  int
	tokens float64
	// refillInterval switches token refills from continuous to batch mode.
	// A zero interval keeps the standard continuous refill behavior.
	refillInterval time.Duration
	// refillTokens is the number of tokens granted per refillInterval.
	// When it is zero, the amount is derived from limit and refillInterval.
	refillTokens float64
	// refillRate overrides the continuous token refill rate.
	refillRate Limit
	// last is the last time the limiter's tokens field was updated
	last time.Time
	// lastEvent is the latest time of a rate-limited event (past or future)
	lastEvent time.Time
}

// LimiterConfig controls optional token bucket behavior.
type LimiterConfig struct {
	// InitialTokens is the number of tokens available when the bucket is created.
	// It is clamped to the configured burst size.
	InitialTokens int
	// InitialTokensSet distinguishes an explicit zero from an unset value.
	InitialTokensSet bool

	// RefillInterval controls how often tokens are added after the bucket has
	// been created. A zero value refills tokens continuously.
	RefillInterval time.Duration

	// RefillTokens is the number of tokens added on each RefillInterval.
	// A zero value derives the amount from the limit and RefillInterval,
	// preserving the configured average rate.
	RefillTokens int

	// RefillRate overrides the continuous token refill rate. A zero value
	// uses the configured limit.
	RefillRate Limit

	// Initial is an alias for InitialTokens.
	Initial int
	// InitialBurst is an alias for InitialTokens.
	InitialBurst int
	// WarmupTokens is an alias for InitialTokens.
	WarmupTokens int

	// RefillEvery is an alias for RefillInterval.
	RefillEvery time.Duration
	// RecoveryInterval is an alias for RefillInterval.
	RecoveryInterval time.Duration

	// RefillAmount is an alias for RefillTokens.
	RefillAmount int
	// RecoveryTokens is an alias for RefillTokens.
	RecoveryTokens int
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

// Burst returns the maximum burst size. Burst is the maximum number of tokens
// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
// Burst values allow more events to happen at once.
// A zero Burst allows no events, unless limit == Inf.
func (lim *Limiter) Burst() int {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.burst
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(r Limit, b int) *Limiter {
	return NewLimiterWithOptions(r, b)
}

// NewLimiterWithConfig returns a new Limiter with optional initial tokens and
// a custom refill cadence.
func NewLimiterWithConfig(r Limit, b int, cfg LimiterConfig) *Limiter {
	return NewLimiterWithOptions(r, b, cfg)
}

// NewLimiterWithOptions returns a new Limiter with optional configuration.
// With no configuration it keeps the standard token bucket behavior.
func NewLimiterWithOptions(r Limit, b int, configs ...LimiterConfig) *Limiter {
	if len(configs) == 0 {
		return &Limiter{
			limit: r,
			burst: b,
		}
	}

	cfg := mergeLimiterConfigs(configs)
	initialSet := cfg.InitialTokensSet || cfg.InitialTokens > 0 ||
		cfg.Initial > 0 || cfg.InitialBurst > 0 || cfg.WarmupTokens > 0
	initial := cfg.InitialTokens
	if initial == 0 {
		switch {
		case cfg.Initial > 0:
			initial = cfg.Initial
		case cfg.InitialBurst > 0:
			initial = cfg.InitialBurst
		case cfg.WarmupTokens > 0:
			initial = cfg.WarmupTokens
		}
	}
	if !initialSet {
		initial = b
	}
	if initial < 0 {
		initial = 0
	}
	if initial > b {
		initial = b
	}

	lim := &Limiter{
		limit:          r,
		burst:          b,
		tokens:         float64(initial),
		last:           time.Now(),
		refillInterval: cfg.RefillInterval,
		refillRate:     cfg.RefillRate,
	}
	if lim.refillInterval <= 0 {
		switch {
		case cfg.RefillEvery > 0:
			lim.refillInterval = cfg.RefillEvery
		case cfg.RecoveryInterval > 0:
			lim.refillInterval = cfg.RecoveryInterval
		}
	}
	if cfg.RefillTokens > 0 {
		lim.refillTokens = float64(cfg.RefillTokens)
	} else {
		switch {
		case cfg.RefillAmount > 0:
			lim.refillTokens = float64(cfg.RefillAmount)
		case cfg.RecoveryTokens > 0:
			lim.refillTokens = float64(cfg.RecoveryTokens)
		}
	}
	return lim
}

func mergeLimiterConfigs(configs []LimiterConfig) LimiterConfig {
	var cfg LimiterConfig
	for _, next := range configs {
		if next.InitialTokensSet || next.InitialTokens != 0 {
			cfg.InitialTokens = next.InitialTokens
			cfg.InitialTokensSet = true
		}
		if next.Initial > 0 {
			cfg.Initial = next.Initial
		}
		if next.InitialBurst > 0 {
			cfg.InitialBurst = next.InitialBurst
		}
		if next.WarmupTokens > 0 {
			cfg.WarmupTokens = next.WarmupTokens
		}
		if next.RefillInterval > 0 {
			cfg.RefillInterval = next.RefillInterval
		}
		if next.RefillEvery > 0 {
			cfg.RefillEvery = next.RefillEvery
		}
		if next.RecoveryInterval > 0 {
			cfg.RecoveryInterval = next.RecoveryInterval
		}
		if next.RefillTokens > 0 {
			cfg.RefillTokens = next.RefillTokens
		}
		if next.RefillAmount > 0 {
			cfg.RefillAmount = next.RefillAmount
		}
		if next.RecoveryTokens > 0 {
			cfg.RecoveryTokens = next.RecoveryTokens
		}
		if next.RefillRate > 0 {
			cfg.RefillRate = next.RefillRate
		}
	}
	return cfg
}

// WithInitialTokens returns a limiter option that sets the initial token count.
func WithInitialTokens(tokens int) LimiterConfig {
	return LimiterConfig{InitialTokens: tokens, InitialTokensSet: true}
}

// WithWarmupTokens is an alias for WithInitialTokens.
func WithWarmupTokens(tokens int) LimiterConfig {
	return WithInitialTokens(tokens)
}

// WithRefillInterval returns a limiter option that sets the refill cadence.
func WithRefillInterval(interval time.Duration) LimiterConfig {
	return LimiterConfig{RefillInterval: interval}
}

// WithRecoveryInterval is an alias for WithRefillInterval.
func WithRecoveryInterval(interval time.Duration) LimiterConfig {
	return WithRefillInterval(interval)
}

// WithRefillTokens returns a limiter option that sets tokens per refill interval.
func WithRefillTokens(tokens int) LimiterConfig {
	return LimiterConfig{RefillTokens: tokens}
}

// WithRecoveryTokens is an alias for WithRefillTokens.
func WithRecoveryTokens(tokens int) LimiterConfig {
	return WithRefillTokens(tokens)
}

// WithRefillRate returns a limiter option that sets the continuous refill rate.
func WithRefillRate(rate Limit) LimiterConfig {
	return LimiterConfig{RefillRate: rate}
}

// NewLimiterWithTokens returns a new Limiter with an explicit initial token count.
func NewLimiterWithTokens(r Limit, b int, initialTokens int) *Limiter {
	return NewLimiterWithOptions(r, b, LimiterConfig{InitialTokens: initialTokens, InitialTokensSet: true})
}

// NewLimiterWithInitialTokens is an alias for NewLimiterWithTokens.
func NewLimiterWithInitialTokens(r Limit, b int, initialTokens int) *Limiter {
	return NewLimiterWithTokens(r, b, initialTokens)
}

// SetInitialTokens sets the number of tokens granted when the limiter is created.
// The value is clamped to the burst size.
func (lim *Limiter) SetInitialTokens(tokens int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.tokens = float64(clampTokens(tokens, lim.burst))
	lim.last = time.Now()
}

// SetRefillInterval sets the cadence used to add tokens.
// A zero or negative interval restores continuous refill behavior.
func (lim *Limiter) SetRefillInterval(interval time.Duration) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.refillInterval = interval
}

// SetRefillTokens sets the number of tokens added on each refill interval.
// A zero value derives the amount from the limit and interval.
func (lim *Limiter) SetRefillTokens(tokens int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	if tokens < 0 {
		tokens = 0
	}
	lim.refillTokens = float64(tokens)
}

// SetRefillRate sets the continuous token refill rate.
func (lim *Limiter) SetRefillRate(rate Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.refillRate = rate
}

func clampTokens(tokens, burst int) int {
	if tokens < 0 {
		return 0
	}
	if tokens > burst {
		return burst
	}
	return tokens
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// TokensAt returns the number of tokens available for the given time.
func (lim *Limiter) TokensAt(t time.Time) float64 {
	lim.mu.Lock()
	_, _, tokens := lim.advance(t) // does not mutate lim
	lim.mu.Unlock()
	return tokens
}

// AllowN reports whether n events may happen at time now.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// Otherwise use Reserve or Wait.
func (lim *Limiter) AllowN(now time.Time, n int) bool {
	allowed, _ := lim.AllowNWithTokens(now, n)
	return allowed
}

// AllowNWithTokens reports whether n events may happen at time now and returns
// the number of tokens remaining after applying the same calculation.
func (lim *Limiter) AllowNWithTokens(now time.Time, n int) (bool, float64) {
	r, tokens := lim.reserveNWithTokens(now, n, 0)
	return r.ok, tokens
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok        bool
	lim       *Limiter
	tokens    int
	timeToAct time.Time
	// This is the Limit at reservation time, it can change later.
	limit Limit
}

// OK returns whether the limiter can provide the requested number of tokens
// within the maximum wait time.  If OK is false, Delay returns InfDuration, and
// Cancel does nothing.
func (r *Reservation) OK() bool {
	return r.ok
}

// Delay is shorthand for DelayFrom(time.Now()).
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(1<<63 - 1)

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// InfDuration means the limiter cannot grant the tokens requested in this
// Reservation within the maximum wait time.
func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

// Cancel is shorthand for CancelAt(time.Now()).
func (r *Reservation) Cancel() {
	r.CancelAt(time.Now())
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible,
// considering that other reservations may have already been made.
func (r *Reservation) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	r.lim.mu.Lock()
	defer r.lim.mu.Unlock()

	if r.lim.limit == Inf || r.tokens == 0 || r.timeToAct.Before(now) {
		return
	}

	// calculate tokens to restore
	// The duration between lim.lastEvent and r.timeToAct tells us how many tokens were reserved
	// after r was obtained. These tokens should not be restored.
	restoreTokens := float64(r.tokens) - r.limit.tokensFromDuration(r.lim.lastEvent.Sub(r.timeToAct))
	if restoreTokens <= 0 {
		return
	}
	// advance time to now
	newNow, _, tokens := r.lim.advance(now)
	// calculate new number of tokens
	tokens += restoreTokens
	if burst := float64(r.lim.burst); tokens > burst {
		tokens = burst
	}
	// update state
	r.lim.last = newNow
	r.lim.tokens = tokens
	if r.timeToAct == r.lim.lastEvent {
		prevEvent := r.timeToAct.Add(r.limit.durationFromTokens(float64(-r.tokens)))
		if !prevEvent.Before(now) {
			r.lim.lastEvent = prevEvent
		}
	}
}

// Reserve is shorthand for ReserveN(time.Now(), 1).
func (lim *Limiter) Reserve() *Reservation {
	return lim.ReserveN(time.Now(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
// The Limiter takes this Reservation into account when allowing future events.
// The returned Reservation’s OK() method returns false if n exceeds the Limiter's burst size.
// Usage example:
//
//	r := lim.ReserveN(time.Now(), 1)
//	if !r.OK() {
//	  // Not allowed to act! Did you remember to set lim.burst to be > 0 ?
//	  return
//	}
//	time.Sleep(r.Delay())
//	Act()
//
// Use this method if you wish to wait and slow down in accordance with the rate limit without dropping events.
// If you need to respect a deadline or cancel the delay, use Wait instead.
// To drop or skip events exceeding rate limit, use Allow instead.
func (lim *Limiter) ReserveN(now time.Time, n int) *Reservation {
	r := lim.reserveN(now, n, InfDuration)
	return &r
}

// Wait is shorthand for WaitN(ctx, 1).
func (lim *Limiter) Wait(ctx context.Context) (err error) {
	return lim.WaitN(ctx, 1)
}

// WaitN blocks until lim permits n events to happen.
// It returns an error if n exceeds the Limiter's burst size, the Context is
// canceled, or the expected wait time exceeds the Context's Deadline.
// The burst limit is ignored if the rate limit is Inf.
func (lim *Limiter) WaitN(ctx context.Context, n int) (err error) {
	lim.mu.Lock()
	burst := lim.burst
	limit := lim.limit
	lim.mu.Unlock()

	if n > burst && limit != Inf {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's burst %d", n, burst)
	}
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Determine wait limit
	now := time.Now()
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now)
	}
	// Reserve
	r := lim.reserveN(now, n, waitLimit)
	if !r.ok {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}
	// Wait if necessary
	delay := r.DelayFrom(now)
	if delay == 0 {
		return nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		// We can proceed.
		return nil
	case <-ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		r.Cancel()
		return ctx.Err()
	}
}

// SetLimit is shorthand for SetLimitAt(time.Now(), newLimit).
func (lim *Limiter) SetLimit(newLimit Limit) {
	lim.SetLimitAt(time.Now(), newLimit)
}

// SetLimitAt sets a new Limit for the limiter. The new Limit, and Burst, may be violated
// or underutilized by those which reserved (using Reserve or Wait) but did not yet act
// before SetLimitAt was called.
func (lim *Limiter) SetLimitAt(now time.Time, newLimit Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	newNow, _, tokens := lim.advance(now)

	lim.last = newNow
	lim.tokens = tokens
	lim.limit = newLimit
}

// SetBurst is shorthand for SetBurstAt(time.Now(), newBurst).
func (lim *Limiter) SetBurst(newBurst int) {
	lim.SetBurstAt(time.Now(), newBurst)
}

// SetBurstAt sets a new burst size for the limiter.
func (lim *Limiter) SetBurstAt(now time.Time, newBurst int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	newNow, _, tokens := lim.advance(now)

	lim.last = newNow
	lim.tokens = tokens
	lim.burst = newBurst
}

// reserveN is a helper method for AllowN, ReserveN, and WaitN.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserveN returns Reservation, not *Reservation, to avoid allocation in AllowN and WaitN.
func (lim *Limiter) reserveN(now time.Time, n int, maxFutureReserve time.Duration) Reservation {
	r, _ := lim.reserveNWithTokens(now, n, maxFutureReserve)
	return r
}

func (lim *Limiter) reserveNWithTokens(now time.Time, n int, maxFutureReserve time.Duration) (Reservation, float64) {
	lim.mu.Lock()

	if lim.limit == Inf {
		tokens := math.Inf(1)
		lim.mu.Unlock()
		return Reservation{
			ok:        true,
			lim:       lim,
			tokens:    n,
			timeToAct: now,
		}, tokens
	}

	newNow, last, tokens := lim.advance(now)

	// Calculate the remaining number of tokens resulting from the request.
	tokens -= float64(n)

	// Calculate the wait duration
	var waitDuration time.Duration
	if tokens < 0 {
		waitDuration = lim.limit.durationFromTokens(-tokens)
	}

	// Decide result
	ok := n <= lim.burst && waitDuration <= maxFutureReserve

	// Prepare reservation
	r := Reservation{
		ok:    ok,
		lim:   lim,
		limit: lim.limit,
	}
	if ok {
		r.tokens = n
		r.timeToAct = now.Add(waitDuration)
	}

	// Update state
	if ok {
		lim.last = newNow
		lim.tokens = tokens
		lim.lastEvent = r.timeToAct
	} else {
		lim.last = last
	}

	lim.mu.Unlock()

	if tokens < 0 {
		return r, 0
	}
	return r, tokens
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
func (lim *Limiter) advance(now time.Time) (newNow time.Time, newLast time.Time, newTokens float64) {
	last := lim.last
	if now.Before(last) {
		last = now
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := now.Sub(last)
	if lim.refillInterval > 0 {
		intervals := int64(elapsed / lim.refillInterval)
		if intervals <= 0 {
			return last, last, lim.tokens
		}

		tokens := lim.tokens + lim.refillTokensFor(intervals)
		if burst := float64(lim.burst); tokens > burst {
			tokens = burst
			// Once the bucket is full there is no useful refill phase to carry.
			return now, now, tokens
		}
		return last.Add(time.Duration(intervals) * lim.refillInterval), last, tokens
	}

	refillLimit := lim.limit
	if lim.refillRate > 0 {
		refillLimit = lim.refillRate
	}
	delta := refillLimit.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}
	return now, last, tokens
}

func (lim *Limiter) refillTokensFor(intervals int64) float64 {
	if lim.refillTokens > 0 {
		return lim.refillTokens * float64(intervals)
	}
	refillLimit := lim.limit
	if lim.refillRate > 0 {
		refillLimit = lim.refillRate
	}
	return refillLimit.tokensFromDuration(time.Duration(intervals) * lim.refillInterval)
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of limit tokens per second.
func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	seconds := tokens / float64(limit)
	return time.Duration(float64(time.Second) * seconds)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	return d.Seconds() * float64(limit)
}

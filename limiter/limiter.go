// Package limiter provides data structure to configure rate-limiter.
package limiter

import (
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	cache "github.com/go-pkgz/expirable-cache/v3"

	tbErrors "github.com/didip/tollbooth/v8/errors"
	"github.com/didip/tollbooth/v8/internal/time/rate"
)

// New is a constructor for Limiter.
func New(generalExpirableOptions *ExpirableOptions) *Limiter {
	lmt := &Limiter{}

	lmt.SetMessageContentType("text/plain; charset=utf-8").
		SetMessage("You have reached maximum request limit.").
		SetStatusCode(429).
		SetOnLimitReached(nil).
		SetForwardedForIndexFromBehind(0).
		SetHeaders(make(map[string][]string)).
		SetContextValues(make(map[string][]string)).
		SetIgnoreURL(false)

	if generalExpirableOptions != nil {
		lmt.generalExpirableOptions = generalExpirableOptions
	} else {
		lmt.generalExpirableOptions = &ExpirableOptions{}
	}

	// Default for DefaultExpirationTTL is 10 years.
	if lmt.generalExpirableOptions.DefaultExpirationTTL <= 0 {
		lmt.generalExpirableOptions.DefaultExpirationTTL = 87600 * time.Hour
	}

	lmt.tokenBuckets = cache.NewCache[string, *rate.Limiter]().WithTTL(lmt.generalExpirableOptions.DefaultExpirationTTL)

	lmt.basicAuthUsers = cache.NewCache[string, bool]().WithTTL(lmt.generalExpirableOptions.DefaultExpirationTTL)

	if lmt.generalExpirableOptions.GlobalMax > 0 {
		if lmt.generalExpirableOptions.GlobalBurst > 0 {
			lmt.SetGlobalBurst(lmt.generalExpirableOptions.GlobalBurst)
		}
		lmt.SetGlobalMax(lmt.generalExpirableOptions.GlobalMax)
	}

	return lmt
}

// IPLookup is a config struct to define how users want to pick the remote IP address.
type IPLookup struct {
	// The name of lookup method.
	// Possible options are: RemoteAddr, X-Forwarded-For, X-Real-IP, CF-Connecting-IP
	// All other headers are considered unknown and will be ignored.
	Name string

	// The index position to pick the ip address from a comma separated list.
	// The index goes from right to left.
	IndexFromRight int
}

type globalLimitConfig struct {
	max           float64
	burst         int
	burstExplicit bool
	bucket        *globalLimitBucket
}

// globalLimitBucket is a lock-free GCRA bucket. Each decision uses one atomic
// compare-and-swap, so unrelated keys do not contend on the Limiter mutex.
type globalLimitBucket struct {
	emissionInterval int64
	burst            int
	tat              atomic.Int64
}

func newGlobalLimitBucket(max float64, burst int) *globalLimitBucket {
	if burst < 1 {
		burst = 1
	}

	interval := int64(float64(time.Second) / max)
	if interval < 1 {
		interval = 1
	}

	return &globalLimitBucket{
		emissionInterval: interval,
		burst:            burst,
	}
}

func defaultGlobalBurst(max float64) int {
	if max <= 0 {
		return 1
	}
	return int(math.Max(1, math.Ceil(max)))
}

func (b *globalLimitBucket) allow(now int64) (bool, int) {
	tolerance := float64(b.emissionInterval) * float64(b.burst)

	for {
		tat := b.tat.Load()
		base := now
		if tat > base {
			base = tat
		}
		if base > math.MaxInt64-b.emissionInterval {
			return false, 0
		}

		next := base + b.emissionInterval
		delay := float64(next - now)
		if delay > tolerance {
			return false, 0
		}

		if b.tat.CompareAndSwap(tat, next) {
			remaining := int((tolerance - delay) / float64(b.emissionInterval))
			if remaining < 0 {
				remaining = 0
			}
			return true, remaining
		}
	}
}

func (b *globalLimitBucket) tokensAt(now int64) int {
	tat := b.tat.Load()
	if tat <= now {
		return b.burst
	}

	used := float64(tat-now) / float64(b.emissionInterval)
	remaining := b.burst - int(math.Ceil(used))
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Limiter is a config struct to limit a particular request handler.
type Limiter struct {
	// Maximum number of requests to limit per second.
	max float64

	// Limiter burst size
	burst int

	// HTTP message when limit is reached.
	message string

	// Content-Type for Message
	messageContentType string

	// HTTP status code when limit is reached.
	statusCode int

	// A function to call when a request is rejected.
	onLimitReached func(w http.ResponseWriter, r *http.Request)

	// An option to write back what you want upon reaching a limit.
	overrideDefaultResponseWriter bool

	// Explicitly define how to look up IP address.
	// This is intended to  replace ipLookups
	explicitIPLookup IPLookup

	forwardedForIndex int

	// List of HTTP Methods to limit (GET, POST, PUT, etc.).
	// Empty means limit all methods.
	methods []string

	// Able to configure token bucket expirations.
	generalExpirableOptions *ExpirableOptions

	// List of basic auth usernames to limit.
	basicAuthUsers cache.Cache[string, bool]

	// Map of HTTP headers to limit.
	// Empty means skip headers checking.
	headers map[string]cache.Cache[string, bool]

	// Map of Context values to limit.
	contextValues map[string]cache.Cache[string, bool]

	// Map of limiters with TTL
	tokenBuckets cache.Cache[string, *rate.Limiter]

	// Aggregate limiter shared by every key. A nil or disabled config means
	// only per-key limits apply.
	globalLimit atomic.Pointer[globalLimitConfig]

	// Ignore URL on the rate limiter keys
	ignoreURL bool

	tokenBucketExpirationTTL  time.Duration
	basicAuthExpirationTTL    time.Duration
	headerEntryExpirationTTL  time.Duration
	contextEntryExpirationTTL time.Duration

	sync.RWMutex
}

// SetTokenBucketExpirationTTL is thread-safe way of setting custom token bucket expiration TTL.
func (l *Limiter) SetTokenBucketExpirationTTL(ttl time.Duration) *Limiter {
	l.Lock()
	l.tokenBucketExpirationTTL = ttl
	l.Unlock()

	return l
}

// GetTokenBucketExpirationTTL is thread-safe way of getting custom token bucket expiration TTL.
func (l *Limiter) GetTokenBucketExpirationTTL() time.Duration {
	l.RLock()
	defer l.RUnlock()
	return l.tokenBucketExpirationTTL
}

// SetBasicAuthExpirationTTL is thread-safe way of setting custom basic auth expiration TTL.
func (l *Limiter) SetBasicAuthExpirationTTL(ttl time.Duration) *Limiter {
	l.Lock()
	l.basicAuthExpirationTTL = ttl
	l.Unlock()

	return l
}

// GetBasicAuthExpirationTTL is thread-safe way of getting custom basic auth expiration TTL.
func (l *Limiter) GetBasicAuthExpirationTTL() time.Duration {
	l.RLock()
	defer l.RUnlock()
	return l.basicAuthExpirationTTL
}

// SetHeaderEntryExpirationTTL is thread-safe way of setting custom basic auth expiration TTL.
func (l *Limiter) SetHeaderEntryExpirationTTL(ttl time.Duration) *Limiter {
	l.Lock()
	l.headerEntryExpirationTTL = ttl
	l.Unlock()

	return l
}

// GetHeaderEntryExpirationTTL is thread-safe way of getting custom basic auth expiration TTL.
func (l *Limiter) GetHeaderEntryExpirationTTL() time.Duration {
	l.RLock()
	defer l.RUnlock()
	return l.headerEntryExpirationTTL
}

// SetContextValueEntryExpirationTTL is thread-safe way of setting custom Context value expiration TTL.
func (l *Limiter) SetContextValueEntryExpirationTTL(ttl time.Duration) *Limiter {
	l.Lock()
	l.contextEntryExpirationTTL = ttl
	l.Unlock()

	return l
}

// GetContextValueEntryExpirationTTL is thread-safe way of getting custom Context value expiration TTL.
func (l *Limiter) GetContextValueEntryExpirationTTL() time.Duration {
	l.RLock()
	defer l.RUnlock()
	return l.contextEntryExpirationTTL
}

// SetMax is thread-safe way of setting maximum number of requests to limit per second.
func (l *Limiter) SetMax(max float64) *Limiter {
	l.Lock()
	l.max = max
	l.Unlock()

	return l
}

// GetMax is thread-safe way of getting maximum number of requests to limit per second.
func (l *Limiter) GetMax() float64 {
	l.RLock()
	defer l.RUnlock()
	return l.max
}

// SetBurst is thread-safe way of setting maximum burst size.
func (l *Limiter) SetBurst(burst int) *Limiter {
	l.Lock()
	l.burst = burst
	l.Unlock()

	return l
}

// GetBurst is thread-safe way of setting maximum burst size.
func (l *Limiter) GetBurst() int {
	l.RLock()
	defer l.RUnlock()

	return l.burst
}

// SetGlobalMax sets the aggregate request rate shared by all keys. A value
// less than or equal to zero disables the global limiter.
func (l *Limiter) SetGlobalMax(max float64) *Limiter {
	current := l.globalLimit.Load()
	burst := defaultGlobalBurst(max)
	burstExplicit := false
	if current != nil && current.burstExplicit {
		burst = current.burst
		burstExplicit = true
	}

	config := &globalLimitConfig{
		max:           max,
		burst:         burst,
		burstExplicit: burstExplicit,
	}
	if max > 0 {
		config.bucket = newGlobalLimitBucket(max, burst)
	}
	l.globalLimit.Store(config)

	return l
}

// GetGlobalMax returns the aggregate request rate shared by all keys.
func (l *Limiter) GetGlobalMax() float64 {
	config := l.globalLimit.Load()
	if config == nil || config.max <= 0 {
		return 0
	}
	return config.max
}

// SetGlobalBurst sets how many requests the aggregate limiter can admit at
// once. A non-positive value derives the burst from the global max. It is safe
// to call before SetGlobalMax.
func (l *Limiter) SetGlobalBurst(burst int) *Limiter {
	current := l.globalLimit.Load()
	max := 0.0
	if current != nil {
		max = current.max
	}

	burstExplicit := burst > 0
	if !burstExplicit {
		burst = defaultGlobalBurst(max)
	}

	config := &globalLimitConfig{
		max:           max,
		burst:         burst,
		burstExplicit: burstExplicit,
	}
	if max > 0 {
		config.bucket = newGlobalLimitBucket(max, burst)
	}
	l.globalLimit.Store(config)

	return l
}

// GetGlobalBurst returns the aggregate limiter burst size.
func (l *Limiter) GetGlobalBurst() int {
	config := l.globalLimit.Load()
	if config == nil {
		return 0
	}
	return config.burst
}

// GlobalLimitEnabled reports whether an aggregate limit is configured.
func (l *Limiter) GlobalLimitEnabled() bool {
	config := l.globalLimit.Load()
	return config != nil && config.max > 0 && config.bucket != nil
}

// GlobalLimitReached consumes one token from the aggregate limiter and reports
// whether the global limit was reached.
func (l *Limiter) GlobalLimitReached() bool {
	reached, _ := l.GlobalLimitReachedAndTokens()
	return reached
}

// GlobalLimitReachedAndTokens consumes one token from the aggregate limiter and
// returns both whether it was reached and the approximate remaining capacity.
func (l *Limiter) GlobalLimitReachedAndTokens() (bool, int) {
	config := l.globalLimit.Load()
	if config == nil || config.max <= 0 || config.bucket == nil {
		return false, 0
	}

	allowed, remaining := config.bucket.allow(time.Now().UnixNano())
	return !allowed, remaining
}

// GlobalTokens returns the approximate number of tokens left in the aggregate
// limiter without consuming one.
func (l *Limiter) GlobalTokens() int {
	config := l.globalLimit.Load()
	if config == nil || config.max <= 0 || config.bucket == nil {
		return 0
	}
	return config.bucket.tokensAt(time.Now().UnixNano())
}

// SetGlobalLimit is an alias for SetGlobalMax.
func (l *Limiter) SetGlobalLimit(max float64) *Limiter {
	return l.SetGlobalMax(max)
}

// GetGlobalLimit is an alias for GetGlobalMax.
func (l *Limiter) GetGlobalLimit() float64 {
	return l.GetGlobalMax()
}

// SetTotalMax is an alias for SetGlobalMax.
func (l *Limiter) SetTotalMax(max float64) *Limiter {
	return l.SetGlobalMax(max)
}

// GetTotalMax is an alias for GetGlobalMax.
func (l *Limiter) GetTotalMax() float64 {
	return l.GetGlobalMax()
}

// SetAggregateMax is an alias for SetGlobalMax.
func (l *Limiter) SetAggregateMax(max float64) *Limiter {
	return l.SetGlobalMax(max)
}

// GetAggregateMax is an alias for GetGlobalMax.
func (l *Limiter) GetAggregateMax() float64 {
	return l.GetGlobalMax()
}

// SetMessage is thread-safe way of setting HTTP message when limit is reached.
func (l *Limiter) SetMessage(msg string) *Limiter {
	l.Lock()
	l.message = msg
	l.Unlock()

	return l
}

// GetMessage is thread-safe way of getting HTTP message when limit is reached.
func (l *Limiter) GetMessage() string {
	l.RLock()
	defer l.RUnlock()
	return l.message
}

// SetMessageContentType is thread-safe way of setting HTTP message Content-Type when limit is reached.
func (l *Limiter) SetMessageContentType(contentType string) *Limiter {
	l.Lock()
	l.messageContentType = contentType
	l.Unlock()

	return l
}

// GetMessageContentType is thread-safe way of getting HTTP message Content-Type when limit is reached.
func (l *Limiter) GetMessageContentType() string {
	l.RLock()
	defer l.RUnlock()
	return l.messageContentType
}

// SetStatusCode is thread-safe way of setting HTTP status code when limit is reached.
func (l *Limiter) SetStatusCode(statusCode int) *Limiter {
	l.Lock()
	l.statusCode = statusCode
	l.Unlock()

	return l
}

// GetStatusCode is thread-safe way of getting HTTP status code when limit is reached.
func (l *Limiter) GetStatusCode() int {
	l.RLock()
	defer l.RUnlock()
	return l.statusCode
}

// SetOnLimitReached is thread-safe way of setting after-rejection function when limit is reached.
func (l *Limiter) SetOnLimitReached(fn func(w http.ResponseWriter, r *http.Request)) *Limiter {
	l.Lock()
	l.onLimitReached = fn
	l.Unlock()

	return l
}

// ExecOnLimitReached is thread-safe way of executing after-rejection function when limit is reached.
func (l *Limiter) ExecOnLimitReached(w http.ResponseWriter, r *http.Request) {
	l.RLock()
	fn := l.onLimitReached
	l.RUnlock()

	if fn != nil {
		fn(w, r)
	}
}

// SetOverrideDefaultResponseWriter is a thread-safe way of setting the response writer override variable.
func (l *Limiter) SetOverrideDefaultResponseWriter(override bool) *Limiter {
	l.Lock()
	l.overrideDefaultResponseWriter = override
	l.Unlock()

	return l
}

// GetOverrideDefaultResponseWriter is a thread-safe way of getting the response writer override variable.
func (l *Limiter) GetOverrideDefaultResponseWriter() bool {
	l.RLock()
	defer l.RUnlock()
	return l.overrideDefaultResponseWriter
}

// SetIPLookup is thread-safe way of setting an explicit way to look up IP address.
// This method is intended to replace SetIPLookups (version 6 or older).
func (l *Limiter) SetIPLookup(lookup IPLookup) *Limiter {
	l.Lock()
	l.explicitIPLookup = lookup
	l.Unlock()

	return l
}

// GetIPLookup is thread-safe way of getting an explicit way to look up IP address.
// This method is intended to replace the old GetIPLookups (version 6 or older).
func (l *Limiter) GetIPLookup() IPLookup {
	l.RLock()
	defer l.RUnlock()
	return l.explicitIPLookup
}

// SetIgnoreURL is thread-safe way of setting whenever ignore the URL on rate limit keys
func (l *Limiter) SetIgnoreURL(enabled bool) *Limiter {
	l.Lock()
	l.ignoreURL = enabled
	l.Unlock()

	return l
}

// GetIgnoreURL returns whether the URL is ignored in the rate limit key set
func (l *Limiter) GetIgnoreURL() bool {
	l.RLock()
	defer l.RUnlock()
	return l.ignoreURL
}

// SetForwardedForIndexFromBehind is thread-safe way of setting which X-Forwarded-For index to choose.
func (l *Limiter) SetForwardedForIndexFromBehind(forwardedForIndex int) *Limiter {
	l.Lock()
	l.forwardedForIndex = forwardedForIndex
	l.Unlock()

	return l
}

// GetForwardedForIndexFromBehind is thread-safe way of getting which X-Forwarded-For index to choose.
func (l *Limiter) GetForwardedForIndexFromBehind() int {
	l.RLock()
	defer l.RUnlock()
	return l.forwardedForIndex
}

// SetMethods is thread-safe way of setting list of HTTP Methods to limit (GET, POST, PUT, etc.).
func (l *Limiter) SetMethods(methods []string) *Limiter {
	l.Lock()
	l.methods = methods
	l.Unlock()

	return l
}

// GetMethods is thread-safe way of getting list of HTTP Methods to limit (GET, POST, PUT, etc.).
func (l *Limiter) GetMethods() []string {
	l.RLock()
	defer l.RUnlock()
	return l.methods
}

// SetBasicAuthUsers is thread-safe way of setting list of basic auth usernames to limit.
func (l *Limiter) SetBasicAuthUsers(basicAuthUsers []string) *Limiter {
	ttl := l.GetBasicAuthExpirationTTL()
	if ttl <= 0 {
		ttl = l.generalExpirableOptions.DefaultExpirationTTL
	}

	for _, basicAuthUser := range basicAuthUsers {
		l.basicAuthUsers.Set(basicAuthUser, true, ttl)
	}

	return l
}

// GetBasicAuthUsers is thread-safe way of getting list of basic auth usernames to limit.
func (l *Limiter) GetBasicAuthUsers() []string {
	return l.basicAuthUsers.Keys()
}

// RemoveBasicAuthUsers is thread-safe way of removing basic auth usernames from existing list.
func (l *Limiter) RemoveBasicAuthUsers(basicAuthUsers []string) *Limiter {
	for _, toBeRemoved := range basicAuthUsers {
		l.basicAuthUsers.Invalidate(toBeRemoved)
	}

	return l
}

// DeleteExpiredTokenBuckets is thread-safe way of deleting expired token buckets
func (l *Limiter) DeleteExpiredTokenBuckets() {
	l.tokenBuckets.DeleteExpired()
}

// SetHeaders is thread-safe way of setting map of HTTP headers to limit.
func (l *Limiter) SetHeaders(headers map[string][]string) *Limiter {
	if l.headers == nil {
		l.headers = make(map[string]cache.Cache[string, bool])
	}

	for header, entries := range headers {
		l.SetHeader(header, entries)
	}

	return l
}

// GetHeaders is thread-safe way of getting map of HTTP headers to limit.
func (l *Limiter) GetHeaders() map[string][]string {
	results := make(map[string][]string)

	l.RLock()
	defer l.RUnlock()

	for header, entriesAsGoCache := range l.headers {
		results[header] = entriesAsGoCache.Keys()
	}

	return results
}

// SetHeader is thread-safe way of setting entries of 1 HTTP header.
func (l *Limiter) SetHeader(header string, entries []string) *Limiter {
	l.RLock()
	existing, found := l.headers[header]
	l.RUnlock()

	ttl := l.GetHeaderEntryExpirationTTL()
	if ttl <= 0 {
		ttl = l.generalExpirableOptions.DefaultExpirationTTL
	}

	if !found {
		existing = cache.NewCache[string, bool]().WithTTL(ttl)
	}

	for _, entry := range entries {
		existing.Set(entry, true, ttl)
	}

	l.Lock()
	l.headers[header] = existing
	l.Unlock()

	return l
}

// GetHeader is thread-safe way of getting entries of 1 HTTP header.
func (l *Limiter) GetHeader(header string) []string {
	l.RLock()
	entriesAsGoCache := l.headers[header]
	l.RUnlock()

	return entriesAsGoCache.Keys()
}

// RemoveHeader is thread-safe way of removing entries of 1 HTTP header.
func (l *Limiter) RemoveHeader(header string) *Limiter {
	ttl := l.GetHeaderEntryExpirationTTL()
	if ttl <= 0 {
		ttl = l.generalExpirableOptions.DefaultExpirationTTL
	}

	l.Lock()
	l.headers[header] = cache.NewCache[string, bool]().WithTTL(ttl)
	l.Unlock()

	return l
}

// RemoveHeaderEntries is thread-safe way of removing new entries to 1 HTTP header rule.
func (l *Limiter) RemoveHeaderEntries(header string, entriesForRemoval []string) *Limiter {
	l.RLock()
	entries, found := l.headers[header]
	l.RUnlock()

	if !found {
		return l
	}

	for _, toBeRemoved := range entriesForRemoval {
		entries.Invalidate(toBeRemoved)
	}

	return l
}

// SetContextValues is thread-safe way of setting map of HTTP headers to limit.
func (l *Limiter) SetContextValues(contextValues map[string][]string) *Limiter {
	if l.contextValues == nil {
		l.contextValues = make(map[string]cache.Cache[string, bool])
	}

	for contextValue, entries := range contextValues {
		l.SetContextValue(contextValue, entries)
	}

	return l
}

// GetContextValues is thread-safe way of getting a map of Context values to limit.
func (l *Limiter) GetContextValues() map[string][]string {
	results := make(map[string][]string)

	l.RLock()
	defer l.RUnlock()

	for contextValue, entriesAsGoCache := range l.contextValues {
		results[contextValue] = entriesAsGoCache.Keys()
	}

	return results
}

// SetContextValue is thread-safe way of setting entries of 1 Context value.
func (l *Limiter) SetContextValue(contextValue string, entries []string) *Limiter {
	l.RLock()
	existing, found := l.contextValues[contextValue]
	l.RUnlock()

	ttl := l.GetContextValueEntryExpirationTTL()
	if ttl <= 0 {
		ttl = l.generalExpirableOptions.DefaultExpirationTTL
	}

	if !found {
		existing = cache.NewCache[string, bool]().WithTTL(ttl)
	}

	for _, entry := range entries {
		existing.Set(entry, true, ttl)
	}

	l.Lock()
	l.contextValues[contextValue] = existing
	l.Unlock()

	return l
}

// GetContextValue is thread-safe way of getting 1 Context value entry.
func (l *Limiter) GetContextValue(contextValue string) []string {
	l.RLock()
	entriesAsGoCache := l.contextValues[contextValue]
	l.RUnlock()

	return entriesAsGoCache.Keys()
}

// RemoveContextValue is thread-safe way of removing entries of 1 Context value.
func (l *Limiter) RemoveContextValue(contextValue string) *Limiter {
	ttl := l.GetContextValueEntryExpirationTTL()
	if ttl <= 0 {
		ttl = l.generalExpirableOptions.DefaultExpirationTTL
	}

	l.Lock()
	l.contextValues[contextValue] = cache.NewCache[string, bool]().WithTTL(ttl)
	l.Unlock()

	return l
}

// RemoveContextValuesEntries is thread-safe way of removing entries to a ContextValue.
func (l *Limiter) RemoveContextValuesEntries(contextValue string, entriesForRemoval []string) *Limiter {
	l.RLock()
	entries, found := l.contextValues[contextValue]
	l.RUnlock()

	if !found {
		return l
	}

	for _, toBeRemoved := range entriesForRemoval {
		entries.Invalidate(toBeRemoved)
	}

	return l
}

func (l *Limiter) allowWithTokenBucketTTL(key string, tokenBucketTTL time.Duration) (bool, int) {
	l.RLock()
	lmtMax := l.max
	lmtBurst := l.burst
	l.RUnlock()

	tokenBucket, found := l.tokenBuckets.Get(key)
	if !found {
		tokenBucket = rate.NewLimiter(rate.Limit(lmtMax), lmtBurst)
		l.tokenBuckets.Set(key, tokenBucket, tokenBucketTTL)
		// Another request may have initialized the same key first. Always use
		// the bucket stored in the cache so concurrent misses share one bucket.
		if stored, ok := l.tokenBuckets.Get(key); ok {
			tokenBucket = stored
		}
	}

	now := time.Now()
	allowed := tokenBucket.AllowN(now, 1)
	tokens := int(tokenBucket.TokensAt(now))
	return !allowed, tokens
}

func (l *Limiter) tokenBucketTTL() time.Duration {
	ttl := l.GetTokenBucketExpirationTTL()

	if ttl <= 0 {
		ttl = l.generalExpirableOptions.DefaultExpirationTTL
	}

	return ttl
}

// LimitReached reports whether either the per-key or aggregate limit is exhausted.
func (l *Limiter) LimitReached(key string) bool {
	limitReached, _, _ := l.LimitReachedWithReasonAndTokens(key)
	return limitReached
}

// KeyLimitReachedAndTokens reports whether the per-key bucket is exhausted and
// returns the remaining tokens from the same calculation. It does not consume
// aggregate capacity.
func (l *Limiter) KeyLimitReachedAndTokens(key string) (bool, int) {
	return l.allowWithTokenBucketTTL(key, l.tokenBucketTTL())
}

// LimitReachedAndTokens reports whether either configured limit is exhausted
// and returns the remaining per-key tokens from the same calculation.
func (l *Limiter) LimitReachedAndTokens(key string) (bool, int) {
	limitReached, tokens, _ := l.LimitReachedWithReasonAndTokens(key)
	return limitReached, tokens
}

// AllowAndTokens is an alias for LimitReachedAndTokens.
func (l *Limiter) AllowAndTokens(key string) (bool, int) {
	return l.LimitReachedAndTokens(key)
}

// LimitReachedWithReason reports whether either configured limit is exhausted
// and identifies the rejecting layer.
func (l *Limiter) LimitReachedWithReason(key string) (bool, tbErrors.LimitReason) {
	limitReached, _, reason := l.LimitReachedWithReasonAndTokens(key)
	return limitReached, reason
}

// LimitReachedWithReasonAndTokens checks the per-key limit first and then the
// aggregate limit. It returns the reason for a rejection, or an empty reason
// when the request is allowed.
func (l *Limiter) LimitReachedWithReasonAndTokens(key string) (bool, int, tbErrors.LimitReason) {
	limitReached, tokens := l.KeyLimitReachedAndTokens(key)
	if limitReached {
		return true, 0, tbErrors.ReasonKey
	}

	if l.GlobalLimitReached() {
		return true, 0, tbErrors.ReasonGlobal
	}

	return false, tokens, ""
}

// Tokens returns current amount of tokens left in the Bucket identified by key.
func (l *Limiter) Tokens(key string) int {
	expiringMap, found := l.tokenBuckets.Get(key)
	if !found {
		return 0
	}

	return int(expiringMap.TokensAt(time.Now()))
}

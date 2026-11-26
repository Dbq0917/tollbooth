package limiter

import (
	"strings"
	"sync"
	"sync/atomic"
)

// RequestStats contains cumulative decision counts for one key or for all keys.
type RequestStats struct {
	// Allowed is the number of decisions that admitted the request.
	Allowed int64

	// Rejected is the number of decisions that reached the limit.
	Rejected int64
}

// Stats is an alias for RequestStats.
type Stats = RequestStats

// Total returns the total number of recorded decisions.
func (s RequestStats) Total() int64 {
	return s.Allowed + s.Rejected
}

// Requests is an alias for Total.
func (s RequestStats) Requests() int64 {
	return s.Total()
}

// RejectionRate returns the fraction of decisions that were rejected. The zero
// value is returned when there are no recorded decisions.
func (s RequestStats) RejectionRate() float64 {
	total := s.Total()
	if total == 0 {
		return 0
	}

	return float64(s.Rejected) / float64(total)
}

// AllowRate returns the fraction of decisions that were allowed. The zero
// value is returned when there are no recorded decisions.
func (s RequestStats) AllowRate() float64 {
	total := s.Total()
	if total == 0 {
		return 0
	}

	return float64(s.Allowed) / float64(total)
}

type keyRequestStats struct {
	allowed  atomic.Int64
	rejected atomic.Int64
}

type statsRegistry struct {
	allowed  atomic.Int64
	rejected atomic.Int64
	keys     sync.Map // map[string]*keyRequestStats

	hottest           atomic.Pointer[string]
	currentHottestKey string
	hottestCount      int64 // guarded by Limiter.Lock during record
}

func (r *statsRegistry) record(key string, rejected bool) {
	value, found := r.keys.Load(key)
	if !found {
		value, _ = r.keys.LoadOrStore(key, &keyRequestStats{})
	}
	keyCounters := value.(*keyRequestStats)

	if rejected {
		r.rejected.Add(1)
		keyCounters.rejected.Add(1)
	} else {
		r.allowed.Add(1)
		keyCounters.allowed.Add(1)
	}

	total := keyCounters.allowed.Load() + keyCounters.rejected.Load()
	if total > r.hottestCount {
		r.hottestCount = total
		if r.currentHottestKey != key {
			r.currentHottestKey = key
			candidate := key
			r.hottest.Store(&candidate)
		}
	}
}

func (r *statsRegistry) totalStats() RequestStats {
	return RequestStats{
		Allowed:  r.allowed.Load(),
		Rejected: r.rejected.Load(),
	}
}

func (r *statsRegistry) keyStats(key string) RequestStats {
	value, found := r.keys.Load(key)
	if !found {
		return RequestStats{}
	}

	counters := value.(*keyRequestStats)
	return RequestStats{
		Allowed:  counters.allowed.Load(),
		Rejected: counters.rejected.Load(),
	}
}

func (r *statsRegistry) hottestKey() string {
	key := r.hottest.Load()
	if key == nil {
		return ""
	}

	return *key
}

func statsKey(keys []string) string {
	return strings.Join(keys, "|")
}

// Stats returns cumulative allow/reject statistics.
//
// With no key argument, it returns O(1) totals for all keys. With one or more
// key parts, it returns the statistics for that exact limiter key. Key parts
// are joined with "|", matching the key format used by tollbooth.LimitByKeys.
//
// Reads use atomic counters and do not take the Limiter write lock or iterate
// token buckets. Counts are retained after token buckets expire.
func (l *Limiter) Stats(keys ...string) RequestStats {
	if len(keys) == 0 {
		return l.stats.totalStats()
	}

	return l.stats.keyStats(statsKey(keys))
}

// RequestStats returns cumulative statistics. It is an alias for Stats.
func (l *Limiter) RequestStats(keys ...string) RequestStats {
	return l.Stats(keys...)
}

// GetStats is an alias for Stats.
func (l *Limiter) GetStats(keys ...string) RequestStats {
	return l.Stats(keys...)
}

// GetRequestStats is an alias for Stats.
func (l *Limiter) GetRequestStats(keys ...string) RequestStats {
	return l.Stats(keys...)
}

// HottestKey returns the key with the largest cumulative number of decisions.
// It is maintained while decisions are recorded and can be read in O(1).
func (l *Limiter) HottestKey() string {
	return l.stats.hottestKey()
}

// TopKey is an alias for HottestKey.
func (l *Limiter) TopKey() string {
	return l.HottestKey()
}

// Counters is an alias for Stats.
func (l *Limiter) Counters(keys ...string) RequestStats {
	return l.Stats(keys...)
}

// GetCounters is an alias for Stats.
func (l *Limiter) GetCounters(keys ...string) RequestStats {
	return l.Stats(keys...)
}

// Counts returns allowed and rejected decision counts. With no key argument,
// the result is the total across all keys.
func (l *Limiter) Counts(keys ...string) (allowed int64, rejected int64) {
	stats := l.Stats(keys...)
	return stats.Allowed, stats.Rejected
}

// GetCounts is an alias for Counts.
func (l *Limiter) GetCounts(keys ...string) (allowed int64, rejected int64) {
	return l.Counts(keys...)
}

// RequestCounts is an alias for Counts.
func (l *Limiter) RequestCounts(keys ...string) (allowed int64, rejected int64) {
	return l.Counts(keys...)
}

// GetRequestCounts is an alias for Counts.
func (l *Limiter) GetRequestCounts(keys ...string) (allowed int64, rejected int64) {
	return l.Counts(keys...)
}

// AllowedRequests returns the cumulative number of allowed decisions. With no
// key argument, the result is the total across all keys.
func (l *Limiter) AllowedRequests(keys ...string) int64 {
	return l.Stats(keys...).Allowed
}

// GetAllowedRequests is an alias for AllowedRequests.
func (l *Limiter) GetAllowedRequests(keys ...string) int64 {
	return l.AllowedRequests(keys...)
}

// AllowedCount is an alias for AllowedRequests.
func (l *Limiter) AllowedCount(keys ...string) int64 {
	return l.AllowedRequests(keys...)
}

// GetAllowedCount is an alias for AllowedRequests.
func (l *Limiter) GetAllowedCount(keys ...string) int64 {
	return l.AllowedRequests(keys...)
}

// Allowed is an alias for AllowedRequests.
func (l *Limiter) Allowed(keys ...string) int64 {
	return l.AllowedRequests(keys...)
}

// RejectedRequests returns the cumulative number of rejected decisions. With
// no key argument, the result is the total across all keys.
func (l *Limiter) RejectedRequests(keys ...string) int64 {
	return l.Stats(keys...).Rejected
}

// GetRejectedRequests is an alias for RejectedRequests.
func (l *Limiter) GetRejectedRequests(keys ...string) int64 {
	return l.RejectedRequests(keys...)
}

// RejectedCount is an alias for RejectedRequests.
func (l *Limiter) RejectedCount(keys ...string) int64 {
	return l.RejectedRequests(keys...)
}

// GetRejectedCount is an alias for RejectedRequests.
func (l *Limiter) GetRejectedCount(keys ...string) int64 {
	return l.RejectedRequests(keys...)
}

// Rejected is an alias for RejectedRequests.
func (l *Limiter) Rejected(keys ...string) int64 {
	return l.RejectedRequests(keys...)
}

// TotalRequests returns the cumulative number of decisions. With no key
// argument, the result is the total across all keys.
func (l *Limiter) TotalRequests(keys ...string) int64 {
	return l.Stats(keys...).Total()
}

// GetTotalRequests is an alias for TotalRequests.
func (l *Limiter) GetTotalRequests(keys ...string) int64 {
	return l.TotalRequests(keys...)
}

// RequestCount is an alias for TotalRequests.
func (l *Limiter) RequestCount(keys ...string) int64 {
	return l.TotalRequests(keys...)
}

// GetRequestCount is an alias for TotalRequests.
func (l *Limiter) GetRequestCount(keys ...string) int64 {
	return l.TotalRequests(keys...)
}

// RejectionRate returns the cumulative rejection rate. With no key argument,
// the result is the rate across all keys.
func (l *Limiter) RejectionRate(keys ...string) float64 {
	return l.Stats(keys...).RejectionRate()
}

// GetRejectionRate is an alias for RejectionRate.
func (l *Limiter) GetRejectionRate(keys ...string) float64 {
	return l.RejectionRate(keys...)
}

// RemainingTokens is an alias for Tokens.
func (l *Limiter) RemainingTokens(key string) int {
	return l.Tokens(key)
}

// GetTokens is an alias for Tokens.
func (l *Limiter) GetTokens(key string) int {
	return l.Tokens(key)
}

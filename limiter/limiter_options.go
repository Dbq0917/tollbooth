package limiter

import (
	"time"
)

// ExpirableOptions are options used for new limiter creation
type ExpirableOptions struct {
	DefaultExpirationTTL time.Duration

	// How frequently expire job triggers
	// Deprecated: not used anymore
	ExpireJobInterval time.Duration

	// GlobalMax is the aggregate request rate shared by all keys. Zero disables
	// the global limiter.
	GlobalMax float64

	// GlobalBurst is the maximum number of requests the aggregate limiter can
	// admit at once. Zero derives it from GlobalMax.
	GlobalBurst int
}

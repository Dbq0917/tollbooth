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

	// InitialTokens is the number of tokens available when a token bucket is
	// created. It is clamped to the configured burst size. Zero keeps the
	// default behavior of starting with a full burst.
	InitialTokens int

	// InitialQuota is an alias for InitialTokens.
	InitialQuota int

	// WarmupTokens is an alias for InitialTokens.
	WarmupTokens int

	// InitialBurst is an alias for InitialTokens.
	InitialBurst int

	// WarmupBurst is an alias for InitialTokens.
	WarmupBurst int

	// PrefillTokens is an alias for InitialTokens.
	PrefillTokens int

	// RefillInterval controls how often a depleted bucket is replenished.
	// Zero uses continuous replenishment at the configured maximum rate.
	RefillInterval time.Duration

	// RecoveryInterval is an alias for RefillInterval.
	RecoveryInterval time.Duration

	// RefillEvery is an alias for RefillInterval.
	RefillEvery time.Duration

	// RefillTokens is the number of tokens added per RefillInterval. Zero
	// derives the amount from the maximum rate and the interval.
	RefillTokens int

	// RecoveryTokens is an alias for RefillTokens.
	RecoveryTokens int

	// RefillAmount is an alias for RefillTokens.
	RefillAmount int

	// RefillRate is the number of tokens restored per second. Zero uses the
	// configured maximum rate.
	RefillRate float64

	// RecoveryRate is an alias for RefillRate.
	RecoveryRate float64

	// WarmupStrategy controls the tokens available when a bucket is created.
	WarmupStrategy WarmupStrategy

	// RecoveryStrategy controls how a depleted bucket is replenished.
	RecoveryStrategy RecoveryStrategy
}

// LimiterOptions is an alias for ExpirableOptions.
type LimiterOptions = ExpirableOptions

// WarmupStrategy describes the one-time token allocation for a new bucket.
type WarmupStrategy struct {
	Tokens    int
	Initial   int
	Burst     int
	Prefilled int
}

// RecoveryStrategy describes how tokens are replenished after depletion.
type RecoveryStrategy struct {
	Interval time.Duration
	Every    time.Duration
	Tokens   int
	Amount   int
	Rate     float64
}

// RecoveryPolicy is an alias for RecoveryStrategy.
type RecoveryPolicy = RecoveryStrategy

// RefillPolicy is an alias for RecoveryStrategy.
type RefillPolicy = RecoveryStrategy

// WarmupPolicy is an alias for WarmupStrategy.
type WarmupPolicy = WarmupStrategy

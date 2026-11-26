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

	// PathTemplates normalizes dynamic path segments before building a key.
	PathTemplates []string

	// PathTemplate configures a single path template.
	PathTemplate string

	// PathPatterns is an alternative name for PathTemplates.
	PathPatterns []string

	// PathPattern is an alternative name for PathTemplate.
	PathPattern string
}

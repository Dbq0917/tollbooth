package tollbooth

import "github.com/didip/tollbooth/v8/limiter"

// PathTemplate re-exports the path template string type.
type PathTemplate = limiter.PathTemplate

// PathTemplateMatcher re-exports the segment-based template matcher.
type PathTemplateMatcher = limiter.PathTemplateMatcher

// NewPathTemplateMatcher creates a segment-based path template matcher.
func NewPathTemplateMatcher(templates ...string) *PathTemplateMatcher {
	return limiter.NewPathTemplateMatcher(templates...)
}

// NormalizePath forwards to limiter.NormalizePath.
func NormalizePath(args ...any) string {
	return limiter.NormalizePath(args...)
}

// NormalizeURLPath is an alias for NormalizePath.
func NormalizeURLPath(args ...any) string {
	return limiter.NormalizePath(args...)
}

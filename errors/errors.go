// Package errors provide data structure for errors.
package errors

import "fmt"

// LimitReason identifies the layer that rejected a request.
type LimitReason string

const (
	// ReasonKey means the request was rejected by a per-key limiter.
	ReasonKey LimitReason = "key"
	// ReasonGlobal means the request was rejected by the aggregate limiter.
	ReasonGlobal LimitReason = "global"
)

const (
	// LimitReasonKey is an alias for ReasonKey.
	LimitReasonKey = ReasonKey
	// LimitReasonGlobal is an alias for ReasonGlobal.
	LimitReasonGlobal = ReasonGlobal
)

// HTTPError is an error struct that returns both message and status code.
type HTTPError struct {
	Message    string
	StatusCode int
	// Reason identifies whether the per-key or global limiter rejected the request.
	// It is empty when no limiter rejected the request.
	Reason LimitReason
}

// Error returns error message.
func (httperror *HTTPError) Error() string {
	if httperror.Reason != "" {
		return fmt.Sprintf("%v: %v (%s)", httperror.StatusCode, httperror.Message, httperror.Reason)
	}
	return fmt.Sprintf("%v: %v", httperror.StatusCode, httperror.Message)
}

// GetReason returns the layer that rejected the request.
func (httperror *HTTPError) GetReason() LimitReason {
	return httperror.Reason
}

// IsGlobalLimit reports whether the aggregate limiter rejected the request.
func (httperror *HTTPError) IsGlobalLimit() bool {
	return httperror.Reason == ReasonGlobal
}

// IsKeyLimit reports whether a per-key limiter rejected the request.
func (httperror *HTTPError) IsKeyLimit() bool {
	return httperror.Reason == ReasonKey
}

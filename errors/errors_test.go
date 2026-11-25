package errors

import (
	"strings"
	"testing"
)

func TestError(t *testing.T) {
	errs := HTTPError{Message: "blah", StatusCode: 429, Reason: ReasonKey}
	if errs.Error() == "" {
		t.Errorf("Unable to print Error(). Value: %v", errs.Error())
	}
	if !strings.Contains(errs.Error(), string(ReasonKey)) {
		t.Errorf("Error() does not identify the rejecting layer: %v", errs.Error())
	}
	if errs.GetReason() != ReasonKey {
		t.Errorf("GetReason() returned %q, want %q", errs.GetReason(), ReasonKey)
	}
	if !errs.IsKeyLimit() || errs.IsGlobalLimit() {
		t.Error("HTTPError did not report the per-key rejection layer")
	}
}

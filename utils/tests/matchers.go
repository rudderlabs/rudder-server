package testutils

import (
	"fmt"

	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type beValidUUIDMatcher struct{}

func (*beValidUUIDMatcher) Match(actual interface{}) (success bool, err error) {
	s, ok := actual.(string)

	if !ok {
		return false, fmt.Errorf("Expected a string.  Got:\n%s", format.Object(actual, 1))
	}

	return misc.IsValidUUID(s), nil
}

func (*beValidUUIDMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "to be a valid uuid")
}

func (*beValidUUIDMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "not to be a valid uuid")
}

/*
BeValidUUID returns a gomega matcher that checks validity of a UUID string
*/
func BeValidUUID() types.GomegaMatcher {
	return &beValidUUIDMatcher{}
}

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReasonsAreRegistered guards the registry against a reason declared with a bare &statReason{} literal, which
// would be invisible to Reasons() and therefore to every consumer checking it handles the whole set.
func TestReasonsAreRegistered(t *testing.T) {
	require.NotEmpty(t, Reasons())
	require.Contains(t, Reasons(), ReasonRateLimit, "ReasonRateLimit should be registered")
	require.Contains(t, Reasons(), ReasonSourceTransformerResponseError, "the last declared reason should be registered")
}

// TestReasonValuesAreUnique guards the one failure this package cannot otherwise notice: two reasons that mean
// different things but report the same string merge into a single Prometheus series, and every query over them is
// quietly wrong from then on.
func TestReasonValuesAreUnique(t *testing.T) {
	byValue := make(map[string]StatReason, len(Reasons()))
	for _, r := range Reasons() {
		require.NotEmpty(t, r.Value(), "a reason with an empty value would report as an absent label")
		_, duplicate := byValue[r.Value()]
		require.False(t, duplicate, "two distinct reasons both report %q", r.Value())
		byValue[r.Value()] = r
	}
}

// TestReasonsSurviveCopy checks that Reasons() hands out a copy: a consumer that sorts or truncates the slice must not
// be able to disturb the registry every other consumer reads.
func TestReasonsSurviveCopy(t *testing.T) {
	before := len(Reasons())
	got := Reasons()
	clear(got)
	require.Len(t, Reasons(), before)
	require.NotNil(t, Reasons()[0])
}

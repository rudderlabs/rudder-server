package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeMaxComputeDateTime(t *testing.T) {
	for input, want := range map[string]string{
		"2025-03-04T05:06:07.123456Z":          "2025-03-04 05:06:07.123456",
		"2025-03-04 06:06:07.123456 +0100 CET": "2025-03-04 05:06:07.123456",
		"2025-03-04 06:06:07.123456 +0100":     "2025-03-04 05:06:07.123456",
		"2025-03-04 05:06:07.123456":           "2025-03-04 05:06:07.123456",
		"2025-03-04 05:06:07":                  "2025-03-04 05:06:07",
	} {
		got, ok := NormalizeMaxComputeDateTime(input)
		require.True(t, ok, input)
		require.Equal(t, want, got)
		again, ok := NormalizeMaxComputeDateTime(got)
		require.True(t, ok)
		require.Equal(t, got, again)
	}
	for _, invalid := range []any{"not a date", 42, nil} {
		_, ok := NormalizeMaxComputeDateTime(invalid)
		require.False(t, ok)
	}
}

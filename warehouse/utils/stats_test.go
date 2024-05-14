package warehouseutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableNameForStats(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Should return the input string 'tracks'", "tracks", "tracks"},
		{"Should return the input string 'identifies'", "identifies", "identifies"},
		{"Should return the input string 'users'", "users", "users"},
		{"Should return the input string 'pages'", "pages", "pages"},
		{"Should return the input string 'screens'", "screens", "screens"},
		{"Should return the input string 'aliases'", "aliases", "aliases"},
		{"Should return the input string 'groups'", "groups", "groups"},
		{"Should return 'others' for unsupported table names", "random", "others"},
		{"Should return 'others' for an empty input string", "", "others"},
		{"Should handle case sensitivity", "Tracks", "tracks"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, TableNameForStats(tc.input))
		})
	}
}

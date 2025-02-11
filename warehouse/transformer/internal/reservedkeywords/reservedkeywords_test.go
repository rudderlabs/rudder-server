package reservedkeywords_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/reservedkeywords"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestReservedKeywords(t *testing.T) {
	t.Run("IsTableOrColumn", func(t *testing.T) {
		testCases := []struct {
			keyword    string
			isReserved bool
		}{
			{"SELECT", true},
			{"select", true},
			{"Select", true},
			{"not_reserved", false},
		}
		for _, tc := range testCases {
			require.Equal(t, tc.isReserved, reservedkeywords.IsTableOrColumn(whutils.POSTGRES, tc.keyword))
		}
	})
	t.Run("IsNamespace", func(t *testing.T) {
		testCases := []struct {
			keyword    string
			isReserved bool
		}{
			{"SELECT", true},
			{"select", true},
			{"Select", true},
			{"not_reserved", false},
		}
		for _, tc := range testCases {
			require.Equal(t, tc.isReserved, reservedkeywords.IsNamespace(whutils.POSTGRES, tc.keyword))
		}
	})
}

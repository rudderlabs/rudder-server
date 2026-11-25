package reservedkeywords_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/reservedkeywords"
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
		for _, destination := range []string{whutils.POSTGRES, whutils.MicrosoftFabric, whutils.MAXCOMPUTE} {
			for _, tc := range testCases {
				require.Equal(t, tc.isReserved, reservedkeywords.IsTableOrColumn(destination, tc.keyword))
			}
		}
		// pt is MaxCompute's generated partition column
		require.True(t, reservedkeywords.IsTableOrColumn(whutils.MAXCOMPUTE, "pt"))
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
		for _, destination := range []string{whutils.POSTGRES, whutils.MicrosoftFabric, whutils.MAXCOMPUTE} {
			for _, tc := range testCases {
				require.Equal(t, tc.isReserved, reservedkeywords.IsNamespace(destination, tc.keyword))
			}
		}
		require.True(t, reservedkeywords.IsNamespace(whutils.MAXCOMPUTE, "pt"))
	})
}

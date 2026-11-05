package reservedkeywords_test

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/reservedkeywords"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

//go:embed tablescolumns.json
var tableColumnKeywords []byte

func TestMaxComputeServerTransformerKeywordParity(t *testing.T) {
	var lists map[string][]string
	require.NoError(t, jsonrs.Unmarshal(tableColumnKeywords, &lists))
	transformer := make(map[string]bool, len(lists[whutils.MAXCOMPUTE]))
	for _, keyword := range lists[whutils.MAXCOMPUTE] {
		transformer[strings.ToUpper(keyword)] = true
	}
	require.Equal(t, whutils.ReservedKeywords[whutils.MAXCOMPUTE], transformer)
}

func TestMaxComputeSQLV2Keywords(t *testing.T) {
	for _, keyword := range []string{"pt", "partitioned", "lateral", "select"} {
		require.True(t, reservedkeywords.IsTableOrColumn(whutils.MAXCOMPUTE, keyword), keyword)
		require.True(t, reservedkeywords.IsNamespace(whutils.MAXCOMPUTE, keyword), keyword)
	}
	require.False(t, reservedkeywords.IsTableOrColumn(whutils.MAXCOMPUTE, "not_reserved"))
}

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
	})
}

package constraints

import (
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/utils/types"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestConstraintsManager(t *testing.T) {
	testCases := []struct {
		name            string
		destinationType string
		brEvent         *types.BatchRouterEvent
		columnName      string
		expected        *Violation
	}{
		{
			name:            "Unsupported destination type",
			destinationType: warehouseutils.RS,
			expected:        &Violation{},
		},
		{
			name:            "Violates index constraint",
			destinationType: warehouseutils.BQ,
			brEvent: &types.BatchRouterEvent{
				Metadata: types.Metadata{
					Table: "rudder_identity_merge_rules",
					Columns: model.TableSchema{
						"merge_property_1_type":  "string",
						"merge_property_1_value": "string",
					},
				},
				Data: map[string]interface{}{
					"merge_property_1_type":  "xdopqvvuprdwzekiuscckloityeitccatjcjzloyoftujlgzblytbdgnyarxtbwsbtioqlmawfnnrkemlzulxjzvbfpgkjwqdnfjfkodjmanubkpxsbyvzpkonfisutvkyeqlckesjafsdfjzjoayosxjkinwemicmusrwlcwkmzrwxysjxstqcdehetvscihxqrbieogqijsyvqwidupjfnvhvibqnqlvwujwmonuljejjcpvyedcxdediqviyevaiooeyhcztplpakprbparizdjmrnwuajzyfdejhseym",
					"merge_property_1_value": "xdequlyaotmwomivhsngqfiokpvdzvqfbelljpzhqgldgforwnsuuobsilwneviwyeidqyotgddenilpjkfwzecyagyyrgslwppjgdbetcogbtryoozefbwaghpgscdqktwkogsmvuiefmanfckhyuyezxmmwpgxdulvwqowtdoantflxmusglrlvmgdmcyugcijolssywjskrsntrtimyngeppuwlmfnltznzioijmtnyuiiqfbvoyealmaovuqsamfdsndqcotpwvxmdhuwedzsuxxmmnopdebjztinacn",
				},
			},
			columnName: "merge_property_1_value",
			expected: &Violation{
				IsViolated:         true,
				ViolatedIdentifier: "rudder-discards-",
			},
		},
		{
			name:            "Does not violates index constraint",
			destinationType: warehouseutils.BQ,
			brEvent: &types.BatchRouterEvent{
				Metadata: types.Metadata{
					Table: "rudder_identity_merge_rules",
					Columns: model.TableSchema{
						"merge_property_1_type":  "string",
						"merge_property_1_value": "string",
					},
				},
				Data: map[string]interface{}{
					"merge_property_1_type":  "uhqoxesrjrdjqrgnyorocsdccjmlsoolufqijertjqxzytnqiqwptahpokhbucbydkxtwamwbgcnphevaktfzfeovzelyzhxmsttgvqkarplokecfngtwoazrtgevraaegduykpcalgwfzgkjcarwf",
					"merge_property_1_value": "wopubjftfnqapctttpsfassyvbesjypimpmtweoxuifhzcxcigbhwpxkrijqqgbeehgepsplbcguztgdtipsobxoxnrqifrrbaiofkjgxilidrvffnymfqzixlubaipofijtmacswuzrgwwkvatscn",
				},
			},
			columnName: "merge_property_1_value",
			expected:   &Violation{},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cm := New(config.New())
			cv := cm.ViolatedConstraints(tc.destinationType, tc.brEvent, tc.columnName)
			require.Equal(t, tc.expected.IsViolated, cv.IsViolated)
			require.True(t, strings.HasPrefix(cv.ViolatedIdentifier, tc.expected.ViolatedIdentifier))
		})
	}
}

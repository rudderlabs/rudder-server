package warehouse

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestConstraintsManager(t *testing.T) {
	testCases := []struct {
		destinationType string
		brEvent         *BatchRouterEvent
		columnName      string
		expected        *constraintsViolation
	}{
		{warehouseutils.RS, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.POSTGRES, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.CLICKHOUSE, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.MSSQL, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.AzureSynapse, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.DELTALAKE, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.S3Datalake, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.GCSDatalake, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.AzureDatalake, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{warehouseutils.BQ, &BatchRouterEvent{}, "id", &constraintsViolation{}},
		{
			warehouseutils.BQ,
			&BatchRouterEvent{
				Metadata: Metadata{
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
			}, "merge_property_1_value",
			&constraintsViolation{
				isViolated:         true,
				violatedIdentifier: "rudder-discards-",
			},
		},
		{
			warehouseutils.BQ,
			&BatchRouterEvent{
				Metadata: Metadata{
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
			}, "merge_property_1_value",
			&constraintsViolation{
				isViolated:         false,
				violatedIdentifier: "",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.destinationType, func(t *testing.T) {
			t.Parallel()

			cm := newConstraintsManager(config.Default)
			cv := cm.violatedConstraints(tc.destinationType, tc.brEvent, tc.columnName)
			require.Equal(t, tc.expected.isViolated, cv.isViolated)
			require.True(t, strings.HasPrefix(cv.violatedIdentifier, tc.expected.violatedIdentifier))
		})
	}
}

package warehouse_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	. "github.com/rudderlabs/rudder-server/warehouse"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Constraint", func() {
	config.Reset()
	logger.Reset()
	Init6()

	DescribeTable("DurationBeforeNextAttempt", func(destinationType string, brEvent *BatchRouterEventT, columnName string, expected *ConstraintsViolationT) {
		cv := ViolatedConstraints(destinationType, brEvent, columnName)
		Expect(cv.IsViolated).To(Equal(expected.IsViolated))
		Expect(cv.ViolatedIdentifier).Should(HavePrefix(expected.ViolatedIdentifier))
	},
		Entry(nil, warehouseutils.RS, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.POSTGRES, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.CLICKHOUSE, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.MSSQL, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.AZURE_SYNAPSE, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.DELTALAKE, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.S3_DATALAKE, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.GCS_DATALAKE, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.AZURE_DATALAKE, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.BQ, &BatchRouterEventT{}, "id", &ConstraintsViolationT{}),
		Entry(nil, warehouseutils.BQ,
			&BatchRouterEventT{
				Metadata: MetadataT{
					Table: "rudder_identity_merge_rules",
					Columns: map[string]string{
						"merge_property_1_type":  "string",
						"merge_property_1_value": "string",
					},
				},
				Data: map[string]interface{}{
					"merge_property_1_type":  "xdopqvvuprdwzekiuscckloityeitccatjcjzloyoftujlgzblytbdgnyarxtbwsbtioqlmawfnnrkemlzulxjzvbfpgkjwqdnfjfkodjmanubkpxsbyvzpkonfisutvkyeqlckesjafsdfjzjoayosxjkinwemicmusrwlcwkmzrwxysjxstqcdehetvscihxqrbieogqijsyvqwidupjfnvhvibqnqlvwujwmonuljejjcpvyedcxdediqviyevaiooeyhcztplpakprbparizdjmrnwuajzyfdejhseym",
					"merge_property_1_value": "xdequlyaotmwomivhsngqfiokpvdzvqfbelljpzhqgldgforwnsuuobsilwneviwyeidqyotgddenilpjkfwzecyagyyrgslwppjgdbetcogbtryoozefbwaghpgscdqktwkogsmvuiefmanfckhyuyezxmmwpgxdulvwqowtdoantflxmusglrlvmgdmcyugcijolssywjskrsntrtimyngeppuwlmfnltznzioijmtnyuiiqfbvoyealmaovuqsamfdsndqcotpwvxmdhuwedzsuxxmmnopdebjztinacn",
				},
			}, "merge_property_1_value",
			&ConstraintsViolationT{
				IsViolated:         true,
				ViolatedIdentifier: "rudder-discards-",
			},
		),
		Entry(nil, warehouseutils.BQ,
			&BatchRouterEventT{
				Metadata: MetadataT{
					Table: "rudder_identity_merge_rules",
					Columns: map[string]string{
						"merge_property_1_type":  "string",
						"merge_property_1_value": "string",
					},
				},
				Data: map[string]interface{}{
					"merge_property_1_type":  "uhqoxesrjrdjqrgnyorocsdccjmlsoolufqijertjqxzytnqiqwptahpokhbucbydkxtwamwbgcnphevaktfzfeovzelyzhxmsttgvqkarplokecfngtwoazrtgevraaegduykpcalgwfzgkjcarwf",
					"merge_property_1_value": "wopubjftfnqapctttpsfassyvbesjypimpmtweoxuifhzcxcigbhwpxkrijqqgbeehgepsplbcguztgdtipsobxoxnrqifrrbaiofkjgxilidrvffnymfqzixlubaipofijtmacswuzrgwwkvatscn",
				},
			}, "merge_property_1_value",
			&ConstraintsViolationT{
				IsViolated:         false,
				ViolatedIdentifier: "",
			},
		),
	)
})

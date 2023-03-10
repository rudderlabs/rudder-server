package warehouse_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	. "github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var _ = Describe("Constraint", func() {
	config.Reset()
	logger.Reset()
	Init6()

	DescribeTable("DurationBeforeNextAttempt", func(destinationType string, brEvent *BatchRouterEvent, columnName string, expected *ConstraintsViolation) {
		cv := ViolatedConstraints(destinationType, brEvent, columnName)
		Expect(cv.IsViolated).To(Equal(expected.IsViolated))
		Expect(cv.ViolatedIdentifier).Should(HavePrefix(expected.ViolatedIdentifier))
	},
		Entry(nil, warehouseutils.RS, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.POSTGRES, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.CLICKHOUSE, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.MSSQL, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.AZURE_SYNAPSE, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.DELTALAKE, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.S3_DATALAKE, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.GCS_DATALAKE, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.AZURE_DATALAKE, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.BQ, &BatchRouterEvent{}, "id", &ConstraintsViolation{}),
		Entry(nil, warehouseutils.BQ,
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
			&ConstraintsViolation{
				IsViolated:         true,
				ViolatedIdentifier: "rudder-discards-",
			},
		),
		Entry(nil, warehouseutils.BQ,
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
			&ConstraintsViolation{
				IsViolated:         false,
				ViolatedIdentifier: "",
			},
		),
	)
})

package validations

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

var _ = Describe("Steps", func() {
	warehouseutils.Init()

	DescribeTable("Validation steps", func(destinationType string, expectedSteps []string) {
		ct := &CTHandleT{
			infoRequest: &DestinationValidationRequest{
				Destination: backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destinationType,
					},
				},
			},
		}
		vs := ct.validationSteps()
		Expect(vs).To(HaveLen(len(expectedSteps)))
		for i, step := range expectedSteps {
			Expect(vs[i]).To(HaveField("ID", i+1))
			Expect(vs[i]).To(HaveField("Name", step))
		}
	},
		Entry("S3_DATALAKE", "S3_DATALAKE", []string{
			verifyingObjectStorage,
		}),
		Entry("RS", "RS", []string{
			verifyingObjectStorage,
			verifyingConnections,
			verifyingCreateSchema,
			verifyingCreateAndAlterTable,
			verifyingFetchSchema,
			verifyingLoadTable,
		}),
	)
})

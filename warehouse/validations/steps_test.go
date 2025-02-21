package validations_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestValidationSteps(t *testing.T) {
	warehouseutils.Init()

	testCases := []struct {
		name  string
		dest  backendconfig.DestinationT
		steps []string
	}{
		{
			name: "GCS",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.GCSDatalake,
				},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: "Azure",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.AzureDatalake,
				},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: "S3 without Glue",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3Datalake,
				},
				Config: map[string]interface{}{},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: "S3 with Glue",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.S3Datalake,
				},
				Config: map[string]interface{}{
					"region":  "us-east-1",
					"useGlue": true,
				},
			},
			steps: []string{
				model.VerifyingObjectStorage,
				model.VerifyingCreateSchema,
				model.VerifyingCreateAndAlterTable,
				model.VerifyingFetchSchema,
			},
		},
		{
			name: "RS",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.RS,
				},
			},
			steps: []string{
				model.VerifyingObjectStorage,
				model.VerifyingConnections,
				model.VerifyingCreateSchema,
				model.VerifyingCreateAndAlterTable,
				model.VerifyingFetchSchema,
				model.VerifyingLoadTable,
			},
		},
		{
			name: "Snowpipe",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.SnowpipeStreaming,
				},
			},
			steps: []string{
				model.VerifyingConnections,
				model.VerifyingCreateSchema,
				model.VerifyingCreateAndAlterTable,
				model.VerifyingFetchSchema,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			steps := validations.StepsToValidate(&tc.dest)
			require.Len(t, steps.Steps, len(tc.steps))

			for i, step := range steps.Steps {
				require.Equal(t, step.ID, i+1)
				require.Equal(t, step.Name, tc.steps[i])
			}
		})
	}
}

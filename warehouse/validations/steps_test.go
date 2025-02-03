package validations_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestValidationSteps(t *testing.T) {
	whutils.Init()

	testCases := []struct {
		name  string
		dest  backendconfig.DestinationT
		steps []string
	}{
		{
			name: whutils.GCSDatalake,
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.GCSDatalake,
				},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: whutils.AzureDatalake,
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.AzureDatalake,
				},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: whutils.S3Datalake + " without Glue",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.S3Datalake,
				},
				Config: map[string]interface{}{},
			},
			steps: []string{model.VerifyingObjectStorage},
		},
		{
			name: whutils.S3Datalake + " with Glue",
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.S3Datalake,
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
			name: whutils.RS,
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.RS,
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
			name: whutils.SnowpipeStreaming,
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.SnowpipeStreaming,
				},
				Config: map[string]interface{}{},
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
		t.Run(tc.name, func(t *testing.T) {
			steps := validations.StepsToValidate(&tc.dest)
			require.Len(t, steps.Steps, len(tc.steps))
			for i, step := range steps.Steps {
				require.Equal(t, step.ID, i+1)
				require.Equal(t, step.Name, tc.steps[i])
			}
		})
	}
}

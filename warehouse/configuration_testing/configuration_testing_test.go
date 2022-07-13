package configuration_testing

import (
	"github.com/rudderlabs/rudder-server/config/backend-config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"reflect"
	"testing"
)

func TestValidationSteps(t *testing.T) {
	inputs := []struct {
		destinationName string
		steps           []string
	}{
		{
			destinationName: warehouseutils.S3_DATALAKE,
			steps:           []string{verifyingObjectStorage, verifyingCreateSchema, verifyingFetchSchema},
		},
		{
			destinationName: warehouseutils.GCS_DATALAKE,
			steps:           []string{verifyingObjectStorage},
		},
		{
			destinationName: warehouseutils.RS,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateSchema, verifyingCreateAndAlterTable, verifyingFetchSchema, verifyingLoadTable},
		},
	}
	for _, input := range inputs {
		t.Run(input.destinationName, func(t *testing.T) {
			ct := &CTHandleT{}
			steps := []string{}
			ct.infoRequest = &DestinationValidationRequest{
				Destination: backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: input.destinationName,
					},
				},
			}

			validationSteps := ct.validationSteps()
			for _, vs := range validationSteps {
				steps = append(steps, vs.Name)
			}

			if !reflect.DeepEqual(steps, input.steps) {
				t.Errorf("got %#v, want %#v", steps, input.steps)
			}
		})
	}
}

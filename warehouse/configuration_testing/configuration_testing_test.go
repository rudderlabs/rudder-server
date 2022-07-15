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
			destinationName: warehouseutils.GCS_DATALAKE,
			steps:           []string{verifyingObjectStorage},
		},
		{
			destinationName: warehouseutils.AZURE_DATALAKE,
			steps:           []string{verifyingObjectStorage},
		},
		{
			destinationName: warehouseutils.S3_DATALAKE,
			steps:           []string{verifyingObjectStorage, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable},
		},
		{
			destinationName: warehouseutils.RS,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.BQ,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.SNOWFLAKE,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.POSTGRES,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.CLICKHOUSE,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.MSSQL,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.AZURE_SYNAPSE,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
		},
		{
			destinationName: warehouseutils.DELTALAKE,
			steps:           []string{verifyingObjectStorage, verifyingConnections, verifyingCreateAndFetchSchema, verifyingCreateAndAlterTable, verifyingLoadTable},
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

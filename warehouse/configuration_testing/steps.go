package configuration_testing

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (ct *CTHandleT) validationStepsFunc(req json.RawMessage, _ string) (json.RawMessage, error) {
	ct.infoRequest = &DestinationValidationRequest{}
	if err := parseOptions(req, ct.infoRequest); err != nil {
		return nil, err
	}

	return json.Marshal(validationStepsResponse{
		Steps: ct.validationSteps(),
	})
}

// validationSteps returns series of validation steps for
// a particular destination.
func (ct *CTHandleT) validationSteps() []*validationStep {
	steps := []*validationStep{{
		ID:        1,
		Name:      verifyingObjectStorage,
		Validator: ct.verifyingObjectStorage,
	}}

	switch ct.infoRequest.Destination.DestinationDefinition.Name {
	case warehouseutils.RS, warehouseutils.BQ, warehouseutils.SNOWFLAKE, warehouseutils.POSTGRES, warehouseutils.CLICKHOUSE, warehouseutils.MSSQL, warehouseutils.AZURE_SYNAPSE, warehouseutils.DELTALAKE:
		steps = append(steps,
			&validationStep{
				ID:        2,
				Name:      verifyingConnections,
				Validator: ct.verifyingConnections,
			},
			&validationStep{
				ID:        3,
				Name:      verifyingCreateSchema,
				Validator: ct.verifyingCreateSchema,
			},
			&validationStep{
				ID:        4,
				Name:      verifyingCreateAndAlterTable,
				Validator: ct.verifyingCreateAlterTable,
			},
			&validationStep{
				ID:        5,
				Name:      verifyingFetchSchema,
				Validator: ct.verifyingFetchSchema,
			},
			&validationStep{
				ID:        6,
				Name:      verifyingLoadTable,
				Validator: ct.verifyingLoadTable,
			},
		)
	case warehouseutils.S3_DATALAKE:
		steps = append(steps,
			&validationStep{
				ID:        2,
				Name:      verifyingCreateSchema,
				Validator: ct.verifyingCreateSchema,
			},
			&validationStep{
				ID:        3,
				Name:      verifyingFetchSchema,
				Validator: ct.verifyingFetchSchema,
			},
		)
	}
	return steps
}

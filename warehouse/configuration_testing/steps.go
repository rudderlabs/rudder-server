package configuration_testing

import (
	"encoding/json"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (ct *CTHandleT) validationStepsFunc(req json.RawMessage, _ string) (json.RawMessage, error) {
	ct.infoRequest = &DestinationValidationRequest{}
	if err := ct.parseOptions(req, ct.infoRequest); err != nil {
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
		Name:      "Verifying Object Storage",
		Validator: ct.verifyingObjectStorage,
	}}

	// Time window destination contains only object storage verification
	if misc.ContainsString(warehouseutils.TimeWindowDestinations, ct.infoRequest.Destination.DestinationDefinition.Name) {
		return steps
	}

	steps = append(steps,
		&validationStep{
			ID:        2,
			Name:      "Verifying Connections",
			Validator: ct.verifyingConnections,
		},
		&validationStep{
			ID:        3,
			Name:      "Verifying Create Schema",
			Validator: ct.verifyingCreateSchema,
		},
		&validationStep{
			ID:        4,
			Name:      "Verifying Create Table",
			Validator: ct.verifyingCreateTable,
		},
		&validationStep{
			ID:        5,
			Name:      "Verifying Load Table",
			Validator: ct.verifyingLoadTable,
		},
	)

	return steps
}

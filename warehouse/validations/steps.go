package validations

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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

	// Time window destination contains only object storage verification
	if misc.Contains(warehouseutils.TimeWindowDestinations, ct.infoRequest.Destination.DestinationDefinition.Name) {
		return steps
	}

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
	return steps
}

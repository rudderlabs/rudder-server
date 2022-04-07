package configuration_testing

import (
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (ct *CTHandleT) validationStepsFunc(req json.RawMessage, role string, step string) (json.RawMessage, error) {
	ct.infoRequest = &infoRequest{}
	if err := ct.parseOptions(req, ct.infoRequest); err != nil {
		return nil, err
	}

	return json.Marshal(validationStepsResponse{
		Steps: ct.validationSteps(role),
	})
}

// validationSteps returns validation step for a Destination
func (ct *CTHandleT) validationSteps(role string) (steps []*validationStep) {
	if !config.GetBool(fmt.Sprintf(`Warehouse.%v.enabledConfigurationTesting`, role), true) {
		steps = []*validationStep{}
		return
	}
	steps = append(steps, &validationStep{
		ID:        1,
		Name:      "Verifying Object Storage",
		Validator: ct.verifyingObjectStorage,
	})

	// Time window destination contains only object storage verification
	if misc.ContainsString(warehouseutils.TimeWindowDestinations, ct.infoRequest.Destination.DestinationDefinition.Name) {
		return
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
	return
}

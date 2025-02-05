package validations

import (
	"context"
	"encoding/json"

	"github.com/samber/lo"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func validateStepFunc(_ context.Context, destination *backendconfig.DestinationT, _ string) (json.RawMessage, error) {
	return json.Marshal(StepsToValidate(destination))
}

func StepsToValidate(dest *backendconfig.DestinationT) *model.StepsResponse {
	destType := dest.DestinationDefinition.Name

	if destType == warehouseutils.SnowpipeStreaming {
		return &model.StepsResponse{
			Steps: []*model.Step{
				{ID: 1, Name: model.VerifyingConnections},
				{ID: 2, Name: model.VerifyingCreateSchema},
				{ID: 3, Name: model.VerifyingCreateAndAlterTable},
				{ID: 4, Name: model.VerifyingFetchSchema},
			},
		}
	}

	steps := []*model.Step{
		{ID: 1, Name: model.VerifyingObjectStorage},
	}

	appendSteps := func(newSteps ...string) {
		for _, step := range newSteps {
			steps = append(steps, &model.Step{ID: len(steps) + 1, Name: step})
		}
	}

	switch destType {
	case warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		// No additional steps
	case warehouseutils.S3Datalake:
		if schemarepository.UseGlue(lo.ToPtr(createDummyWarehouse(dest))) {
			appendSteps(
				model.VerifyingCreateSchema,
				model.VerifyingCreateAndAlterTable,
				model.VerifyingFetchSchema,
			)
		}
	default:
		appendSteps(
			model.VerifyingConnections,
			model.VerifyingCreateSchema,
			model.VerifyingCreateAndAlterTable,
			model.VerifyingFetchSchema,
			model.VerifyingLoadTable,
		)
	}

	return &model.StepsResponse{Steps: steps}
}

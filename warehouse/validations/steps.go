package validations

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func validateStepFunc(destination *backendconfig.DestinationT, _ string) (json.RawMessage, error) {
	return json.Marshal(StepsToValidate(destination))
}

func StepsToValidate(dest *backendconfig.DestinationT) *model.StepsResponse {
	var (
		destType = dest.DestinationDefinition.Name
		steps    []*model.Step
	)

	steps = []*model.Step{{
		ID:   len(steps) + 1,
		Name: model.VerifyingObjectStorage,
	}}

	switch destType {
	case warehouseutils.GCS_DATALAKE, warehouseutils.AZURE_DATALAKE:
		break
	case warehouseutils.S3_DATALAKE:
		if canUseGlue := schemarepository.UseGlue(&warehouseutils.Warehouse{
			Destination: *dest,
		}); !canUseGlue {
			break
		}

		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingCreateSchema,
		})
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingCreateAndAlterTable,
		})
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingFetchSchema,
		})
	default:
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingConnections,
		})
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingCreateSchema,
		})
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingCreateAndAlterTable,
		})
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingFetchSchema,
		})
		steps = append(steps, &model.Step{
			ID:   len(steps) + 1,
			Name: model.VerifyingLoadTable,
		})
	}
	return &model.StepsResponse{
		Steps: steps,
	}
}

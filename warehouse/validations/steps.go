package validations

import (
	"context"
	"encoding/json"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func validateStepFunc(_ context.Context, destination *backendconfig.DestinationT, _ string) (json.RawMessage, error) {
	return json.Marshal(StepsToValidate(destination))
}

func StepsToValidate(dest *backendconfig.DestinationT) *model.StepsResponse {
	steps := []*model.Step{{
		ID:   1,
		Name: model.VerifyingObjectStorage,
	}}

	if dest.DestinationDefinition.Name != whutils.CLICKHOUSE {
		steps = append(steps, &model.Step{
			ID:   2,
			Name: model.VerifyingNamespace,
		})
	}

	switch dest.DestinationDefinition.Name {
	case whutils.GCSDatalake, whutils.AzureDatalake:
	case whutils.S3Datalake:
		wh := createDummyWarehouse(dest)
		if canUseGlue := schemarepository.UseGlue(&wh); !canUseGlue {
			break
		}

		length := len(steps)
		steps = append(steps,
			&model.Step{
				ID:   length + 1,
				Name: model.VerifyingCreateSchema,
			},
			&model.Step{
				ID:   length + 2,
				Name: model.VerifyingCreateAndAlterTable,
			},
			&model.Step{
				ID:   length + 3,
				Name: model.VerifyingFetchSchema,
			},
		)
	default:
		length := len(steps)
		steps = append(steps,
			&model.Step{
				ID:   length + 1,
				Name: model.VerifyingConnections,
			},
			&model.Step{
				ID:   length + 2,
				Name: model.VerifyingCreateSchema,
			},
			&model.Step{
				ID:   length + 3,
				Name: model.VerifyingCreateAndAlterTable,
			},
			&model.Step{
				ID:   length + 4,
				Name: model.VerifyingFetchSchema,
			},
			&model.Step{
				ID:   length + 5,
				Name: model.VerifyingLoadTable,
			},
		)
	}
	return &model.StepsResponse{Steps: steps}
}

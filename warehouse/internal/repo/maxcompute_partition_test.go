package repo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestMaxComputePartitionPlansSurviveUploadMetadata(t *testing.T) {
	plans := map[string]model.PartitionPlan{
		"events": {Source: "received_at", Granularity: "hour"},
	}
	metadata := ExtractUploadMetadata(model.Upload{PartitionPlans: plans})
	encoded, err := jsonrs.Marshal(metadata)
	require.NoError(t, err)
	var restored UploadMetadata
	require.NoError(t, jsonrs.Unmarshal(encoded, &restored))
	require.Equal(t, plans, restored.PartitionPlans)
}

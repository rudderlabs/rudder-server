package loadfiles

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestMaxComputePartitionModesWorkerPayload(t *testing.T) {
	modes := map[string]string{"events": "fixed", "other": "dynamic"}
	job := &model.UploadJob{PartitionModes: modes, StagingFiles: []*model.StagingFile{{}}}
	req := (&LoadFileGenerator{}).prepareJobRequestV2(job, "id", job.StagingFiles, nil)
	raw, err := jsonrs.Marshal(req)
	require.NoError(t, err)
	var decoded struct {
		PartitionModes map[string]string `json:"partition_modes"`
	}
	require.NoError(t, jsonrs.Unmarshal(raw, &decoded))
	require.Equal(t, modes, decoded.PartitionModes)
}

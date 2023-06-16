package model_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestStagingFile_Builder(t *testing.T) {
	now := time.Now()

	stagingFile := model.StagingFile{
		ID:                    1,
		WorkspaceID:           "workspace_id",
		Location:              "s3://path",
		SourceID:              "source_id",
		DestinationID:         "destination_id",
		Status:                "waiting",
		Error:                 nil,
		FirstEventAt:          now,
		LastEventAt:           now,
		UseRudderStorage:      false,
		DestinationRevisionID: "",
		TotalEvents:           0,
		SourceTaskRunID:       "",
		SourceJobID:           "",
		SourceJobRunID:        "",
		TimeWindow:            time.Time{},
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	wSchema := stagingFile.WithSchema([]byte("test"))

	require.Equal(t, model.StagingFileWithSchema{
		StagingFile: stagingFile,
		Schema:      []byte("test"),
	}, wSchema)
}

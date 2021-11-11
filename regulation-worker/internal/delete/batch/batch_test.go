package batch_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func TestDelete(t *testing.T) {
	config.Load()
	logger.Init()
	// backendconfig.Init()

	ctx := context.Background()
	tests := []struct {
		name           string
		job            model.Job
		dest           model.Destination
		expectedErr    error
		expectedStatus model.JobStatus
	}{
		{
			name: "testing batch deletion flow by deletion from 'mock_batch' destination",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatusPending,
				UserAttributes: []model.UserAttribute{
					{
						UserID: "12",
						Phone:  strPtr("1234567890"),
						Email:  strPtr("abc@xyz.com"),
					},
					{
						UserID: "13",
						Email:  strPtr("abcd@xyz.com"),
					},
					{
						UserID: "14",
						Phone:  strPtr("1111567890"),
					},
				},
			},
			dest: model.Destination{
				Config: map[string]interface{}{
					"bucketName":  "regulation-test-data",
					"prefix":      "latest",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
				},
				DestinationID: "1234",
				Type:          "batch",
				Name:          "S3",
			},
			expectedStatus: model.JobStatusComplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmFactory := filemanager.FileManagerFactoryT{}
			fm, _ := fmFactory.New(&filemanager.SettingsT{
				Provider: tt.dest.Name,
				Config:   tt.dest.Config,
			})
			delBatch := batch.Batch{
				FileManager:   fm,
				DeleteManager: &batch.S3DeleteManager{},
			}

			delBatch.DeleteManager.Delete(ctx, tt.job.UserAttributes, "latest_original.json.gz")
			// status, err := delBatch.Delete(ctx, tt.job, tt.dest)

			// require.Equal(t, tt.expectedErr, err, "actual error different than expected")
			// require.Equal(t, tt.expectedStatus, status, "actual job status different than expected")

		})
	}
}

func strPtr(str string) *string {
	return &(str)
}

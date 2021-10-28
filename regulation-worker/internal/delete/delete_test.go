package delete_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/stretchr/testify/require"
)

func TestDeletionFlow(t *testing.T) {

	var ctx context.Context

	tests := []struct {
		name string
		job  model.Job
		dest model.Destination
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
					"bucketName":  "malani-deletefeature-testdata",
					"prefix":      "regulation",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
				},
				DestinationID: "1234",
				Type:          "batch",
				Name:          "mock_batch",
			},
		},
		{
			name: "testing API deletion flow by calling mock_api to delete by calling transformer",
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
					"bucketName":  "malani-deletefeature-testdata",
					"prefix":      "regulation",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
				},
				DestinationID: "1234",
				Type:          "api",
				Name:          "mock_api",
			},
		},
		{
			name: "testing KVStore deletion flow by deletion from 'mock_kvstore' destination",
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
					"bucketName":  "malani-deletefeature-testdata",
					"prefix":      "regulation",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
				},
				DestinationID: "1234",
				Type:          "kv_store",
				Name:          "mock_kvstore",
			},
		},
		{
			name: "testing KVStore deletion flow by deletion from 'mock_kvstore' destination",
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
					"bucketName":  "malani-deletefeature-testdata",
					"prefix":      "regulation",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
				},
				DestinationID: "1234",
				Type:          "random",
				Name:          "mock_random",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delSvc := delete.DeleteSvc{}
			status, err := delSvc.Delete(ctx, tt.job, tt.dest)
			require.NoError(t, err, "found error")
			require.Equal(t, model.JobStatusComplete, status, "actual job status different than expected")

		})
	}
}

func strPtr(str string) *string {
	return &(str)
}

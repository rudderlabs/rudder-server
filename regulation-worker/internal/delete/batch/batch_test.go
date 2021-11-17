package batch_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
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
						UserID: "Jermaine1473336609491897794707338",
						Phone:  strPtr("6463633841"),
						Email:  strPtr("dorowane8n285680461479465450293436@gmail.com"),
					},
					{
						UserID: "Mercie8221821544021583104106123",
						Email:  strPtr("dshirilad8536019424659691213279980@gmail.com"),
					},
					{
						UserID: "Claiborn443446989226249191822329",
						Phone:  strPtr("8782905113"),
					},
				},
			},
			dest: model.Destination{
				Config: map[string]interface{}{
					"bucketName":  "regulation-test-data",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
				},
				Name: "S3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// fmFactory := filemanager.FileManagerFactoryT{}
			// fm, _ := fmFactory.New(&filemanager.SettingsT{
			// 	Provider: tt.dest.Name,
			// 	Config:   tt.dest.Config,
			// })
			// delBatch := batch.Batch{
			// 	FM: fm,
			// 	DM: &batch.S3DeleteManager{},
			// }

			err := batch.Delete(ctx, tt.job, tt.dest.Config, tt.dest.Name)
			require.NoError(t, err, "expected no error")
			// err := delBatch.Upload(ctx, "latest100_original.json.gz")
			// err := delBatch.Download(ctx, "test/latest100_original.json.gz")
			// require.NoError(t, err, "expected no error")
			/*
				files := []*filemanager.FileObject{
					{
						Key: "latest/latest_original.json.gz",
					},
					{
						Key: "latest100_original.json.gz",
					},
					{
						Key: "latest_original.json.gz",
					},
					{
						Key: "test/latest100_original.json.gz",
					},
				}
				cleanedFiles := []string{"latest100_original.json.gz"}
				batch.RemoveCleanedFiles(files, cleanedFiles)
			*/
			// batch.Decompress("latest100_original.json.gz", "decompressedFile.json")
			// out, _ := os.ReadFile("decompressedFile.json")
			// batch.Compress("latest_original.json.gz", out)

		})
	}
}

func strPtr(str string) *string {
	return &(str)
}

// func TestGetDeleteManager(t *testing.T) {
// 	t.Run("testing func", func(t *testing.T) {
// 		dm, err := batch.GetDeleteManager("S3")
// 		fmt.Println("dm=", dm)
// 		require.NoError(t, err, "expected no error")
// 	})
// }

/*
func TestS3Delete(t *testing.T) {
	ctx := context.Background()
	// test := []struct {
	// 	name           string
	// 	job            model.Job
	// 	dest           model.Destination
	// 	expectedErr    error
	// 	expectedStatus model.JobStatus
	// }
	// tests:=[]test{
	// 	userAttributes	model.UserAtt
	// 	uncompressedFileName string
	// }{
	// 	{
	// 		userAttributes: []model.UserAttribute{
	// 			{
	// 				UserID: "12",
	// 				Phone:  strPtr("1234567890"),
	// 				Email:  strPtr("dorowane8n285680461479465450293436@gmail.com"),
	// 			}
	// 		}
	// 	}
	// }

	userAttributes := []model.UserAttribute{
		{
			UserID: "Jermaine1473336609491897794707338",
			Phone:  strPtr("6463633841"),
			Email:  strPtr("dorowane8n285680461479465450293436@gmail.com"),
		},
		{
			UserID: "Mercie8221821544021583104106123",
			Email:  strPtr("dshirilad8536019424659691213279980@gmail.com"),
		},
		{
			UserID: "Claiborn443446989226249191822329",
			Phone:  strPtr("8782905113"),
		},
	}

	delBatch := batch.Batch{
		DM: &batch.S3DeleteManager{},
	}

	t.Run("testing func", func(t *testing.T) {
		_, err := delBatch.DM.Delete(ctx, userAttributes, "decompressedFile.json")
		require.NoError(t, err, "expected no error")
		// batch.Compress("cleaned_decompuressedFile.json", out)
	})
}
*/

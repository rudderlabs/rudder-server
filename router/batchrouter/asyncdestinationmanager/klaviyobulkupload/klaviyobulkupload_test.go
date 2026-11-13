package klaviyobulkupload_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockAPIService "github.com/rudderlabs/rudder-server/mocks/router/klaviyobulkupload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	klaviyobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload"
)

var currentDir, _ = os.Getwd()

func importingJob(t *testing.T, jobID int64, identifier string) *jobsdb.JobT {
	t.Helper()
	sum := sha256.Sum256([]byte(identifier))
	params, err := sjson.SetRawBytes([]byte(`{"importId":"importId1"}`), "metadata", []byte(fmt.Sprintf(`{"identifierHash":%q}`, hex.EncodeToString(sum[:]))))
	require.NoError(t, err)
	return &jobsdb.JobT{JobID: jobID, LastJobStatus: jobsdb.JobStatusT{Parameters: params}}
}

func requireImportingHashes(t *testing.T, output common.AsyncUploadOutput, identifiers map[int64]string) {
	t.Helper()
	require.Len(t, output.JobImportingParameters, len(identifiers))
	for jobID, identifier := range identifiers {
		sum := sha256.Sum256([]byte(identifier))
		require.JSONEq(t, fmt.Sprintf(`{"identifierHash":%q}`, hex.EncodeToString(sum[:])), string(output.JobImportingParameters[jobID]))
		require.NotContains(t, string(output.JobImportingParameters[jobID]), identifier)
	}
}

var destination = &backendconfig.DestinationT{
	ID:   "1",
	Name: "KLAVIYO_BULK_UPLOAD",
	DestinationDefinition: backendconfig.DestinationDefinitionT{
		Name: "KLAVIYO_BULK_UPLOAD",
	},
	Config: map[string]any{
		"privateApiKey": "1223",
	},
	Enabled:     true,
	WorkspaceID: "1",
}

func TestNewManagerSuccess(t *testing.T) {
	manager, err := klaviyobulkupload.NewManager(logger.NOP, stats.NOP, destination)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.Equal(t, "KLAVIYO_BULK_UPLOAD", destination.Name)
}

func TestUpload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	testLogger := logger.NewLogger().Child("klaviyo-bulk-upload-test")

	uploader := klaviyobulkupload.KlaviyoBulkUploader{
		DestName:              "Klaviyo Bulk Upload",
		DestinationConfig:     destination.Config,
		Logger:                testLogger,
		StatsFactory:          stats.NOP,
		KlaviyoAPIService:     mockKlaviyoAPIService,
		BatchSize:             10000,
		MaxPayloadSize:        4600000,
		MaxAllowedProfileSize: 512000,
	}

	// Create a temporary file with test data
	tempFile, err := os.CreateTemp("", "test_upload_*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	testData := []byte(`{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"qwe22@mail.com","first_name":"Testqwe0022","last_name":"user","phone_number":"+919902330123","location":{"address1":"dallas street","address2":"oppenheimer market","city":"delhi","country":"India","ip":"213.5.6.41"},"anonymous_id":"user1","jobIdentifier":"user1:1"}}]}},"relationships":{"lists":{"data":[{"type":"list","id":"list101"}]}}}}}},"metadata":{"job_id":1}}`)
	_, err = tempFile.Write(testData)
	if err != nil {
		t.Fatal(err)
	}
	tempFile.Close()

	t.Run("Successful Upload", func(t *testing.T) {
		mockKlaviyoAPIService.EXPECT().
			UploadProfiles(gomock.Any()).
			Return(&klaviyobulkupload.UploadResp{
				Data: struct {
					Id string "json:\"id\""
				}{
					Id: "importId1",
				},
				Errors: nil,
			}, nil)

		asyncDestStruct := &common.AsyncDestinationStruct{
			Destination:     destination,
			FileName:        tempFile.Name(),
			ImportingJobIDs: []int64{1},
		}

		output := uploader.Upload(context.Background(), asyncDestStruct)
		require.Equal(t, common.AsyncUploadOutput{
			ImportingJobIDs:        []int64{1},
			ImportingParameters:    output.ImportingParameters,
			FailedJobIDs:           nil,
			FailedReason:           "",
			FailedCount:            0,
			AbortJobIDs:            nil,
			AbortReason:            "",
			AbortCount:             0,
			SucceededJobIDs:        nil,
			SuccessResponse:        "",
			ImportingCount:         1,
			JobImportingParameters: output.JobImportingParameters,
			DestinationID:          destination.ID,
		}, output)
		require.Equal(t, `{"importId":"importId1","importCount":1}`, string(output.ImportingParameters))
		requireImportingHashes(t, output, map[int64]string{1: "user1"})
	})

	t.Run("Unsuccessful Upload", func(t *testing.T) {
		mockKlaviyoAPIService.EXPECT().
			UploadProfiles(gomock.Any()).
			Return(nil, fmt.Errorf("upload failed with errors: %+v", []klaviyobulkupload.ErrorDetail{
				{Detail: "upload failed"},
			}))

		asyncDestStruct := &common.AsyncDestinationStruct{
			Destination:     destination,
			FileName:        tempFile.Name(),
			ImportingJobIDs: []int64{1},
		}

		output := uploader.Upload(context.Background(), asyncDestStruct)
		require.Equal(t, common.AsyncUploadOutput{
			ImportingJobIDs:        []int64{},
			ImportingParameters:    output.ImportingParameters,
			FailedJobIDs:           []int64{1},
			FailedReason:           "upload failed with status 0: ",
			FailedCount:            1,
			AbortJobIDs:            nil,
			AbortReason:            "",
			AbortCount:             0,
			SucceededJobIDs:        nil,
			SuccessResponse:        "",
			ImportingCount:         0,
			JobImportingParameters: output.JobImportingParameters,
			DestinationID:          destination.ID,
		}, output)
		require.NotNil(t, output.JobImportingParameters)
		require.Empty(t, output.JobImportingParameters)
	})
	// 3 chunks: chunk 1 fails with 429 (retryable), chunk 2 fails with 400 (non-retryable), chunk 3 succeeds.
	// Validates that retryable and non-retryable errors are routed correctly in the same upload.
	t.Run("UploadThreeChunksWithMixedFailures", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
		uploader.BatchSize = 2
		uploader.KlaviyoAPIService = mockKlaviyoAPIService

		tempFile, err := os.CreateTemp("", "test_upload_3chunks_*.jsonl")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		const testProfileTemplate = `{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"%s@mail.com","jobIdentifier":"%s:%d"}}]}}}}}},"metadata":{"job_id":%d}}`

		profiles := []string{
			fmt.Sprintf(testProfileTemplate, "user1", "user1", 1, 1),
			fmt.Sprintf(testProfileTemplate, "user2", "user2", 2, 2),
			fmt.Sprintf(testProfileTemplate, "user3", "user3", 3, 3),
			fmt.Sprintf(testProfileTemplate, "user4", "user4", 4, 4),
			fmt.Sprintf(testProfileTemplate, "user5", "user5", 5, 5),
			fmt.Sprintf(testProfileTemplate, "user6", "user6", 6, 6),
		}

		for i, profile := range profiles {
			if i > 0 {
				_, err = tempFile.WriteString("\n")
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err = tempFile.WriteString(profile)
			if err != nil {
				t.Fatal(err)
			}
		}
		tempFile.Close()

		callCount := 0
		mockKlaviyoAPIService.EXPECT().
			UploadProfiles(gomock.Any()).
			DoAndReturn(func(payload klaviyobulkupload.Payload) (*klaviyobulkupload.UploadResp, error) {
				callCount++
				switch callCount {
				case 1:
					// Chunk 1 — 429 Too Many Requests (retryable)
					return &klaviyobulkupload.UploadResp{
						Errors: []klaviyobulkupload.ErrorDetail{
							{Detail: "rate limit exceeded"},
						},
						StatusCode: http.StatusTooManyRequests,
					}, fmt.Errorf("rate limit exceeded")
				case 2:
					// Chunk 2 — 400 Bad Request (non-retryable)
					return &klaviyobulkupload.UploadResp{
						Errors: []klaviyobulkupload.ErrorDetail{
							{Detail: "invalid payload structure"},
						},
						StatusCode: http.StatusBadRequest,
					}, fmt.Errorf("invalid payload structure")
				case 3:
					// Chunk 3 — success
					return &klaviyobulkupload.UploadResp{
						Data: struct {
							Id string "json:\"id\""
						}{Id: "importId3"},
					}, nil
				}
				return nil, fmt.Errorf("unexpected call count: %d", callCount)
			}).Times(3)

		output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
			Destination:     destination,
			FileName:        tempFile.Name(),
			ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
		})

		require.Equal(t, common.AsyncUploadOutput{
			FailedJobIDs:           []int64{1, 2},                                                                                                                       // chunk 1: 429, retryable
			FailedCount:            2,                                                                                                                                   // matches FailedJobIDs
			AbortJobIDs:            []int64{3, 4},                                                                                                                       // chunk 2: 400, non-retryable
			AbortReason:            "upload rejected by Klaviyo with status 400: {ID=, Code=, Title=, Detail=invalid payload structure, Source={Pointer=, Parameter=}}", // exact reason
			AbortCount:             2,                                                                                                                                   // matches AbortJobIDs
			ImportingJobIDs:        []int64{5, 6},                                                                                                                       // chunk 3: success
			ImportingParameters:    output.ImportingParameters,                                                                                                          // validated below
			ImportingCount:         2,                                                                                                                                   // matches ImportingJobIDs
			JobImportingParameters: output.JobImportingParameters,
			SucceededJobIDs:        nil,
			SuccessResponse:        "",
			FailedReason:           "upload failed with status 429: {ID=, Code=, Title=, Detail=rate limit exceeded, Source={Pointer=, Parameter=}}",
			DestinationID:          destination.ID,
		}, output)
		require.Equal(t, `{"importId":"importId3","importCount":2}`, string(output.ImportingParameters))
		requireImportingHashes(t, output, map[int64]string{5: "user5", 6: "user6"})
		require.NotContains(t, output.JobImportingParameters, int64(1))
		require.NotContains(t, output.JobImportingParameters, int64(2))
		require.NotContains(t, output.JobImportingParameters, int64(3))
		require.NotContains(t, output.JobImportingParameters, int64(4))
	})
}

// A chunk is rejected with a 400 that pinpoints one invalid profile (index 1).
// That job must be aborted on its own and the chunk re-uploaded with the
// remaining valid profiles, which then succeeds.
func TestUploadStripAndRetryInvalidProfiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	uploader := klaviyobulkupload.KlaviyoBulkUploader{
		DestName:              "Klaviyo Bulk Upload",
		DestinationConfig:     destination.Config,
		Logger:                logger.NOP,
		StatsFactory:          stats.NOP,
		KlaviyoAPIService:     mockKlaviyoAPIService,
		BatchSize:             10000,
		MaxPayloadSize:        4600000,
		MaxAllowedProfileSize: 512000,
	}

	tempFile, err := os.CreateTemp("", "test_upload_strip_*.jsonl")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	const tmpl = `{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"%s","jobIdentifier":"%s:%d"}}]}}}}}},"metadata":{"job_id":%d}}`
	lines := []string{
		fmt.Sprintf(tmpl, "good1@mail.com", "good1", 1, 1),
		fmt.Sprintf(tmpl, "bad@", "bad", 2, 2), // index 1 -> rejected by Klaviyo
		fmt.Sprintf(tmpl, "good2@mail.com", "good2", 3, 3),
	}
	for i, line := range lines {
		if i > 0 {
			_, err = tempFile.WriteString("\n")
			require.NoError(t, err)
		}
		_, err = tempFile.WriteString(line)
		require.NoError(t, err)
	}
	tempFile.Close()

	callCount := 0
	mockKlaviyoAPIService.EXPECT().
		UploadProfiles(gomock.Any()).
		DoAndReturn(func(payload klaviyobulkupload.Payload) (*klaviyobulkupload.UploadResp, error) {
			callCount++
			switch callCount {
			case 1:
				// All 3 profiles sent; Klaviyo rejects the one at index 1.
				require.Len(t, payload.Data.Attributes.Profiles.Data, 3)
				return &klaviyobulkupload.UploadResp{
					Errors: klaviyobulkupload.ErrorDetailList{
						{
							Code:   "invalid",
							Title:  "Invalid input.",
							Detail: "Invalid email address",
							Source: klaviyobulkupload.ErrorSource{Pointer: "/data/attributes/profiles/data/1/attributes/email"},
						},
					},
					StatusCode: http.StatusBadRequest,
				}, fmt.Errorf("upload rejected")
			case 2:
				// Retry with the bad profile stripped -> 2 profiles, succeeds.
				require.Len(t, payload.Data.Attributes.Profiles.Data, 2)
				return &klaviyobulkupload.UploadResp{
					Data: struct {
						Id string "json:\"id\""
					}{Id: "importIdRetry"},
				}, nil
			}
			return nil, fmt.Errorf("unexpected call count: %d", callCount)
		}).Times(2)

	output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination:     destination,
		FileName:        tempFile.Name(),
		ImportingJobIDs: []int64{1, 2, 3},
	})

	require.Equal(t, []int64{2}, output.AbortJobIDs)
	require.Equal(t, 1, output.AbortCount)
	require.Equal(t, "profile rejected by Klaviyo's synchronous validation: Invalid email address", output.AbortReason)
	require.ElementsMatch(t, []int64{1, 3}, output.ImportingJobIDs)
	require.Equal(t, 2, output.ImportingCount)
	require.Empty(t, output.FailedJobIDs)
	require.Equal(t, `{"importId":"importIdRetry","importCount":2}`, string(output.ImportingParameters))
	requireImportingHashes(t, output, map[int64]string{1: "good1", 3: "good2"})
	require.NotContains(t, output.JobImportingParameters, int64(2))
}

// The first upload is rejected (one profile stripped) and the single retry is
// rejected too. There must be no third attempt; the stripped job and the
// surviving jobs are all aborted.
func TestUploadRetryAlsoFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	uploader := klaviyobulkupload.KlaviyoBulkUploader{
		DestName:              "Klaviyo Bulk Upload",
		DestinationConfig:     destination.Config,
		Logger:                logger.NOP,
		StatsFactory:          stats.NOP,
		KlaviyoAPIService:     mockKlaviyoAPIService,
		BatchSize:             10000,
		MaxPayloadSize:        4600000,
		MaxAllowedProfileSize: 512000,
	}

	tempFile, err := os.CreateTemp("", "test_upload_retryfail_*.jsonl")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	const tmpl = `{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"%s","jobIdentifier":"%s:%d"}}]}}}}}},"metadata":{"job_id":%d}}`
	lines := []string{
		fmt.Sprintf(tmpl, "good1@mail.com", "good1", 1, 1),
		fmt.Sprintf(tmpl, "bad@", "bad", 2, 2),
		fmt.Sprintf(tmpl, "good2@mail.com", "good2", 3, 3),
	}
	for i, line := range lines {
		if i > 0 {
			_, err = tempFile.WriteString("\n")
			require.NoError(t, err)
		}
		_, err = tempFile.WriteString(line)
		require.NoError(t, err)
	}
	tempFile.Close()

	callCount := 0
	mockKlaviyoAPIService.EXPECT().
		UploadProfiles(gomock.Any()).
		DoAndReturn(func(payload klaviyobulkupload.Payload) (*klaviyobulkupload.UploadResp, error) {
			callCount++
			switch callCount {
			case 1: // index 1 rejected
				return &klaviyobulkupload.UploadResp{
					Errors: klaviyobulkupload.ErrorDetailList{
						{Detail: "Invalid email address", Source: klaviyobulkupload.ErrorSource{Pointer: "/data/attributes/profiles/data/1/attributes/email"}},
					},
					StatusCode: http.StatusBadRequest,
				}, fmt.Errorf("upload rejected")
			case 2: // retry rejected again
				return &klaviyobulkupload.UploadResp{
					Errors: klaviyobulkupload.ErrorDetailList{
						{Detail: "Invalid input.", Source: klaviyobulkupload.ErrorSource{Pointer: "/data/attributes/profiles/data/0/attributes/email"}},
					},
					StatusCode: http.StatusBadRequest,
				}, fmt.Errorf("upload rejected")
			}
			return nil, fmt.Errorf("unexpected call count: %d", callCount)
		}).Times(2)

	output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination:     destination,
		FileName:        tempFile.Name(),
		ImportingJobIDs: []int64{1, 2, 3},
	})

	require.Equal(t, 2, callCount, "must try once then retry once, no third attempt")
	require.ElementsMatch(t, []int64{1, 2, 3}, output.AbortJobIDs)
	require.Equal(t, 3, output.AbortCount)
	require.Empty(t, output.ImportingJobIDs)
	require.Empty(t, output.FailedJobIDs)
	require.Empty(t, output.JobImportingParameters)
}

func TestExtractProfileValidInput(t *testing.T) {
	kbu := klaviyobulkupload.KlaviyoBulkUploader{}

	dataPayloadJSON := `{
		"attributes": {
			"profiles": {
				"data": [
					{
						"attributes": {
							"anonymous_id": 111222334,
							"email": "qwe122@mail.com",
							"first_name": "Testqwe0122",
							"jobIdentifier": "111222334:1",
							"last_name": "user0122",
							"location": {
								"city": "delhi",
								"country": "India",
								"ip": "213.5.6.41"
							},
							"phone_number": "+919912000123"
						},
						"id": "111222334",
						"type": "profile"
					}
				]
			}
		},
		"relationships": {
			"lists": {
				"data": [
					{
						"id": "UKth4J",
						"type": "list"
					}
				]
			}
		},
		"type": "profile-bulk-import-job"
	}`
	var data klaviyobulkupload.Data
	err := jsonrs.Unmarshal([]byte(dataPayloadJSON), &data)
	if err != nil {
		t.Errorf("jsonrs.Unmarshal failed: %v", err)
	}
	expectedProfile := `{"attributes":{"email":"qwe122@mail.com","phone_number":"+919912000123","first_name":"Testqwe0122","last_name":"user0122","location":{"city":"delhi","country":"India","ip":"213.5.6.41"}},"id":"111222334","type":"profile"}`
	result := kbu.ExtractProfile(data)
	profileJson, _ := jsonrs.Marshal(result)
	assert.JSONEq(t, expectedProfile, string(profileJson))
}

// Test case for doing integration test of Upload method
func TestUploadIntegration(t *testing.T) {
	t.Skip("Skipping this integ test for now.")
	kbu, err := klaviyobulkupload.NewManager(logger.NOP, stats.NOP, destination)
	assert.NoError(t, err)
	assert.NotNil(t, kbu)

	asyncDestStruct := &common.AsyncDestinationStruct{
		Destination:     destination,
		FileName:        filepath.Join(currentDir, "testdata/uploadData.jsonl"),
		ImportingJobIDs: []int64{1, 2, 3},
	}

	uploadResp := kbu.Upload(context.Background(), asyncDestStruct)
	assert.NotNil(t, uploadResp)
	assert.Equal(t, destination.ID, uploadResp.DestinationID)
	assert.Empty(t, uploadResp.FailedJobIDs)
	assert.Empty(t, uploadResp.AbortJobIDs)
	assert.Empty(t, uploadResp.AbortReason)
	assert.NotEmpty(t, uploadResp.ImportingJobIDs)
	assert.NotNil(t, uploadResp.ImportingParameters)

	importId := gjson.GetBytes(uploadResp.ImportingParameters, "importId").String()
	pollResp := kbu.Poll(context.Background(), common.AsyncPoll{ImportId: importId})
	assert.NotNil(t, pollResp)
	assert.Equal(t, http.StatusOK, pollResp.StatusCode)
	assert.True(t, pollResp.Complete)
	assert.False(t, pollResp.HasFailed)
	assert.False(t, pollResp.HasWarning)
}

func TestPoll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	testLogger := logger.NewLogger().Child("klaviyo-bulk-upload-test")

	uploader := klaviyobulkupload.KlaviyoBulkUploader{
		DestName:          "Klaviyo Bulk Upload",
		DestinationConfig: destination.Config,
		Logger:            testLogger,
		StatsFactory:      stats.NOP,
		KlaviyoAPIService: mockKlaviyoAPIService,
	}

	t.Run("Successful Poll", func(t *testing.T) {
		pollStatusResp := &klaviyobulkupload.PollResp{
			Data: struct {
				Id         string `json:"id"`
				Attributes struct {
					Total_count     int    `json:"total_count"`
					Completed_count int    `json:"completed_count"`
					Failed_count    int    `json:"failed_count"`
					Status          string `json:"status"`
				} `json:"attributes"`
			}{
				Id: "importId1",
				Attributes: struct {
					Total_count     int    `json:"total_count"`
					Completed_count int    `json:"completed_count"`
					Failed_count    int    `json:"failed_count"`
					Status          string `json:"status"`
				}{
					Total_count:     1,
					Completed_count: 1,
					Failed_count:    0,
					Status:          "complete",
				},
			},
		}

		mockKlaviyoAPIService.EXPECT().
			GetUploadStatus("importId1").
			Return(pollStatusResp, nil)

		pollInput := common.AsyncPoll{
			ImportId: "importId1",
		}

		jobStatus := uploader.Poll(context.Background(), pollInput)
		assert.NotNil(t, jobStatus)
		assert.Equal(t, true, jobStatus.Complete)
		assert.Equal(t, http.StatusOK, jobStatus.StatusCode)
		assert.Equal(t, false, jobStatus.HasFailed)
		assert.Equal(t, false, jobStatus.HasWarning)
		assert.Empty(t, jobStatus.FailedJobParameters)
		assert.Empty(t, jobStatus.WarningJobParameters)
		assert.Empty(t, jobStatus.Error)
	})

	t.Run("Poll with Errors", func(t *testing.T) {
		pollStatusFailedResp := &klaviyobulkupload.PollResp{
			Data: struct {
				Id         string `json:"id"`
				Attributes struct {
					Total_count     int    `json:"total_count"`
					Completed_count int    `json:"completed_count"`
					Failed_count    int    `json:"failed_count"`
					Status          string `json:"status"`
				} `json:"attributes"`
			}{
				Id: "importId2",
				Attributes: struct {
					Total_count     int    `json:"total_count"`
					Completed_count int    `json:"completed_count"`
					Failed_count    int    `json:"failed_count"`
					Status          string `json:"status"`
				}{
					Total_count:     1,
					Completed_count: 0,
					Failed_count:    1,
					Status:          "complete",
				},
			},
		}

		mockKlaviyoAPIService.EXPECT().
			GetUploadStatus("importId2").
			Return(pollStatusFailedResp, fmt.Errorf("The import job failed"))

		pollInput := common.AsyncPoll{
			ImportId: "importId2",
		}

		jobStatus := uploader.Poll(context.Background(), pollInput)
		assert.NotNil(t, jobStatus)
		assert.Equal(t, true, jobStatus.Complete)
		assert.Equal(t, true, jobStatus.HasFailed)
		assert.Equal(t, false, jobStatus.HasWarning)
		assert.Empty(t, jobStatus.WarningJobParameters)
		assert.Equal(t, "Error during fetching upload status The import job failed", jobStatus.Error)
	})
}

func TestGetUploadStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	testLogger := logger.NewLogger().Child("klaviyo-bulk-upload-test")

	uploader := klaviyobulkupload.KlaviyoBulkUploader{
		DestName:          "Klaviyo Bulk Upload",
		DestinationConfig: destination.Config,
		Logger:            testLogger,
		StatsFactory:      stats.NOP,
		KlaviyoAPIService: mockKlaviyoAPIService,
	}

	t.Run("Failure GetUploadStats: Import Job Failed", func(t *testing.T) {
		uploadStatsResp := &klaviyobulkupload.UploadStatusResp{
			Data: []struct {
				Type       string `json:"type"`
				ID         string `json:"id"`
				Attributes struct {
					Code   string `json:"code"`
					Title  string `json:"title"`
					Detail string `json:"detail"`
					Source struct {
						Pointer string `json:"pointer"`
					} `json:"source"`
					OriginalPayload struct {
						Id          string `json:"id"`
						AnonymousId string `json:"anonymous_id"`
					} `json:"original_payload"`
				} `json:"attributes"`
				Links struct {
					Self string `json:"self"`
				} `json:"links"`
			}{
				{
					Type: "error",
					ID:   "1",
					Attributes: struct {
						Code   string `json:"code"`
						Title  string `json:"title"`
						Detail string `json:"detail"`
						Source struct {
							Pointer string `json:"pointer"`
						} `json:"source"`
						OriginalPayload struct {
							Id          string `json:"id"`
							AnonymousId string `json:"anonymous_id"`
						} `json:"original_payload"`
					}{
						Code:   "400",
						Title:  "Bad Request",
						Detail: "The import job failed",
						Source: struct {
							Pointer string `json:"pointer"`
						}{Pointer: "importId1"},
						OriginalPayload: struct {
							Id          string `json:"id"`
							AnonymousId string `json:"anonymous_id"`
						}{AnonymousId: "111222334"},
					},
					Links: struct {
						Self string `json:"self"`
					}{Self: "selfLink"},
				},
			},
		}

		mockKlaviyoAPIService.EXPECT().
			GetUploadErrors("importId1").
			Return(uploadStatsResp, nil)

		uploadStatsInput := common.GetUploadStatsInput{
			FailedJobParameters: "importId1",
			ImportingList: []*jobsdb.JobT{
				importingJob(t, 1, "111222334"),
				importingJob(t, 2, "222333445"),
			},
		}

		statsResponse := uploader.GetUploadStats(uploadStatsInput)
		assert.NotNil(t, statsResponse)
		assert.Equal(t, http.StatusOK, statsResponse.StatusCode)
		// assert.Equal(t, "The import job failed", statsResponse.Error)
		require.Equal(t, []int64{1}, statsResponse.Metadata.AbortedKeys)
		require.Equal(t, map[int64]string{1: "The import job failed"}, statsResponse.Metadata.AbortedReasons)
		require.Equal(t, []int64{2}, statsResponse.Metadata.SucceededKeys)
	})

	t.Run("GetUploadStats with Errors", func(t *testing.T) {
		mockKlaviyoAPIService.EXPECT().
			GetUploadErrors("importId1").
			Return(nil, fmt.Errorf("some error"))

		uploadStatsInput := common.GetUploadStatsInput{
			FailedJobParameters: "importId1",
			ImportingList: []*jobsdb.JobT{
				importingJob(t, 1, "111222334"),
				importingJob(t, 2, "222333445"),
			},
		}

		statsResponse := uploader.GetUploadStats(uploadStatsInput)
		assert.NotNil(t, statsResponse)
		assert.Equal(t, http.StatusBadRequest, statsResponse.StatusCode)
		assert.Equal(t, "some error", statsResponse.Error)
	})
}

// Correlation survives an uploader replacement (including an identifierType config
// change). Klaviyo returns original_payload.id when identifierType is id, even if
// anonymous_id is present, so the id must win during matching.
func TestUploadStatsAfterManagerReplacement(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	file := filepath.Join(t.TempDir(), "profiles.jsonl")
	const identifier = "urn:abc"
	// The processor suffix differs from the batch-router job_id on purpose.
	const line = `{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","id":"urn:abc","attributes":{"email":"person@example.com","jobIdentifier":"urn:abc:901"}}]}}}}}},"metadata":{"job_id":42}}`
	const lineWithoutJobIdentifier = `{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","id":"other","attributes":{"email":"other@example.com"}}]}}}}}},"metadata":{"job_id":43}}`
	require.NoError(t, os.WriteFile(file, []byte(line+"\n"+lineWithoutJobIdentifier), 0o600))

	api.EXPECT().UploadProfiles(gomock.Any()).DoAndReturn(func(payload klaviyobulkupload.Payload) (*klaviyobulkupload.UploadResp, error) {
		require.Len(t, payload.Data.Attributes.Profiles.Data, 2)
		require.Empty(t, payload.Data.Attributes.Profiles.Data[0].Attributes.JobIdentifier)
		require.Empty(t, payload.Data.Attributes.Profiles.Data[1].Attributes.JobIdentifier)
		require.Equal(t, identifier, payload.Data.Attributes.Profiles.Data[0].ID)
		return &klaviyobulkupload.UploadResp{Data: struct {
			Id string `json:"id"`
		}{Id: "import-42"}}, nil
	})
	uploader := &klaviyobulkupload.KlaviyoBulkUploader{
		Logger: logger.NOP, StatsFactory: stats.NOP, KlaviyoAPIService: api,
		BatchSize: 10000, MaxPayloadSize: 4600000, MaxAllowedProfileSize: 512000,
	}
	output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination: destination, FileName: file, ImportingJobIDs: []int64{42, 43},
	})
	require.Equal(t, []int64{42, 43}, output.ImportingJobIDs)
	requireImportingHashes(t, output, map[int64]string{42: identifier})
	require.NotContains(t, output.JobImportingParameters, int64(43))
	require.NotContains(t, string(output.ImportingParameters), identifier)

	// Simulate batchrouter's metadata merge into LastJobStatus.Parameters.
	statusParameters, err := sjson.SetBytes(output.ImportingParameters, "metadata", output.JobImportingParameters[42])
	require.NoError(t, err)
	job := &jobsdb.JobT{JobID: 42, LastJobStatus: jobsdb.JobStatusT{Parameters: statusParameters}}
	jobWithoutIdentifier := &jobsdb.JobT{JobID: 43, LastJobStatus: jobsdb.JobStatusT{Parameters: output.ImportingParameters}}
	updatedDestination := *destination
	updatedDestination.Config = map[string]any{"privateApiKey": "changed-key", "identifierType": "id"}
	fresh, err := klaviyobulkupload.NewManager(logger.NOP, stats.NOP, &updatedDestination)
	require.NoError(t, err)
	fresh.KlaviyoAPIService = api

	var errorsResp klaviyobulkupload.UploadStatusResp
	require.NoError(t, jsonrs.Unmarshal([]byte(`{"data":[{"attributes":{"detail":"invalid id","original_payload":{"id":"urn:abc","anonymous_id":"unrelated"}}}]}`), &errorsResp))
	api.EXPECT().GetUploadErrors("import-42").Return(&errorsResp, nil)
	result := fresh.GetUploadStats(common.GetUploadStatsInput{
		FailedJobParameters: "import-42", ImportingList: []*jobsdb.JobT{job, jobWithoutIdentifier},
	})
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.Equal(t, []int64{42}, result.Metadata.AbortedKeys)
	require.Equal(t, map[int64]string{42: "invalid id"}, result.Metadata.AbortedReasons)
	require.Equal(t, []int64{43}, result.Metadata.SucceededKeys)
}

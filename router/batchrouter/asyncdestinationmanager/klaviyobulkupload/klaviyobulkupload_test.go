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
			JobImportingParameters: output.JobImportingParameters,
			FailedJobIDs:           nil,
			FailedReason:           "",
			FailedCount:            0,
			AbortJobIDs:            nil,
			AbortReason:            "",
			AbortCount:             0,
			SucceededJobIDs:        nil,
			SuccessResponse:        "",
			ImportingCount:         1,
			DestinationID:          destination.ID,
		}, output)
		require.Equal(t, `{"importId":"importId1","importCount":1}`, string(output.ImportingParameters))
		require.Len(t, output.JobImportingParameters, 1)
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
			JobImportingParameters: output.JobImportingParameters,
			FailedJobIDs:           []int64{1},
			FailedReason:           "upload failed with status 0: ",
			FailedCount:            1,
			AbortJobIDs:            nil,
			AbortReason:            "",
			AbortCount:             0,
			SucceededJobIDs:        nil,
			SuccessResponse:        "",
			ImportingCount:         0,
			DestinationID:          destination.ID,
		}, output)
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
			JobImportingParameters: output.JobImportingParameters,
			ImportingCount:         2, // matches ImportingJobIDs
			SucceededJobIDs:        nil,
			SuccessResponse:        "",
			FailedReason:           "upload failed with status 429: {ID=, Code=, Title=, Detail=rate limit exceeded, Source={Pointer=, Parameter=}}",
			DestinationID:          destination.ID,
		}, output)
		require.Equal(t, `{"importId":"importId3","importCount":2}`, string(output.ImportingParameters))
		require.Len(t, output.JobImportingParameters, 2)
		require.NotContains(t, output.JobImportingParameters, int64(1))
		require.NotContains(t, output.JobImportingParameters, int64(3))
	})
	t.Run("accepted jobs save sent identifier hashes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockAPIService.NewMockKlaviyoAPIService(ctrl)
		manager := &klaviyobulkupload.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP, BatchSize: 2, MaxPayloadSize: 4600000, MaxAllowedProfileSize: 512000}
		api.EXPECT().UploadProfiles(gomock.Any()).DoAndReturn(func(payload klaviyobulkupload.Payload) (*klaviyobulkupload.UploadResp, error) {
			require.Len(t, payload.Data.Attributes.Profiles.Data, 2)
			for _, profile := range payload.Data.Attributes.Profiles.Data {
				require.Empty(t, profile.Attributes.JobIdentifier)
				require.Empty(t, profile.ID)
				require.NotEmpty(t, profile.Attributes.Email)
			}
			resp := &klaviyobulkupload.UploadResp{}
			resp.Data.Id = "chunk"
			return resp, nil
		}).Times(1)
		lines := []byte(`{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"qwe22@mail.com","anonymous_id":"user1","jobIdentifier":"user1:1"}}]}}}}}},"metadata":{"job_id":1}}` + "\n" +
			`{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"example@mail.com","anonymous_id":"user2","jobIdentifier":"user2:2"}}]}}}}}},"metadata":{"job_id":2}}` + "\n")
		path := filepath.Join(t.TempDir(), "upload.jsonl")
		require.NoError(t, os.WriteFile(path, lines, 0o600))
		out := manager.Upload(context.Background(), &common.AsyncDestinationStruct{Destination: destination, FileName: path, ImportingJobIDs: []int64{1, 2}})
		require.Equal(t, []int64{1, 2}, out.ImportingJobIDs)
		require.Len(t, out.JobImportingParameters, 2)
		require.Equal(t, identifierHash("email", "qwe22@mail.com"), gjson.GetBytes(out.JobImportingParameters[1], "identifierHash").String())
		require.Equal(t, identifierHash("email", "example@mail.com"), gjson.GetBytes(out.JobImportingParameters[2], "identifierHash").String())
		require.Equal(t, "chunk", gjson.GetBytes(out.JobImportingParameters[1], "chunkImportId").String())
		require.NotContains(t, string(out.JobImportingParameters[1]), "user1")
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
	require.Len(t, output.JobImportingParameters, 2)
	require.NotEmpty(t, output.JobImportingParameters[1])
	require.NotEmpty(t, output.JobImportingParameters[3])
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

// TestGetUploadStats preserves the original success and API-error cases with
// correlation restored from importing status parameters.
func TestGetUploadStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockAPIService.NewMockKlaviyoAPIService(ctrl)
	uploader := klaviyobulkupload.KlaviyoBulkUploader{
		DestName: "Klaviyo Bulk Upload", DestinationConfig: map[string]any{"listId": "changed-list"},
		Logger:       logger.NewLogger().Child("klaviyo-bulk-upload-test"),
		StatsFactory: stats.NOP, KlaviyoAPIService: api,
	}
	jobs := []*jobsdb.JobT{
		{JobID: 1, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(fmt.Sprintf(`{"metadata":{"identifierHash":%q,"chunkImportId":"importId1"}}`, identifierHash("id", "1")))}},
		{JobID: 2, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(fmt.Sprintf(`{"metadata":{"identifierHash":%q,"chunkImportId":"importId1"}}`, identifierHash("id", "2")))}},
	}
	input := common.GetUploadStatsInput{FailedJobParameters: "importId1", ImportingList: jobs}

	t.Run("fresh manager after config change matches sent identifier", func(t *testing.T) {
		api.EXPECT().GetUploadStatus("importId1").Return(failedStatus(), nil)
		api.EXPECT().GetUploadErrors("importId1").Return(errorsResponse(t, errorRow(`{"id":"1","anonymous_id":"111222334"}`)), nil)
		result := uploader.GetUploadStats(input)
		assert.Equal(t, http.StatusOK, result.StatusCode)
		assert.Equal(t, []int64{1}, result.Metadata.AbortedKeys)
		assert.Equal(t, "rejected", result.Metadata.AbortedReasons[1])
		assert.Equal(t, []int64{2}, result.Metadata.SucceededKeys)
	})

	t.Run("GetUploadStats with Errors", func(t *testing.T) {
		api.EXPECT().GetUploadStatus("importId1").Return(failedStatus(), nil)
		api.EXPECT().GetUploadErrors("importId1").Return(nil, fmt.Errorf("some error"))
		result := uploader.GetUploadStats(input)
		assert.Equal(t, http.StatusBadRequest, result.StatusCode)
		assert.Equal(t, "some error", result.Error)
	})
	t.Run("partial metadata and shared identifier key", func(t *testing.T) {
		for _, duplicate := range []bool{false, true} {
			t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
				ctrl := gomock.NewController(t)
				api := mockAPIService.NewMockKlaviyoAPIService(ctrl)
				jobs := []*jobsdb.JobT{statusJob(1, identifierHash("id", "1")), statusJob(2, ""), statusJob(3, identifierHash("id", "3"))}
				if duplicate {
					jobs[1] = statusJob(2, identifierHash("id", "1"))
				}
				api.EXPECT().GetUploadStatus("chunk").Return(failedStatus(), nil)
				api.EXPECT().GetUploadErrors("chunk").Return(errorsResponse(t, errorRow(`{"id":"1"}`)), nil)
				result := newStatsManager(api).GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk", ImportingList: jobs})
				require.Equal(t, []int64{1, 2}, result.Metadata.AbortedKeys)
				require.Equal(t, []int64{3}, result.Metadata.SucceededKeys)
				if duplicate {
					require.Equal(t, "rejected", result.Metadata.AbortedReasons[2])
				} else {
					require.Contains(t, result.Metadata.AbortedReasons[2], "cannot reconcile")
				}
			})
		}
	})

	t.Run("unmatched error aborts unproved remainder", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockAPIService.NewMockKlaviyoAPIService(ctrl)
		api.EXPECT().GetUploadStatus("chunk").Return(failedStatus(), nil)
		api.EXPECT().GetUploadErrors("chunk").Return(errorsResponse(t, errorRow(`{"id":"unknown"}`)), nil)
		jobs := []*jobsdb.JobT{statusJob(1, identifierHash("id", "1")), statusJob(2, identifierHash("id", "2"))}
		result := newStatsManager(api).GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"chunk","importCount":2}`), FailedJobParameters: "chunk", ImportingList: jobs})
		require.ElementsMatch(t, []int64{1, 2}, result.Metadata.AbortedKeys)
		require.Empty(t, result.Metadata.SucceededKeys)
	})

	t.Run("no correlation metadata aborts batch", func(t *testing.T) {
		jobs := []*jobsdb.JobT{{JobID: 1}, {JobID: 2}}
		result := newStatsManager(nil).GetUploadStats(common.GetUploadStatsInput{FailedJobParameters: "importId1", ImportingList: jobs})
		assert.Equal(t, http.StatusOK, result.StatusCode)
		assert.ElementsMatch(t, []int64{1, 2}, result.Metadata.AbortedKeys)
		assert.Empty(t, result.Metadata.SucceededKeys)
	})
}

func identifierHash(kind, value string) string {
	sum := sha256.Sum256([]byte(kind + ":" + value))
	return hex.EncodeToString(sum[:])
}

func statusJob(id int64, hash string) *jobsdb.JobT {
	return &jobsdb.JobT{JobID: id, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(fmt.Sprintf(`{"metadata":{"identifierHash":%q,"chunkImportId":"chunk"}}`, hash))}}
}

func errorsResponse(t *testing.T, original ...string) *klaviyobulkupload.UploadStatusResp {
	t.Helper()
	var result klaviyobulkupload.UploadStatusResp
	for _, payload := range original {
		data := []byte(fmt.Sprintf(`{"data":[%s]}`, payload))
		var one klaviyobulkupload.UploadStatusResp
		require.NoError(t, jsonrs.Unmarshal(data, &one))
		result.Data = append(result.Data, one.Data...)
	}
	return &result
}

func errorRow(payload string) string {
	return fmt.Sprintf(`{"attributes":{"detail":"rejected","original_payload":%s}}`, payload)
}

func failedStatus() *klaviyobulkupload.PollResp {
	var status klaviyobulkupload.PollResp
	status.Data.Attributes.Status = "complete"
	status.Data.Attributes.Failed_count = 1
	return &status
}

func newStatsManager(api klaviyobulkupload.KlaviyoAPIService) *klaviyobulkupload.KlaviyoBulkUploader {
	return &klaviyobulkupload.KlaviyoBulkUploader{KlaviyoAPIService: api, Logger: logger.NOP, StatsFactory: stats.NOP}
}

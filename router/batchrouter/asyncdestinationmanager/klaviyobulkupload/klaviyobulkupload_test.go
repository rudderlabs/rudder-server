package klaviyobulkupload_test

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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
	Config: map[string]interface{}{
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
		DestName:          "Klaviyo Bulk Upload",
		DestinationConfig: destination.Config,
		Logger:            testLogger,
		StatsFactory:      stats.NOP,
		KlaviyoAPIService: mockKlaviyoAPIService,
		JobIdToIdentifierMap: map[string]int64{
			"111222334": 1,
			"222333445": 2,
		},
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

	testData := []byte(`{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"qwe22@mail.com","first_name":"Testqwe0022","last_name":"user","phone_number":"+919902330123","location":{"address1":"dallas street","address2":"oppenheimer market","city":"delhi","country":"India","ip":"213.5.6.41"},"anonymous_id":"user1","jobIdentifier":"user1:1"}}]}},"relationships":{"lists":{"data":[{"type":"list","id":"list101"}]}}}}}},"metadata":{"jobId":1}}`)
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

		output := uploader.Upload(asyncDestStruct)
		assert.NotNil(t, output)
		assert.Equal(t, destination.ID, output.DestinationID)
		assert.Empty(t, output.FailedJobIDs)
		assert.Empty(t, output.AbortJobIDs)
		assert.Empty(t, output.AbortReason)
		assert.NotEmpty(t, output.ImportingJobIDs)
	})

	t.Run("Unsuccessful Upload", func(t *testing.T) {
		mockKlaviyoAPIService.EXPECT().
			UploadProfiles(gomock.Any()).
			Return(&klaviyobulkupload.UploadResp{
				Errors: []klaviyobulkupload.ErrorDetail{
					{Detail: "upload failed"},
				},
			}, fmt.Errorf("upload failed with errors: %+v", []klaviyobulkupload.ErrorDetail{
				{Detail: "upload failed"},
			}))

		asyncDestStruct := &common.AsyncDestinationStruct{
			Destination:     destination,
			FileName:        tempFile.Name(),
			ImportingJobIDs: []int64{1},
		}

		output := uploader.Upload(asyncDestStruct)
		assert.NotNil(t, output)
		assert.Equal(t, destination.ID, output.DestinationID)
		assert.NotEmpty(t, output.FailedJobIDs)
		assert.Empty(t, output.ImportingJobIDs)
	})
	t.Run("UploadThreeChunksWithMiddleFailure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockKlaviyoAPIService := mockAPIService.NewMockKlaviyoAPIService(ctrl)
		uploader.BatchSize = 2
		uploader.KlaviyoAPIService = mockKlaviyoAPIService

		// Create a temporary file with test data
		tempFile, err := os.CreateTemp("", "test_upload_3chunks_*.jsonl")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		const testProfileTemplate = `{"message":{"body":{"JSON":{"data":{"type":"profile-bulk-import-job","attributes":{"profiles":{"data":[{"type":"profile","attributes":{"email":"%s@mail.com","jobIdentifier":"%s:%d"}}]}}}}}},"metadata":{"jobId":%d}}`

		profiles := []string{
			fmt.Sprintf(testProfileTemplate, "user1", "user1", 1, 1),
			fmt.Sprintf(testProfileTemplate, "user2", "user2", 2, 2),
			fmt.Sprintf(testProfileTemplate, "user3", "user3", 3, 3),
			fmt.Sprintf(testProfileTemplate, "user4", "user4", 4, 4),
			fmt.Sprintf(testProfileTemplate, "user5", "user5", 5, 5),
			fmt.Sprintf(testProfileTemplate, "user6", "user6", 6, 6),
		}

		// Write profiles to file, one per line
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

		// Mock expectations: Chunk 1 succeeds, Chunk 2 fails, Chunk 3 succeeds
		// Track which job IDs are in each chunk
		var chunk1JobIDs []int64
		var chunk2JobIDs []int64
		var chunk3JobIDs []int64
		callCount := 0

		mockKlaviyoAPIService.EXPECT().
			UploadProfiles(gomock.Any()).
			DoAndReturn(func(payload klaviyobulkupload.Payload) (*klaviyobulkupload.UploadResp, error) {
				callCount++

				// Extract job IDs from this chunk by checking the profiles
				// We'll identify jobs by their email addresses
				var jobIDsInChunk []int64
				for _, profile := range payload.Data.Attributes.Profiles.Data {
					email := profile.Attributes.Email
					switch email {
					case "user1@mail.com":
						jobIDsInChunk = append(jobIDsInChunk, 1)
					case "user2@mail.com":
						jobIDsInChunk = append(jobIDsInChunk, 2)
					case "user3@mail.com":
						jobIDsInChunk = append(jobIDsInChunk, 3)
					case "user4@mail.com":
						jobIDsInChunk = append(jobIDsInChunk, 4)
					case "user5@mail.com":
						jobIDsInChunk = append(jobIDsInChunk, 5)
					case "user6@mail.com":
						jobIDsInChunk = append(jobIDsInChunk, 6)
					}
				}

				switch callCount {
				case 1:
					// First chunk - succeed
					chunk1JobIDs = jobIDsInChunk
					return &klaviyobulkupload.UploadResp{
						Data: struct {
							Id string "json:\"id\""
						}{
							Id: "importId1",
						},
						Errors: nil,
					}, nil
				case 2:
					// Second chunk - fail
					chunk2JobIDs = jobIDsInChunk
					return &klaviyobulkupload.UploadResp{
						Errors: []klaviyobulkupload.ErrorDetail{
							{Detail: "chunk 2 upload failed"},
						},
					}, fmt.Errorf("chunk 2 upload failed")
				case 3:
					// Third chunk - succeed
					chunk3JobIDs = jobIDsInChunk
					return &klaviyobulkupload.UploadResp{
						Data: struct {
							Id string "json:\"id\""
						}{
							Id: "importId3",
						},
						Errors: nil,
					}, nil
				}
				// Should not reach here
				return nil, fmt.Errorf("unexpected call count: %d", callCount)
			}).
			AnyTimes() // Allow any number of calls, but we expect 3

		asyncDestStruct := &common.AsyncDestinationStruct{
			Destination:     destination,
			FileName:        tempFile.Name(),
			ImportingJobIDs: []int64{1, 2, 3, 4, 5, 6},
		}

		output := uploader.Upload(asyncDestStruct)
		assert.NotNil(t, output)

		// Verify that job IDs from chunk 2 (the failed chunk) are in FailedJobIDs
		assert.NotEmpty(t, chunk2JobIDs, "Chunk 2 should contain at least one job ID")
		assert.Equal(t, chunk2JobIDs, output.FailedJobIDs, "FailedCount should match the number of failed job IDs")

		allSuccessfulJobIDs := append(chunk1JobIDs, chunk3JobIDs...)
		for _, jobID := range allSuccessfulJobIDs {
			assert.Contains(t, output.ImportingJobIDs, jobID, "Job IDs from successful chunks should be in ImportingJobIDs")
			assert.NotContains(t, output.FailedJobIDs, jobID, "Job IDs from successful chunks should not be in FailedJobIDs")
		}

		// Verify ImportingParameters contains import IDs for successful chunks only
		assert.NotNil(t, output.ImportingParameters)
		importParamsJSON := string(output.ImportingParameters)
		assert.Contains(t, importParamsJSON, "importId1", "Should contain importId1 from chunk 1")
		assert.Contains(t, importParamsJSON, "importId3", "Should contain importId3 from chunk 3")
		assert.NotContains(t, importParamsJSON, "importId2", "Should not contain importId2 since chunk 2 failed")

		// Verify FailedCount matches the number of failed job IDs
		assert.Equal(t, len(output.FailedJobIDs), output.FailedCount, "FailedCount should match the number of failed job IDs")

		// Verify that all 6 job IDs are accounted for (either in failed or successful)
		totalAccounted := len(output.FailedJobIDs) + len(output.ImportingJobIDs)
		assert.Equal(t, 6, totalAccounted, "All 6 job IDs should be accounted for")
	})
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

	uploadResp := kbu.Upload(asyncDestStruct)
	assert.NotNil(t, uploadResp)
	assert.Equal(t, destination.ID, uploadResp.DestinationID)
	assert.Empty(t, uploadResp.FailedJobIDs)
	assert.Empty(t, uploadResp.AbortJobIDs)
	assert.Empty(t, uploadResp.AbortReason)
	assert.NotEmpty(t, uploadResp.ImportingJobIDs)
	assert.NotNil(t, uploadResp.ImportingParameters)

	importId := gjson.GetBytes(uploadResp.ImportingParameters, "importId").String()
	pollResp := kbu.Poll(common.AsyncPoll{ImportId: importId})
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
		JobIdToIdentifierMap: map[string]int64{
			"111222334": 1,
			"222333445": 2,
		},
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

		jobStatus := uploader.Poll(pollInput)
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

		jobStatus := uploader.Poll(pollInput)
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
		JobIdToIdentifierMap: map[string]int64{
			"111222334": 1,
			"222333445": 2,
		},
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
						}{Id: "1", AnonymousId: "111222334"},
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
				{JobID: 1},
				{JobID: 2},
			},
		}

		statsResponse := uploader.GetUploadStats(uploadStatsInput)
		assert.NotNil(t, statsResponse)
		assert.Equal(t, http.StatusOK, statsResponse.StatusCode)
		// assert.Equal(t, "The import job failed", statsResponse.Error)
		assert.NotEmpty(t, statsResponse.Metadata.AbortedKeys)
		assert.NotEmpty(t, statsResponse.Metadata.AbortedReasons)
		assert.NotEmpty(t, statsResponse.Metadata.SucceededKeys)
	})

	t.Run("GetUploadStats with Errors", func(t *testing.T) {
		mockKlaviyoAPIService.EXPECT().
			GetUploadErrors("importId1").
			Return(nil, fmt.Errorf("some error"))

		uploadStatsInput := common.GetUploadStatsInput{
			FailedJobParameters: "importId1",
			ImportingList: []*jobsdb.JobT{
				{JobID: 1},
				{JobID: 2},
			},
		}

		statsResponse := uploader.GetUploadStats(uploadStatsInput)
		assert.NotNil(t, statsResponse)
		assert.Equal(t, http.StatusBadRequest, statsResponse.StatusCode)
		assert.Equal(t, "some error", statsResponse.Error)
	})
}

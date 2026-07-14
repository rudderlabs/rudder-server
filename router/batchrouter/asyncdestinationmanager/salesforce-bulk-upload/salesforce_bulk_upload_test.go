package salesforcebulkupload_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	salesforcebulkupload_mocks "github.com/rudderlabs/rudder-server/mocks/router/salesforcebulkupload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	salesforcebulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk-upload"
)

func TestSalesforceBulk_Transform(t *testing.T) {
	testCases := []struct {
		name    string
		job     *jobsdb.JobT
		wantErr bool
	}{
		{
			name: "successful transform with valid payload",
			job: &jobsdb.JobT{
				JobID: 123,
				EventPayload: []byte(`{
					"channel": "sources",
					"context": {
						"externalId": [
							{
								"id": "evelyn.gonzalez@example.com",
								"identifierType": "Email",
								"type": "SALESFORCE_BULK_UPLOAD-Lead"
							}
						],
						"mappedToDestination": "true"
					},
					"traits": {
						"City": "Phoenix",
						"Company": "2025-11-25T03:45:21.14287Z",
						"Country": "USA",
						"CreatedDate": "2025-11-25T03:45:21.14287Z",
						"FirstName": "Evelyn",
						"Industry": "AZ",
						"LastName": "Gonzalez",
						"Phone": "555-111-0020"
					},
					"type": "identify",
					"userId": "evelyn.gonzalez@example.com"
				}`),
			},
			wantErr: false,
		},
		{
			name: "transform fails when externalId is absent",
			job: &jobsdb.JobT{
				JobID: 123,
				EventPayload: []byte(`{
					"channel": "sources",
					"context": {
						"mappedToDestination": "true"
					},
					"traits": {
						"City": "Phoenix",
						"Company": "2025-11-25T03:45:21.14287Z",
						"Country": "USA",
						"CreatedDate": "2025-11-25T03:45:21.14287Z",
						"FirstName": "Evelyn",
						"Industry": "AZ",
						"LastName": "Gonzalez",
						"Phone": "555-111-0020"
					},
					"type": "identify",
					"userId": "evelyn.gonzalez@example.com"
				}`),
			},
			wantErr: true,
		},
		{
			name: "transform fails when externalId id is empty",
			job: &jobsdb.JobT{
				JobID: 124,
				EventPayload: []byte(`{
					"channel": "sources",
					"context": {
						"externalId": [
							{
								"id": "",
								"identifierType": "Email",
								"type": "SALESFORCE_BULK_UPLOAD-Lead"
							}
						],
						"mappedToDestination": "true"
					},
					"traits": {
						"FirstName": "Evelyn",
						"LastName": "Gonzalez"
					},
					"type": "identify",
					"userId": "evelyn.gonzalez@example.com"
				}`),
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, nil, &backendconfig.DestinationT{})
			result, err := uploader.Transform(tc.job)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, result)

			var parsed common.AsyncJob
			err = jsonrs.Unmarshal([]byte(result), &parsed)
			require.NoError(t, err)
			require.NotNil(t, parsed.Message)
			require.NotNil(t, parsed.Metadata)
			require.Equal(t, float64(tc.job.JobID), parsed.Metadata["job_id"])
		})
	}
}

func TestSalesforceBulk_Upload_NullTraitsBecomeNASentinelInCSV(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
	uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})

	// Run real events through Transform and stage them the way the batch
	// router does, so the CSV assertion covers the whole pipeline: a null
	// trait in the event must surface as #N/A in the file uploaded to
	// Salesforce, while a field absent from the event stays an empty cell.
	events := []*jobsdb.JobT{
		{
			JobID: 125,
			EventPayload: []byte(`{
				"channel": "sources",
				"context": {
					"externalId": [
						{
							"id": "evelyn.gonzalez@example.com",
							"identifierType": "Email",
							"type": "SALESFORCE_BULK_UPLOAD-Lead"
						}
					],
					"mappedToDestination": "true"
				},
				"traits": {
					"FirstName": "Evelyn",
					"LastName": null,
					"Phone": null
				},
				"type": "identify",
				"userId": "evelyn.gonzalez@example.com"
			}`),
		},
		{
			JobID: 126,
			EventPayload: []byte(`{
				"channel": "sources",
				"context": {
					"externalId": [
						{
							"id": "john.doe@example.com",
							"identifierType": "Email",
							"type": "SALESFORCE_BULK_UPLOAD-Lead"
						}
					],
					"mappedToDestination": "true"
				},
				"traits": {
					"FirstName": "John"
				},
				"type": "identify",
				"userId": "john.doe@example.com"
			}`),
		},
	}

	tempFile, err := os.CreateTemp(t.TempDir(), "test_upload_*.json")
	require.NoError(t, err)
	for _, event := range events {
		line, err := uploader.Transform(event)
		require.NoError(t, err)
		_, err = tempFile.WriteString(line + "\n")
		require.NoError(t, err)
	}
	require.NoError(t, tempFile.Close())

	mockAPI.EXPECT().CreateJob("Lead", "upsert", "Email").Return("sf-job-null", nil)
	// Upload removes the CSV file once it returns, so the file must be
	// inspected at UploadData call time — this is the exact artifact sent
	// to Salesforce.
	mockAPI.EXPECT().UploadData("sf-job-null", gomock.Any()).DoAndReturn(func(_, csvFilePath string) *salesforcebulkupload.APIError {
		file, err := os.Open(csvFilePath)
		require.NoError(t, err)
		defer file.Close()

		records, err := csv.NewReader(file).ReadAll()
		require.NoError(t, err)
		require.Equal(t, [][]string{
			{"Email", "FirstName", "LastName", "Phone"},
			{"evelyn.gonzalez@example.com", "Evelyn", "#N/A", "#N/A"},
			{"john.doe@example.com", "John", "", ""},
		}, records)
		return nil
	})
	mockAPI.EXPECT().CloseJob("sf-job-null").Return(nil)

	result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination:     &backendconfig.DestinationT{ID: "test-dest-null-e2e"},
		FileName:        tempFile.Name(),
		ImportingJobIDs: []int64{125, 126},
	})

	require.Equal(t, 2, result.ImportingCount)
	require.Equal(t, 0, result.FailedCount)
}

func TestSalesforceBulk_Upload(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_upload_*.json")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	testData := []common.AsyncJob{
		{
			Message: map[string]any{
				"Email":     "test1@example.com",
				"FirstName": "John",
				"LastName":  "Doe",
			},
			Metadata: map[string]any{
				"job_id": float64(1),
				"externalId": []map[string]any{
					{
						"id":             "test1@example.com",
						"identifierType": "Email",
						"type":           "SALESFORCE_BULK_UPLOAD-Lead",
					},
				},
			},
		},
		{
			Message: map[string]any{
				"Email":     "test2@example.com",
				"FirstName": "Jane",
				"LastName":  "Smith",
			},
			Metadata: map[string]any{
				"job_id": float64(2),
				"externalId": []map[string]any{
					{
						"id":             "test2@example.com",
						"identifierType": "Email",
						"type":           "SALESFORCE_BULK_UPLOAD-Lead",
					},
				},
			},
		},
	}

	for _, job := range testData {
		jobBytes, _ := jsonrs.Marshal(job)
		_, err := tempFile.Write(jobBytes)
		require.NoError(t, err)
		_, err = tempFile.Write([]byte("\n"))
		require.NoError(t, err)
	}
	tempFile.Close()

	t.Run("successful upload - single operation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).Return("sf-job-123", nil)
		mockAPI.EXPECT().UploadData(gomock.Any(), gomock.Any()).Return(nil)
		mockAPI.EXPECT().CloseJob(gomock.Any()).Return(nil)
		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})

		result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
			},
			FileName: tempFile.Name(),
			ImportingJobIDs: []int64{
				1,
				2,
			},
		})

		require.Equal(t, 2, result.ImportingCount)
		require.Equal(t, 0, result.FailedCount)
		require.JSONEq(t, `{"importId":{"id":"sf-job-123","externalIdField":"Email"}, "importCount":2}`, string(result.ImportingParameters))
		require.Len(t, result.JobImportingParameters, 2)
		require.JSONEq(t, `{"externalIdHash":"`+salesforcebulkupload.HashExternalID("test1@example.com")+`"}`, string(result.JobImportingParameters[1]))
		require.JSONEq(t, `{"externalIdHash":"`+salesforcebulkupload.HashExternalID("test2@example.com")+`"}`, string(result.JobImportingParameters[2]))
	})

	t.Run("emits brt_async_dest_payload_size, brt_async_dest_events_per_file and brt_async_dest_async_upload_time metrics", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		dest := &backendconfig.DestinationT{ID: "test-dest-1", WorkspaceID: "test-ws-1"}
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).Return("sf-job-123", nil)
		mockAPI.EXPECT().UploadData(gomock.Any(), gomock.Any()).Return(nil)
		mockAPI.EXPECT().CloseJob(gomock.Any()).Return(nil)
		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, statsStore, mockAPI, dest)

		result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
			Destination:     dest,
			FileName:        tempFile.Name(),
			ImportingJobIDs: []int64{1, 2},
		})
		require.Equal(t, 2, result.ImportingCount)

		tags := stats.Tags{
			"destType":      "SALESFORCE_BULK_UPLOAD",
			"destinationId": "test-dest-1",
			"workspaceId":   "test-ws-1",
		}
		require.Equal(t, float64(2), statsStore.Get("brt_async_dest_events_per_file", tags).LastValue())
		require.Greater(t, statsStore.Get("brt_async_dest_payload_size", tags).LastValue(), float64(0))
		require.Len(t, statsStore.Get("brt_async_dest_async_upload_time", tags).Durations(), 1)
	})

	t.Run("upload with API error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).Return("", &salesforcebulkupload.APIError{
			StatusCode: 500,
			Message:    "Internal Server Error",
			Category:   "ServerError",
		})

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})

		result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
			},
			FileName: tempFile.Name(),
			ImportingJobIDs: []int64{
				1,
				2,
			},
		})

		require.Equal(t, 0, result.ImportingCount)
		require.Greater(t, result.FailedCount, 0)
		require.Contains(t, result.FailedReason, "creating Salesforce job: Internal Server Error")
	})

	t.Run("upload with rate limit error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).Return("", &salesforcebulkupload.APIError{
			StatusCode: 429,
			Message:    "Rate limit exceeded",
			Category:   "RateLimit",
		})

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})

		result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
			},
			FileName: tempFile.Name(),
			ImportingJobIDs: []int64{
				1,
				2,
			},
		})

		require.Equal(t, 0, result.ImportingCount)
		require.Greater(t, result.FailedCount, 0)
		require.Contains(t, result.FailedReason, "creating Salesforce job: Rate limit exceeded")
	})
}

func TestSalesforceBulk_Poll(t *testing.T) {
	testCases := []struct {
		name           string
		jobStatus      []*salesforcebulkupload.JobResponse
		expectedStatus common.PollStatusResponse
		pollInput      common.AsyncPoll
	}{
		{
			name: "job complete - all success",
			jobStatus: []*salesforcebulkupload.JobResponse{{
				ID:                  "job-123",
				State:               "JobComplete",
				NumberRecordsFailed: 0,
			}},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  false,
			},
			pollInput: common.AsyncPoll{ImportId: `{"id":"job-123","headers":["Email"]}`},
		},
		{
			name: "job in progress",
			jobStatus: []*salesforcebulkupload.JobResponse{{
				ID:    "job-456",
				State: "InProgress",
			}},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				InProgress: true,
			},
			pollInput: common.AsyncPoll{ImportId: `{"id":"job-456","headers":["Email"]}`},
		},
		{
			name: "job complete with failures",
			jobStatus: []*salesforcebulkupload.JobResponse{{
				ID:                  "job-789",
				State:               "JobComplete",
				NumberRecordsFailed: 10,
			}},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  true,
			},
			pollInput: common.AsyncPoll{ImportId: `{"id":"job-789","headers":["Email"]}`},
		},
		{
			name: "job failed",
			jobStatus: []*salesforcebulkupload.JobResponse{{
				ID:           "job-failed",
				State:        "Failed",
				ErrorMessage: "Invalid object type",
			}},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  true,
			},
			pollInput: common.AsyncPoll{ImportId: `{"id":"job-failed","headers":["Email"]}`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
			for _, jobStatus := range tc.jobStatus {
				mockAPI.EXPECT().GetJobStatus(jobStatus.ID).Return(jobStatus, nil)
			}

			uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})

			result := uploader.Poll(context.Background(), tc.pollInput)

			require.Equal(t, tc.expectedStatus.StatusCode, result.StatusCode)
			require.Equal(t, tc.expectedStatus.Complete, result.Complete)
			require.Equal(t, tc.expectedStatus.InProgress, result.InProgress)
			require.Equal(t, tc.expectedStatus.HasFailed, result.HasFailed)
		})
	}
}

func TestSalesforceBulk_GetUploadStats(t *testing.T) {
	// importingJob builds an importing job whose status params carry the per-job
	// externalId metadata, exactly as handle_async persists it during upload.
	// The stored value is the SHA-256 hash of the externalId, not the raw value.
	importingJob := func(jobID int64, externalID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			JobID: jobID,
			LastJobStatus: jobsdb.JobStatusT{
				Parameters: []byte(`{"metadata":{"externalIdHash":"` + salesforcebulkupload.HashExternalID(externalID) + `"}}`),
			},
		}
	}
	newUploader := func(mockAPI *salesforcebulkupload_mocks.MockAPIServiceInterface) *salesforcebulkupload.Uploader {
		return salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})
	}

	importIDParams := json.RawMessage(`{"importId":"{\"id\":\"sf-job-123\",\"externalIdField\":\"Email\"}","metadata":{"csvHeader":""}}`)

	t.Run("matches succeeded and aborted jobs by externalId from job status params", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)

		// job 1 succeeded; job 2 failed. The CreatedDate column is coerced by
		// Salesforce but the Email (externalId) round-trips, so both still match.
		mockAPI.EXPECT().GetSuccessfulRecords(gomock.Any()).Return([]map[string]string{
			{"Email": "test1@example.com", "FirstName": "John", "CreatedDate": "2025-11-25T03:45:21.142Z", "sf__Id": "001"},
		}, nil)
		mockAPI.EXPECT().GetFailedRecords(gomock.Any()).Return([]map[string]string{
			{"Email": "test2@example.com", "FirstName": "Jane", "sf__Error": "REQUIRED_FIELD_MISSING:LastName"},
		}, nil)

		result := newUploader(mockAPI).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    importIDParams,
			ImportingList: []*jobsdb.JobT{importingJob(1, "test1@example.com"), importingJob(2, "test2@example.com")},
		})

		require.Equal(t, 200, result.StatusCode)
		require.Equal(t, []int64{1}, result.Metadata.SucceededKeys)
		require.Equal(t, []int64{2}, result.Metadata.AbortedKeys)
		require.Empty(t, result.Metadata.FailedKeys)
		require.Equal(t, "REQUIRED_FIELD_MISSING:LastName", result.Metadata.AbortedReasons[2])
	})

	t.Run("unmatched records are aborted, not retried", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)

		// Salesforce reformatted the externalId itself (e.g. a numeric/date
		// external id), so neither returned record matches any importing job.
		mockAPI.EXPECT().GetSuccessfulRecords(gomock.Any()).Return([]map[string]string{
			{"Email": "TEST1@EXAMPLE.COM", "sf__Id": "001"},
		}, nil)
		mockAPI.EXPECT().GetFailedRecords(gomock.Any()).Return([]map[string]string{
			{"Email": "TEST2@EXAMPLE.COM", "sf__Error": "boom"},
		}, nil)

		result := newUploader(mockAPI).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    importIDParams,
			ImportingList: []*jobsdb.JobT{importingJob(1, "test1@example.com"), importingJob(2, "test2@example.com")},
		})

		require.Equal(t, 200, result.StatusCode)
		require.ElementsMatch(t, []int64{1, 2}, result.Metadata.AbortedKeys)
		require.Empty(t, result.Metadata.FailedKeys)
		require.NotEmpty(t, result.Metadata.AbortedReasons[1])
		require.NotEmpty(t, result.Metadata.AbortedReasons[2])
	})

	t.Run("no externalId metadata on importing jobs aborts the batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().GetFailedRecords(gomock.Any()).Return([]map[string]string{}, nil)
		mockAPI.EXPECT().GetSuccessfulRecords(gomock.Any()).Return([]map[string]string{
			{"Email": "test1@example.com"}, {"Email": "test2@example.com"},
		}, nil)

		// importing jobs with no metadata in their status params → the map can't
		// be rebuilt, so the batch is aborted (and logged for alerting).
		result := newUploader(mockAPI).GetUploadStats(common.GetUploadStatsInput{
			Parameters:    importIDParams,
			ImportingList: []*jobsdb.JobT{{JobID: 1}, {JobID: 2}},
		})

		require.Equal(t, 200, result.StatusCode)
		require.ElementsMatch(t, []int64{1, 2}, result.Metadata.AbortedKeys)
		require.Empty(t, result.Metadata.FailedKeys)
	})

	t.Run("handles API error fetching failed records", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().GetFailedRecords(gomock.Any()).Return(nil, &salesforcebulkupload.APIError{
			StatusCode: 500,
			Message:    "Server error",
			Category:   "ServerError",
		})

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, &backendconfig.DestinationT{})

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`{"importId":"{\"id\":\"test-job-123\",\"headers\":[\"Email\"]}","metadata":{"csvHeader":""}}`),
		})

		require.Equal(t, 500, result.StatusCode)
		require.Contains(t, result.Error, "Failed to fetch failed records for job: test-job-123, Server error")
	})

	t.Run("handles invalid parameters", func(t *testing.T) {
		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, nil, &backendconfig.DestinationT{})

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`invalid json`),
		})

		require.Equal(t, 500, result.StatusCode)
		require.Contains(t, result.Error, "Failed to parse poll parameters")
	})
}

func TestSalesforceBulk_NewManager(t *testing.T) {
	t.Run("successful manager creation", func(t *testing.T) {
		destination := &backendconfig.DestinationT{
			ID:          "test-dest-123",
			WorkspaceID: "test-workspace-456",
			Config: map[string]any{
				"rudderAccountId": "test-account-789",
			},
		}

		ctrl := gomock.NewController(t)
		mockBackendConfig := mockBackendConfig.NewMockBackendConfig(ctrl)

		manager, err := salesforcebulkupload.NewManager(
			config.New(),
			logger.NOP,
			stats.NOP,
			destination,
			mockBackendConfig,
		)

		require.NoError(t, err)
		require.NotNil(t, manager)
	})
}

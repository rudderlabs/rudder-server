package salesforcebulkupload_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
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
			name: "successful transform with valid payload",
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, nil, nil)
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

func TestSalesforceBulk_Upload(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test_upload_*.json")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	testData := []common.AsyncJob{
		{
			Message: map[string]interface{}{
				"Email":     "test1@example.com",
				"FirstName": "John",
				"LastName":  "Doe",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(1),
				"externalId": []map[string]interface{}{
					{
						"id":             "test1@example.com",
						"identifierType": "Email",
						"type":           "SALESFORCE_BULK_UPLOAD-Lead",
					},
				},
			},
		},
		{
			Message: map[string]interface{}{
				"Email":     "test2@example.com",
				"FirstName": "Jane",
				"LastName":  "Smith",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(2),
				"externalId": []map[string]interface{}{
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
		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, nil)

		result := uploader.Upload(&common.AsyncDestinationStruct{
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
		require.JSONEq(t, `{"importId":{"id":"sf-job-123","headers":["Email","FirstName","LastName"]}}`, string(result.ImportingParameters))
	})

	t.Run("upload with API error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).Return("", &salesforcebulkupload.APIError{
			StatusCode: 500,
			Message:    "Internal Server Error",
			Category:   "ServerError",
		})

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, nil)

		result := uploader.Upload(&common.AsyncDestinationStruct{
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
		require.Contains(t, result.FailedReason, "Error creating Salesforce job: Internal Server Error")
	})

	t.Run("upload with rate limit error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().CreateJob(gomock.Any(), gomock.Any(), gomock.Any()).Return("", &salesforcebulkupload.APIError{
			StatusCode: 429,
			Message:    "Rate limit exceeded",
			Category:   "RateLimit",
		})

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, nil)

		result := uploader.Upload(&common.AsyncDestinationStruct{
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
		require.Contains(t, result.FailedReason, "Error creating Salesforce job: Rate limit exceeded")
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

			uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, nil)

			result := uploader.Poll(tc.pollInput)

			require.Equal(t, tc.expectedStatus.StatusCode, result.StatusCode)
			require.Equal(t, tc.expectedStatus.Complete, result.Complete)
			require.Equal(t, tc.expectedStatus.InProgress, result.InProgress)
			require.Equal(t, tc.expectedStatus.HasFailed, result.HasFailed)
		})
	}
}

func TestSalesforceBulk_GetUploadStats(t *testing.T) {
	t.Run("successful stats retrieval with failures", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_upload_*.json")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())
		testData := make([]common.AsyncJob, 0)
		testData = append(testData, common.AsyncJob{
			Message: map[string]interface{}{
				"Email":     "test1@example.com",
				"FirstName": "John",
				"LastName":  "Doe",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(1),
			},
		})
		for _, job := range testData {
			jobBytes, _ := jsonrs.Marshal(job)
			_, err := tempFile.Write(jobBytes)
			require.NoError(t, err)
			_, err = tempFile.Write([]byte("\n"))
			require.NoError(t, err)
		}
		tempFile.Close()

		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().GetFailedRecords(gomock.Any()).Return([]map[string]string{}, nil)
		mockAPI.EXPECT().GetSuccessfulRecords(gomock.Any()).Return([]map[string]string{{"Email": "test1@example.com", "FirstName": "John", "LastName": "Doe"}, {"Email": "test2@example.com", "FirstName": "Jane", "LastName": "Smith"}}, nil)

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, nil)
		uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{ID: "test-dest"},
			FileName:    tempFile.Name(),
		})

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`{"importId":"{\"id\":\"sf-job-123\",\"headers\":[\"Email\",\"FirstName\",\"LastName\"]}","metadata":{"csvHeader":""}}`),
			ImportingList: []*jobsdb.JobT{
				{JobID: 1},
				{JobID: 2},
			},
		})

		require.Equal(t, 200, result.StatusCode)
		require.NotNil(t, result.Metadata)
	})

	t.Run("handles API error fetching failed records", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockAPI := salesforcebulkupload_mocks.NewMockAPIServiceInterface(ctrl)
		mockAPI.EXPECT().GetFailedRecords(gomock.Any()).Return(nil, &salesforcebulkupload.APIError{
			StatusCode: 500,
			Message:    "Server error",
			Category:   "ServerError",
		})

		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, mockAPI, nil)

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`{"importId":"{\"id\":\"test-job-123\",\"headers\":[\"Email\"]}","metadata":{"csvHeader":""}}`),
		})

		require.Equal(t, 500, result.StatusCode)
		require.Contains(t, result.Error, "Failed to fetch failed records for job: test-job-123, Server error")
	})

	t.Run("handles invalid parameters", func(t *testing.T) {
		uploader := salesforcebulkupload.NewUploader(config.New(), logger.NOP, stats.NOP, nil, nil)

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
			Config: map[string]interface{}{
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

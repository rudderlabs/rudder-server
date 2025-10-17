package salesforcebulk

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
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
					"body": {
						"JSON": {
							"Email": "test@example.com",
							"FirstName": "John",
							"LastName": "Doe"
						}
					}
				}`),
			},
			wantErr: false,
		},
		{
			name: "transform with empty body.JSON",
			job: &jobsdb.JobT{
				JobID:        456,
				EventPayload: []byte(`{"body": {"JSON": {}}}`),
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uploader := &SalesforceBulkUploader{}
			result, err := uploader.Transform(tc.job)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, result)

			var parsed common.AsyncJob
			err = json.Unmarshal([]byte(result), &parsed)
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
			},
		},
	}

	for _, job := range testData {
		jobBytes, _ := jsonrs.Marshal(job)
		tempFile.Write(jobBytes)
		tempFile.Write([]byte("\n"))
	}
	tempFile.Close()

	t.Run("successful upload - single operation", func(t *testing.T) {
		var capturedCSVPath string
		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				require.Equal(t, "Lead", objectName)
				require.Equal(t, "insert", operation)
				return "sf-job-123", nil
			},
			UploadDataFunc: func(jobID, csvFilePath string) *APIError {
				require.Equal(t, "sf-job-123", jobID)
				capturedCSVPath = csvFilePath
				// Verify CSV file exists during upload
				_, err := os.Stat(csvFilePath)
				require.NoError(t, err)
				return nil
			},
			CloseJobFunc: func(jobID string) *APIError {
				require.Equal(t, "sf-job-123", jobID)
				return nil
			},
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			config:          DestinationConfig{Operation: "insert"},
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
			},
			FileName:        tempFile.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		require.Equal(t, 2, result.ImportingCount)
		require.Equal(t, 0, result.FailedCount)
		require.NotNil(t, result.ImportingParameters)

		var params struct {
			Jobs []SalesforceJobInfo `json:"jobs"`
		}
		err := jsonrs.Unmarshal(result.ImportingParameters, &params)
		require.NoError(t, err)
		require.Len(t, params.Jobs, 1)
		require.Equal(t, "sf-job-123", params.Jobs[0].ID)
		require.Equal(t, "insert", params.Jobs[0].Operation)

		// Verify CSV file was cleaned up after upload
		_, err = os.Stat(capturedCSVPath)
		require.True(t, os.IsNotExist(err), "CSV file should be cleaned up after upload")
	})

	t.Run("successful upload - multiple operations", func(t *testing.T) {
		// Create temp file with mixed operations
		tempFileMulti, err := os.CreateTemp("", "test_multi_op_*.json")
		require.NoError(t, err)
		defer os.Remove(tempFileMulti.Name())

		multiOpData := []common.AsyncJob{
			{
				Message: map[string]interface{}{
					"Email":           "insert1@example.com",
					"FirstName":       "Insert",
					"LastName":        "One",
					"rudderOperation": "insert",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(1),
				},
			},
			{
				Message: map[string]interface{}{
					"Email":           "update1@example.com",
					"FirstName":       "Update",
					"LastName":        "One",
					"rudderOperation": "update",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(2),
				},
			},
			{
				Message: map[string]interface{}{
					"Email":           "delete1@example.com",
					"FirstName":       "Delete",
					"LastName":        "One",
					"rudderOperation": "delete",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(3),
				},
			},
		}

		for _, job := range multiOpData {
			jobBytes, _ := jsonrs.Marshal(job)
			tempFileMulti.Write(jobBytes)
			tempFileMulti.Write([]byte("\n"))
		}
		tempFileMulti.Close()

		jobCalls := 0
		var capturedCSVPaths []string
		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				require.Equal(t, "Lead", objectName)
				require.Contains(t, []string{"insert", "update", "delete"}, operation)
				jobCalls++
				return fmt.Sprintf("sf-job-%d", jobCalls), nil
			},
			UploadDataFunc: func(jobID, csvFilePath string) *APIError {
				require.Contains(t, jobID, "sf-job-")
				capturedCSVPaths = append(capturedCSVPaths, csvFilePath)
				return nil
			},
			CloseJobFunc: func(jobID string) *APIError {
				require.Contains(t, jobID, "sf-job-")
				return nil
			},
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			config:          DestinationConfig{Operation: "insert"}, // Default fallback
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-multi",
			},
			FileName:        tempFileMulti.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		require.Equal(t, 3, result.ImportingCount, "All 3 jobs should be importing")
		require.Equal(t, 0, result.FailedCount, "No jobs should have failed")
		require.NotNil(t, result.ImportingParameters)

		var params struct {
			Jobs []SalesforceJobInfo `json:"jobs"`
		}
		err = jsonrs.Unmarshal(result.ImportingParameters, &params)
		require.NoError(t, err)
		require.Len(t, params.Jobs, 3, "Should have created 3 separate Salesforce jobs")
		require.Equal(t, 3, jobCalls, "Should have called CreateJob 3 times")
		require.Len(t, capturedCSVPaths, 3)
		for _, csvPath := range capturedCSVPaths {
			_, err := os.Stat(csvPath)
			require.True(t, os.IsNotExist(err), "CSV file should be cleaned up")
		}
	})

	t.Run("upload with API error", func(t *testing.T) {
		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				return "", &APIError{
					StatusCode: 500,
					Message:    "Internal Server Error",
					Category:   "ServerError",
				}
			},
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			config:          DestinationConfig{Operation: "insert"},
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
			},
			FileName:        tempFile.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		require.Equal(t, 0, result.ImportingCount)
		require.Greater(t, result.FailedCount, 0)
		require.Contains(t, result.FailedReason, "All operations failed")
	})

	t.Run("upload with rate limit error", func(t *testing.T) {
		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				return "", &APIError{
					StatusCode: 429,
					Message:    "Rate limit exceeded",
					Category:   "RateLimit",
				}
			},
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			config:          DestinationConfig{Operation: "insert"},
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
			},
			FileName:        tempFile.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		require.Equal(t, 0, result.ImportingCount)
		require.Greater(t, result.FailedCount, 0)
		require.Contains(t, result.FailedReason, "All operations failed")
	})
}

func TestSalesforceBulk_Poll(t *testing.T) {
	testCases := []struct {
		name           string
		jobStatus      *JobResponse
		setupMock      func(*MockSalesforceAPIService)
		expectedStatus common.PollStatusResponse
	}{
		{
			name: "job complete - all success",
			setupMock: func(mock *MockSalesforceAPIService) {
				mock.GetJobStatusFunc = func(jobID string) (*JobResponse, *APIError) {
					return &JobResponse{
						ID:                     "job-123",
						State:                  "JobComplete",
						NumberRecordsProcessed: 100,
						NumberRecordsFailed:    0,
					}, nil
				}
			},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  false,
			},
		},
		{
			name: "job in progress",
			setupMock: func(mock *MockSalesforceAPIService) {
				mock.GetJobStatusFunc = func(jobID string) (*JobResponse, *APIError) {
					return &JobResponse{
						ID:    "job-456",
						State: "InProgress",
					}, nil
				}
			},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				InProgress: true,
			},
		},
		{
			name: "job complete with failures",
			setupMock: func(mock *MockSalesforceAPIService) {
				mock.GetJobStatusFunc = func(jobID string) (*JobResponse, *APIError) {
					return &JobResponse{
						ID:                     "job-789",
						State:                  "JobComplete",
						NumberRecordsProcessed: 100,
						NumberRecordsFailed:    10,
					}, nil
				}
			},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  true,
			},
		},
		{
			name: "job failed",
			setupMock: func(mock *MockSalesforceAPIService) {
				mock.GetJobStatusFunc = func(jobID string) (*JobResponse, *APIError) {
					return &JobResponse{
						ID:           "job-failed",
						State:        "Failed",
						ErrorMessage: "Invalid object type",
					}, nil
				}
			},
			expectedStatus: common.PollStatusResponse{
				StatusCode: 200,
				Complete:   true,
				HasFailed:  true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockAPI := &MockSalesforceAPIService{}
			tc.setupMock(mockAPI)

			uploader := &SalesforceBulkUploader{
				apiService: mockAPI,
			}

			pollInput := common.AsyncPoll{ImportId: "job-456"}
			result := uploader.Poll(pollInput)

			require.Equal(t, tc.expectedStatus.StatusCode, result.StatusCode)
			require.Equal(t, tc.expectedStatus.Complete, result.Complete)
			require.Equal(t, tc.expectedStatus.InProgress, result.InProgress)
			require.Equal(t, tc.expectedStatus.HasFailed, result.HasFailed)
		})
	}
}

func TestSalesforceBulk_GetUploadStats(t *testing.T) {
	t.Run("successful stats retrieval with failures", func(t *testing.T) {
		mockAPI := &MockSalesforceAPIService{
			GetFailedRecordsFunc: func(jobID string) ([]map[string]string, *APIError) {
				return []map[string]string{}, nil
			},
			GetSuccessfulRecordsFunc: func(jobID string) ([]map[string]string, *APIError) {
				return []map[string]string{}, nil
			},
		}

		uploader := &SalesforceBulkUploader{
			apiService:      mockAPI,
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`{"jobs":[{"id":"test-job-123","operation":"upsert","headers":["Email","FirstName","LastName"]}]}`),
			ImportingList: []*jobsdb.JobT{
				{JobID: 1},
				{JobID: 2},
			},
		})

		require.Equal(t, 200, result.StatusCode)
		require.NotNil(t, result.Metadata)
	})

	t.Run("handles API error fetching failed records", func(t *testing.T) {
		mockAPI := &MockSalesforceAPIService{
			GetFailedRecordsFunc: func(jobID string) ([]map[string]string, *APIError) {
				return nil, &APIError{
					StatusCode: 500,
					Message:    "Server error",
					Category:   "ServerError",
				}
			},
			GetSuccessfulRecordsFunc: func(jobID string) ([]map[string]string, *APIError) {
				return nil, nil
			},
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`{"jobs":[{"id":"test-job-123","operation":"delete","headers":["Email"]}]}`),
		})

		require.Equal(t, 200, result.StatusCode)
		require.NotNil(t, result.Metadata)
	})

	t.Run("handles invalid parameters", func(t *testing.T) {
		uploader := &SalesforceBulkUploader{
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			Parameters: json.RawMessage(`invalid json`),
		})

		require.Equal(t, 500, result.StatusCode)
		require.Contains(t, result.Error, "Failed to parse parameters")
	})
}

func TestSalesforceBulk_handleAPIError(t *testing.T) {
	testCases := []struct {
		name            string
		apiError        *APIError
		failedJobIDs    []int64
		importingJobIDs []int64
		expectedAbort   bool
		expectedFailed  bool
	}{
		{
			name: "token expired - should retry",
			apiError: &APIError{
				StatusCode: 401,
				Message:    "Token expired",
				Category:   "RefreshToken",
			},
			failedJobIDs:    []int64{1, 2},
			importingJobIDs: []int64{3, 4, 5},
			expectedFailed:  true,
			expectedAbort:   false,
		},
		{
			name: "rate limit - should retry",
			apiError: &APIError{
				StatusCode: 429,
				Message:    "Rate limit exceeded",
				Category:   "RateLimit",
			},
			failedJobIDs:    []int64{},
			importingJobIDs: []int64{1, 2},
			expectedFailed:  true,
			expectedAbort:   false,
		},
		{
			name: "bad request - should abort",
			apiError: &APIError{
				StatusCode: 400,
				Message:    "Invalid field name",
				Category:   "BadRequest",
			},
			failedJobIDs:    []int64{1},
			importingJobIDs: []int64{2, 3},
			expectedFailed:  false,
			expectedAbort:   true,
		},
		{
			name: "server error - should retry",
			apiError: &APIError{
				StatusCode: 500,
				Message:    "Internal server error",
				Category:   "ServerError",
			},
			failedJobIDs:    []int64{},
			importingJobIDs: []int64{1},
			expectedFailed:  true,
			expectedAbort:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uploader := &SalesforceBulkUploader{}

			result := uploader.handleAPIError(
				tc.apiError,
				tc.failedJobIDs,
				tc.importingJobIDs,
				"test-dest-id",
			)

			totalJobs := len(tc.failedJobIDs) + len(tc.importingJobIDs)

			if tc.expectedAbort {
				require.Len(t, result.AbortJobIDs, totalJobs)
				require.NotEmpty(t, result.AbortReason)
				require.Equal(t, totalJobs, result.AbortCount)
			}

			if tc.expectedFailed {
				require.Len(t, result.FailedJobIDs, totalJobs)
				require.NotEmpty(t, result.FailedReason)
				require.Equal(t, totalJobs, result.FailedCount)
			}

			require.Equal(t, "test-dest-id", result.DestinationID)
		})
	}
}

func TestSalesforceBulk_NewManager(t *testing.T) {
	t.Run("successful manager creation", func(t *testing.T) {
		destination := &backendconfig.DestinationT{
			ID:          "test-dest-123",
			WorkspaceID: "test-workspace-456",
			Config: map[string]interface{}{
				"rudderAccountId": "test-account-789",
				"operation":       "insert",
			},
		}

		mockBackendConfig := NewMockBackendConfig()

		manager, err := NewManager(
			logger.NOP,
			stats.NOP,
			destination,
			mockBackendConfig,
		)

		require.NoError(t, err)
		require.NotNil(t, manager)

		uploader, ok := manager.(*SalesforceBulkUploader)
		require.True(t, ok)
		require.Equal(t, "insert", uploader.config.Operation)
		require.Equal(t, "v62.0", uploader.config.APIVersion)
	})

	t.Run("invalid config", func(t *testing.T) {
		destination := &backendconfig.DestinationT{
			Config: map[string]interface{}{
				"operation": "insert",
			},
		}

		mockBackendConfig := NewMockBackendConfig()

		manager, err := NewManager(
			logger.NOP,
			stats.NOP,
			destination,
			mockBackendConfig,
		)

		require.Error(t, err)
		require.Nil(t, manager)
		require.Contains(t, err.Error(), "rudderAccountId")
	})
}

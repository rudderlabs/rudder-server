package salesforcebulk

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func TestSalesforceBulk_Transform(t *testing.T) {
	testCases := []struct {
		name     string
		job      *jobsdb.JobT
		expected string
		wantErr  bool
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

			// Verify it's valid JSON with message and metadata
			var parsed common.AsyncJob
			err = json.Unmarshal([]byte(result), &parsed)
			require.NoError(t, err)
			require.NotNil(t, parsed.Message)
			require.NotNil(t, parsed.Metadata)
			require.Equal(t, float64(tc.job.JobID), parsed.Metadata["job_id"])
		})
	}
}

func TestSalesforceBulk_parseDestinationConfig(t *testing.T) {
	testCases := []struct {
		name        string
		destination *backendconfig.DestinationT
		expected    DestinationConfig
		wantErr     bool
		errorMsg    string
	}{
		{
			name: "valid config with all fields",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-123",
					"operation":       "insert",
					"apiVersion":      "v57.0",
				},
			},
			expected: DestinationConfig{
				RudderAccountID: "test-account-123",
				Operation:       "insert",
				APIVersion:      "v57.0",
			},
			wantErr: false,
		},
		{
			name: "valid config with defaults",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-456",
				},
			},
			expected: DestinationConfig{
				RudderAccountID: "test-account-456",
				Operation:       "",
				APIVersion:      "",
			},
			wantErr: false,
		},
		{
			name: "missing rudderAccountId",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"operation": "insert",
				},
			},
			wantErr:  true,
			errorMsg: "rudderAccountId is required",
		},
		{
			name: "invalid operation",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-789",
					"operation":       "invalid_op",
				},
			},
			wantErr:  true,
			errorMsg: "invalid operation",
		},
		{
			name: "upsert operation",
			destination: &backendconfig.DestinationT{
				Config: map[string]interface{}{
					"rudderAccountId": "test-account-upsert",
					"operation":       "upsert",
				},
			},
			expected: DestinationConfig{
				RudderAccountID: "test-account-upsert",
				Operation:       "upsert",
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseDestinationConfig(tc.destination)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected.RudderAccountID, result.RudderAccountID)
			require.Equal(t, tc.expected.Operation, result.Operation)
			if tc.expected.APIVersion != "" {
				require.Equal(t, tc.expected.APIVersion, result.APIVersion)
			}
		})
	}
}

func TestSalesforceBulk_extractObjectInfo(t *testing.T) {
	testCases := []struct {
		name     string
		jobs     []common.AsyncJob
		expected *ObjectInfo
		wantErr  bool
		errorMsg string
	}{
		{
			name: "valid externalId with Contact object",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "test@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(1),
						"externalId": []interface{}{
							map[string]interface{}{
								"type":           "Salesforce-Contact",
								"id":             "test@example.com",
								"identifierType": "Email",
							},
						},
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Contact",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name: "valid externalId with Lead object",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "lead@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(2),
						"externalId": []interface{}{
							map[string]interface{}{
								"type":           "Salesforce-Lead",
								"id":             "lead@example.com",
								"identifierType": "Email",
							},
						},
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name:     "empty jobs array",
			jobs:     []common.AsyncJob{},
			wantErr:  true,
			errorMsg: "no jobs to process",
		},
		{
			name: "missing externalId - falls back to config",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "test@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(3),
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead", // Defaults to Lead
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name: "empty externalId array",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{},
					Metadata: map[string]interface{}{
						"externalId": []interface{}{},
					},
				},
			},
			wantErr:  true,
			errorMsg: "at least one element",
		},
		{
			name: "event stream without externalId - uses config",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "stream@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(10),
						// No externalId - event stream
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Contact",
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
		{
			name: "event stream with default object type",
			jobs: []common.AsyncJob{
				{
					Message: map[string]interface{}{
						"Email": "default@example.com",
					},
					Metadata: map[string]interface{}{
						"job_id": float64(11),
					},
				},
			},
			expected: &ObjectInfo{
				ObjectType:      "Lead", // Should default to Lead
				ExternalIDField: "Email",
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Use config for event stream tests, empty for RETL tests
			testConfig := DestinationConfig{}
			if tc.name == "event stream without externalId - uses config" {
				testConfig.ObjectType = "Contact"
			}

			result, err := extractObjectInfo(tc.jobs, testConfig)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected.ObjectType, result.ObjectType)
			require.Equal(t, tc.expected.ExternalIDField, result.ExternalIDField)
		})
	}
}

func TestSalesforceBulk_createCSVFile(t *testing.T) {
	testCases := []struct {
		name              string
		jobs              []common.AsyncJob
		expectedInserted  int
		expectedOverflow  int
		wantErr           bool
	}{
		{
			name: "create CSV with valid jobs",
			jobs: []common.AsyncJob{
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
			},
			expectedInserted: 2,
			expectedOverflow: 0,
			wantErr:          false,
		},
		{
			name:     "empty jobs array",
			jobs:     []common.AsyncJob{},
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataHashToJobID := make(map[string]int64)
			csvFilePath, insertedJobIDs, overflowedJobIDs, err := createCSVFile(
				"test-dest-123",
				tc.jobs,
				dataHashToJobID,
			)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, csvFilePath)
			require.Len(t, insertedJobIDs, tc.expectedInserted)
			require.Len(t, overflowedJobIDs, tc.expectedOverflow)

			// Verify CSV file was created
			_, err = os.Stat(csvFilePath)
			require.NoError(t, err)

			// Cleanup
			t.Cleanup(func() {
				require.NoError(t, os.Remove(csvFilePath))
			})

			// Verify hash tracking
			require.Len(t, dataHashToJobID, tc.expectedInserted)
		})
	}
}

func TestSalesforceBulk_calculateHashCode(t *testing.T) {
	testCases := []struct {
		name     string
		row      []string
		expected string
	}{
		{
			name:     "simple row",
			row:      []string{"test@example.com", "John", "Doe"},
			expected: calculateHashCode([]string{"test@example.com", "John", "Doe"}),
		},
		{
			name:     "empty row",
			row:      []string{},
			expected: calculateHashCode([]string{}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := calculateHashCode(tc.row)

			require.NotEmpty(t, result)
			require.Equal(t, tc.expected, result)

			// Hash should be deterministic
			result2 := calculateHashCode(tc.row)
			require.Equal(t, result, result2)
		})
	}
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
			jobStatus: &JobResponse{
				ID:                     "job-123",
				State:                  "JobComplete",
				NumberRecordsProcessed: 100,
				NumberRecordsFailed:    0,
			},
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

			// Get job ID from test case
			jobID := "job-456" // default
			if tc.jobStatus != nil {
				jobID = tc.jobStatus.ID
			}
			pollInput := common.AsyncPoll{ImportId: jobID}

			result := uploader.Poll(pollInput)

			require.Equal(t, tc.expectedStatus.StatusCode, result.StatusCode)
			require.Equal(t, tc.expectedStatus.Complete, result.Complete)
			require.Equal(t, tc.expectedStatus.InProgress, result.InProgress)
			require.Equal(t, tc.expectedStatus.HasFailed, result.HasFailed)
		})
	}
}

func TestSalesforceBulk_handleAPIError(t *testing.T) {
	testCases := []struct {
		name           string
		apiError       *APIError
		failedJobIDs   []int64
		importingJobIDs []int64
		expectedAbort  bool
		expectedFailed bool
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

		// Verify it's the correct type
		uploader, ok := manager.(*SalesforceBulkUploader)
		require.True(t, ok)
		require.Equal(t, "insert", uploader.config.Operation)
		require.Equal(t, "v57.0", uploader.config.APIVersion) // Should default to v57.0
	})

	t.Run("invalid config", func(t *testing.T) {
		destination := &backendconfig.DestinationT{
			Config: map[string]interface{}{
				// Missing rudderAccountId
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


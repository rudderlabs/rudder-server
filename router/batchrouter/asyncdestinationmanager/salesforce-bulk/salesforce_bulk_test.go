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
		name             string
		job              *jobsdb.JobT
		wantErr          bool
		defaultOperation string
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
			wantErr:          false,
			defaultOperation: "update",
		},
		{
			name: "transform with empty body.JSON",
			job: &jobsdb.JobT{
				JobID:        456,
				EventPayload: []byte(`{"body": {"JSON": {}}}`),
			},
			wantErr:          false,
			defaultOperation: "insert",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uploader := &SalesforceBulkUploader{config: DestinationConfig{Operation: tc.defaultOperation}}
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

			require.Equal(t, tc.defaultOperation, parsed.Message["rudderOperation"])
		})
	}
}

func TestSalesforceBulk_TransformExternalIDHandling(t *testing.T) {
	t.Parallel()

	type expectedExternalID struct {
		Type           string
		ID             string
		IdentifierType string
	}

	testCases := []struct {
		name                  string
		payload               string
		defaultOperation      string
		expectedOperation     string
		expectedExternalIDs   []expectedExternalID
		expectedIdentifierKey string
		expectedIdentifierVal string
		expectedObjectType    string
		expectedExternalField string
	}{
		{
			name: "no externalId falls back to default operation",
			payload: `{
                                "body": {
                                        "JSON": {
                                                "Email": "no-external@example.com"
                                        }
                                }
                        }`,
			defaultOperation:  "update",
			expectedOperation: "update",
		},
		{
			name: "type only externalId uses configured default",
			payload: `{
                               "body": {
                                       "JSON": {
                                               "Email": "type-only@example.com",
                                               "context": {
                                                       "externalId": [
                                                               {"type": "Salesforce-Lead"}
                                                       ]
                                               }
                                       }
                               }
                       }`,
			defaultOperation:  "upsert",
			expectedOperation: "upsert",
			expectedExternalIDs: []expectedExternalID{
				{Type: "Salesforce-Lead", ID: "", IdentifierType: ""},
			},
		},
		{
			name: "identifierType without id infers upsert",
			payload: `{
                               "body": {
                                       "JSON": {
                                               "Email": "identifier-only@example.com",
                                               "context": {
                                                       "externalId": [
                                                               {"type": "Salesforce-Lead", "identifierType": "External_Id__c"}
                                                       ]
                                               }
                                       }
                               }
                       }`,
			defaultOperation:  "insert",
			expectedOperation: "upsert",
			expectedExternalIDs: []expectedExternalID{
				{Type: "Salesforce-Lead", ID: "", IdentifierType: "External_Id__c"},
			},
		},
		{
			name: "id without identifierType defaults to Id and upsert",
			payload: `{
                                "body": {
                                        "JSON": {
                                                "Email": "id-only@example.com",
                                                "externalId": [
                                                        {"type": "Salesforce-Lead", "id": "001ABC"}
                                                ]
                                        }
                                }
                        }`,
			defaultOperation:      "insert",
			expectedOperation:     "upsert",
			expectedExternalIDs:   []expectedExternalID{{Type: "Salesforce-Lead", ID: "001ABC", IdentifierType: "Id"}},
			expectedIdentifierKey: "Id",
			expectedIdentifierVal: "001ABC",
		},
		{
			name: "multiple externalIds prefer identifier from entry with id",
			payload: `{
                                "body": {
                                        "JSON": {
                                                "Email": "multi@example.com",
                                                "context": {
                                                        "externalId": [
                                                                {"type": "Salesforce-Lead"}
                                                        ]
                                                },
                                                "externalId": [
                                                        {"type": "Salesforce-Account", "id": "ACC123", "identifierType": "External_Id__c"}
                                                ]
                                        }
                                }
                        }`,
			defaultOperation:  "delete",
			expectedOperation: "upsert",
			expectedExternalIDs: []expectedExternalID{
				{Type: "Salesforce-Lead", ID: "", IdentifierType: ""},
				{Type: "Salesforce-Account", ID: "ACC123", IdentifierType: "External_Id__c"},
			},
			expectedIdentifierKey: "External_Id__c",
			expectedIdentifierVal: "ACC123",
			expectedObjectType:    "Account",
			expectedExternalField: "External_Id__c",
		},
		{
			name: "single payload externalId array mixes types",
			payload: `{
                                "body": {
                                        "JSON": {
                                                "Name": "mixed-array", 
                                                "externalId": [
                                                        {"type": "Salesforce-Contact", "identifierType": "Email"},
                                                        {"type": "Salesforce-Account", "id": "ACC789", "identifierType": "External_Id__c"}
                                                ]
                                        }
                                }
                        }`,
			defaultOperation:  "insert",
			expectedOperation: "upsert",
			expectedExternalIDs: []expectedExternalID{
				{Type: "Salesforce-Contact", ID: "", IdentifierType: "Email"},
				{Type: "Salesforce-Account", ID: "ACC789", IdentifierType: "External_Id__c"},
			},
			expectedIdentifierKey: "External_Id__c",
			expectedIdentifierVal: "ACC789",
			expectedObjectType:    "Account",
			expectedExternalField: "External_Id__c",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			job := &jobsdb.JobT{
				JobID:        789,
				EventPayload: []byte(tc.payload),
			}

			uploader := &SalesforceBulkUploader{config: DestinationConfig{Operation: tc.defaultOperation}}
			result, err := uploader.Transform(job)
			require.NoError(t, err)

			var parsed common.AsyncJob
			err = json.Unmarshal([]byte(result), &parsed)
			require.NoError(t, err)

			expectedMessage := messageFromPayload(t, tc.payload)
			expectedMessage["rudderOperation"] = tc.expectedOperation

			if tc.expectedIdentifierKey != "" {
				expectedMessage[tc.expectedIdentifierKey] = tc.expectedIdentifierVal
				require.Equal(t, tc.expectedIdentifierVal, parsed.Message[tc.expectedIdentifierKey])
			} else {
				require.NotContains(t, parsed.Message, "Id")
			}

			require.Equal(t, expectedMessage, parsed.Message)

			externalIDRaw, ok := parsed.Metadata["externalId"]
			if len(tc.expectedExternalIDs) == 0 {
				require.False(t, ok)
				return
			}

			require.True(t, ok)
			externalIDSlice, ok := externalIDRaw.([]interface{})
			require.True(t, ok)
			require.Len(t, externalIDSlice, len(tc.expectedExternalIDs))

			for idx, expected := range tc.expectedExternalIDs {
				entry, ok := externalIDSlice[idx].(map[string]interface{})
				require.True(t, ok)
				require.Equal(t, expected.Type, entry["type"])
				require.Equal(t, expected.ID, entry["id"])
				require.Equal(t, expected.IdentifierType, entry["identifierType"])
			}

			if tc.expectedObjectType != "" {
				objectInfo, err := extractFromVDM(externalIDRaw, parsed.Message)
				require.NoError(t, err)
				require.Equal(t, tc.expectedObjectType, objectInfo.ObjectType)
				require.Equal(t, tc.expectedExternalField, objectInfo.ExternalIDField)
			}
		})
	}
}

func messageFromPayload(t *testing.T, payload string) map[string]interface{} {
	t.Helper()

	var envelope struct {
		Body struct {
			JSON map[string]interface{} `json:"JSON"`
		} `json:"body"`
	}

	require.NoError(t, json.Unmarshal([]byte(payload), &envelope))

	if envelope.Body.JSON == nil {
		return map[string]interface{}{}
	}

	return envelope.Body.JSON
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

	t.Run("mixed object types split into separate Salesforce jobs", func(t *testing.T) {
		tempFileMixed, err := os.CreateTemp("", "test_mixed_object_*.json")
		require.NoError(t, err)
		defer os.Remove(tempFileMixed.Name())

		mixedJobs := []common.AsyncJob{
			{
				Message: map[string]interface{}{
					"Email":           "lead@example.com",
					"rudderOperation": "upsert",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(101),
					"externalId": []interface{}{
						map[string]interface{}{
							"type":           "Salesforce-Lead",
							"id":             "lead@example.com",
							"identifierType": "Email",
						},
					},
				},
			},
			{
				Message: map[string]interface{}{
					"External_Id__c":  "ACC-123",
					"rudderOperation": "upsert",
				},
				Metadata: map[string]interface{}{
					"job_id": float64(102),
					"externalId": []interface{}{
						map[string]interface{}{
							"type":           "Salesforce-Account",
							"id":             "ACC-123",
							"identifierType": "External_Id__c",
						},
					},
				},
			},
		}

		for _, job := range mixedJobs {
			jobBytes, _ := jsonrs.Marshal(job)
			tempFileMixed.Write(jobBytes)
			tempFileMixed.Write([]byte("\n"))
		}
		tempFileMixed.Close()

		type createCall struct {
			Object    string
			Field     string
			Operation string
		}

		var calls []createCall
		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				calls = append(calls, createCall{Object: objectName, Field: externalIDField, Operation: operation})
				return fmt.Sprintf("mixed-job-%d", len(calls)), nil
			},
			UploadDataFunc: func(jobID, csvFilePath string) *APIError { return nil },
			CloseJobFunc:   func(jobID string) *APIError { return nil },
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			config:          DestinationConfig{Operation: "upsert"},
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{ID: "mixed-dest"},
			FileName:    tempFileMixed.Name(),
		})

		require.Equal(t, 2, len(calls))
		require.Equal(t, "Lead", calls[0].Object)
		require.Equal(t, "Email", calls[0].Field)
		require.Equal(t, "upsert", calls[0].Operation)
		require.Equal(t, "Account", calls[1].Object)
		require.Equal(t, "External_Id__c", calls[1].Field)
		require.Equal(t, "upsert", calls[1].Operation)
		require.Equal(t, 2, result.ImportingCount)
		require.Equal(t, 0, result.FailedCount)
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

			pollInput := common.AsyncPoll{ImportId: `{"jobs":[{"id":"job-456","operation":"insert","headers":["Email"]}]}`}
			result := uploader.Poll(pollInput)

			require.Equal(t, tc.expectedStatus.StatusCode, result.StatusCode)
			require.Equal(t, tc.expectedStatus.Complete, result.Complete)
			require.Equal(t, tc.expectedStatus.InProgress, result.InProgress)
			require.Equal(t, tc.expectedStatus.HasFailed, result.HasFailed)
		})
	}
}

func TestSalesforceBulk_Upload_ImportIdPresent(t *testing.T) {
	t.Run("ImportingParameters includes importId for batch router polling", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_importid_*.json")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		testData := []common.AsyncJob{
			{
				Message:  map[string]interface{}{"Email": "test@example.com"},
				Metadata: map[string]interface{}{"job_id": float64(1)},
			},
		}

		for _, job := range testData {
			jobBytes, _ := jsonrs.Marshal(job)
			tempFile.Write(jobBytes)
			tempFile.Write([]byte("\n"))
		}
		tempFile.Close()

		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				return "sf-job-123", nil
			},
			UploadDataFunc: func(jobID, csvFilePath string) *APIError { return nil },
			CloseJobFunc:   func(jobID string) *APIError { return nil },
		}

		uploader := &SalesforceBulkUploader{
			logger:          logger.NOP,
			apiService:      mockAPI,
			config:          DestinationConfig{Operation: "insert"},
			dataHashToJobID: make(map[string][]int64),
		}

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{ID: "test-dest"},
			FileName:    tempFile.Name(),
		})

		var params map[string]interface{}
		err = jsonrs.Unmarshal(result.ImportingParameters, &params)
		require.NoError(t, err)
		require.Contains(t, params, "importId", "ImportingParameters must have importId for batch router")
		require.NotEmpty(t, params["importId"], "importId should not be empty")
	})
}

func TestSalesforceBulk_Upload_OverflowHandling(t *testing.T) {
	t.Run("creates multiple Salesforce jobs when CSV exceeds 100MB", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "test_overflow_*.json")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		largeData := make([]byte, 200*1024)
		for i := 0; i < 200*1024; i++ {
			largeData[i] = 'x'
		}

		var jobCount int
		testData := make([]common.AsyncJob, 0)
		for i := 1; i <= 600; i++ {
			testData = append(testData, common.AsyncJob{
				Message: map[string]interface{}{
					"Email":     fmt.Sprintf("user%d@example.com", i),
					"LargeData": string(largeData),
				},
				Metadata: map[string]interface{}{"job_id": float64(i)},
			})
		}

		for _, job := range testData {
			jobBytes, _ := jsonrs.Marshal(job)
			tempFile.Write(jobBytes)
			tempFile.Write([]byte("\n"))
		}
		tempFile.Close()

		mockAPI := &MockSalesforceAPIService{
			CreateJobFunc: func(objectName, operation, externalIDField string) (string, *APIError) {
				jobCount++
				return fmt.Sprintf("sf-job-%d", jobCount), nil
			},
			UploadDataFunc: func(jobID, csvFilePath string) *APIError {
				return nil
			},
			CloseJobFunc: func(jobID string) *APIError {
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
			Destination: &backendconfig.DestinationT{ID: "test-dest"},
			FileName:    tempFile.Name(),
		})

		require.Greater(t, jobCount, 1, "Should create multiple Salesforce jobs for overflow")
		require.Equal(t, 600, result.ImportingCount, "All jobs should be imported")
		require.Equal(t, 0, result.FailedCount, "No jobs should fail due to overflow")

		var params struct {
			Jobs []SalesforceJobInfo `json:"jobs"`
		}
		err = jsonrs.Unmarshal(result.ImportingParameters, &params)
		require.NoError(t, err)
		require.Len(t, params.Jobs, jobCount, "Should have job info for each Salesforce job")
	})
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

package marketobulkupload_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockmarketoservice "github.com/rudderlabs/rudder-server/mocks/router/marketo_bulk_upload"
	mbu "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
)

func TestMarketoBulkUploader_Upload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPIService := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	testLogger := logger.NewLogger().Child("marketo-bulk-upload-test")

	destConfg := mbu.MarketoConfig{
		MunchkinId:         "123-ABC-456",
		DeduplicationField: "email",
		FieldsMapping: map[string]string{
			"email":     "email",
			"firstName": "firstName",
			"lastName":  "lastName",
		},
	}

	uploader := mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		DestinationName:   "MARKETO_BULK_UPLOAD",
		DestinationConfig: destConfg,
		Logger:            testLogger,
		StatsFactory:      stats.NOP,
		APIService:        mockAPIService,
	})
	// Create a temporary file with test data
	tempFile, err := os.CreateTemp("", "test_upload_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	testData := []common.AsyncJob{
		{
			Message: map[string]interface{}{
				"email":     "test1@example.com",
				"firstName": "Test1",
				"lastName":  "User1",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(1),
			},
		},
		{
			Message: map[string]interface{}{
				"email":     "test2@example.com",
				"firstName": "Test2",
				"lastName":  "User2",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(2),
			},
		},
	}

	for _, job := range testData {
		jobBytes, _ := json.Marshal(job)
		if _, err := tempFile.Write(jobBytes); err != nil {
			t.Fatal(err)
		}
		if _, err := tempFile.Write([]byte("\n")); err != nil {
			t.Fatal(err)
		}
	}
	tempFile.Close()

	t.Run("successful upload", func(t *testing.T) {
		mockAPIService.EXPECT().
			ImportLeads(gomock.Any(), "email").
			Return("test-import-123", nil)

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "MARKETO_BULK_UPLOAD",
				},
			},
			FileName:        tempFile.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		assert.Equal(t, 2, result.ImportingCount)
		assert.Equal(t, 0, result.FailedCount)
		assert.NotNil(t, result.ImportingParameters)
	})

	t.Run("upload with API error", func(t *testing.T) {
		mockAPIService.EXPECT().
			ImportLeads(gomock.Any(), "email").
			Return("", &mbu.APIError{
				StatusCode: 500,
				Category:   "Retryable",
				Message:    "Internal Server Error",
			})

		result := uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "MARKETO_BULK_UPLOAD",
				},
			},
			FileName:        tempFile.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		assert.Equal(t, 0, result.ImportingCount)
		assert.Greater(t, result.FailedCount, 0)
		assert.Contains(t, result.FailedReason, "BRT: Error in Uploading File: Internal Server Error")
	})
}

func TestMarketoBulkUploader_Poll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPIService := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	testLogger := logger.NewLogger().Child("marketo-bulk-upload-test")

	destConfg := mbu.MarketoConfig{
		MunchkinId: "123-ABC-456",
	}

	uploader := mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		DestinationName:   "MARKETO_BULK_UPLOAD",
		DestinationConfig: destConfg,
		Logger:            testLogger,
		StatsFactory:      stats.NOP,
		APIService:        mockAPIService,
	})

	t.Run("polling complete status", func(t *testing.T) {
		marketoResponse := &mbu.MarketoResponse{
			Success: true,
			Result: []mbu.Result{
				{
					ImportID:             "test-import-123",
					Status:               "Complete",
					NumOfRowsFailed:      0,
					NumOfRowsWithWarning: 0,
				},
			},
		}

		mockAPIService.EXPECT().
			PollImportStatus("test-import-123").
			Return(marketoResponse, nil)

		result := uploader.Poll(common.AsyncPoll{
			ImportId: "test-import-123",
		})

		assert.True(t, result.Complete)
		assert.Equal(t, 200, result.StatusCode)
		assert.False(t, result.HasFailed)
		assert.False(t, result.HasWarning)
	})

	t.Run("polling with failures", func(t *testing.T) {
		marketoResponse := &mbu.MarketoResponse{
			Success: true,
			Result: []mbu.Result{
				{
					ImportID:             "test-import-123",
					Status:               "Complete",
					NumOfRowsFailed:      2,
					NumOfRowsWithWarning: 1,
				},
			},
		}

		mockAPIService.EXPECT().
			PollImportStatus("test-import-123").
			Return(marketoResponse, nil)

		result := uploader.Poll(common.AsyncPoll{
			ImportId: "test-import-123",
		})

		assert.True(t, result.Complete)
		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.HasFailed)
		assert.True(t, result.HasWarning)
	})
}

func TestMarketoBulkUploader_GetUploadStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPIService := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	testLogger := logger.NewLogger().Child("marketo-bulk-upload-test")

	// Create a temporary file for the upload setup
	tempFile, err := os.CreateTemp("", "test_upload_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())

	// Create test data that matches the field mapping
	testData := []common.AsyncJob{
		{
			Message: map[string]interface{}{
				"email":     "test1@example.com",
				"firstName": "Test1",
				"lastName":  "User1",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(1),
			},
		},
		{
			Message: map[string]interface{}{
				"email":     "test2@example.com",
				"firstName": "Test2",
				"lastName":  "User2",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(2),
			},
		},
		{
			Message: map[string]interface{}{
				"email":     "test3@example.com",
				"firstName": "Test3",
				"lastName":  "User3",
			},
			Metadata: map[string]interface{}{
				"job_id": float64(3),
			},
		},
	}

	// Write test data to temp file
	for _, job := range testData {
		jobBytes, _ := json.Marshal(job)
		if _, err := tempFile.Write(jobBytes); err != nil {
			t.Fatal(err)
		}

		if _, err := tempFile.Write([]byte("\n")); err != nil {
			t.Fatal(err)
		}
	}
	tempFile.Close()

	destinationConfig := mbu.MarketoConfig{
		MunchkinId: "123-ABC-456",
		FieldsMapping: map[string]string{
			"email":     "email",
			"firstName": "firstName",
			"lastName":  "lastName",
		},
	}

	uploader := mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		DestinationName:   "MARKETO_BULK_UPLOAD",
		DestinationConfig: destinationConfig,
		Logger:            testLogger,
		StatsFactory:      stats.NOP,
		APIService:        mockAPIService,
	})

	t.Run("get upload stats with failures and warnings", func(t *testing.T) {
		// Mock successful upload to set up the state
		mockAPIService.EXPECT().
			ImportLeads(gomock.Any(), gomock.Any()).
			Return("test-import-123", nil).
			AnyTimes()

		mockAPIService.EXPECT().
			PollImportStatus("test-import-123").
			Return(&mbu.MarketoResponse{
				Success: true,
				Result: []mbu.Result{
					{
						Status:               "Complete",
						ImportID:             "test-import-123",
						NumOfLeadsProcessed:  1,
						NumOfRowsFailed:      1,
						NumOfRowsWithWarning: 1,
					},
				},
			}, nil).
			AnyTimes()

		// Do an initial upload to set up the internal state (csvHeaders and dataHashToJobId)
		uploader.Upload(&common.AsyncDestinationStruct{
			Destination: &backendconfig.DestinationT{
				ID: "test-dest-1",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "MARKETO_BULK_UPLOAD",
				},
			},
			FileName:        tempFile.Name(),
			FailedJobIDs:    []int64{},
			ImportingJobIDs: []int64{},
		})

		// Do a poll to set up the internal state (importId)
		uploader.Poll(common.AsyncPoll{
			ImportId: "test-import-123",
		})

		// Mock the API responses for failed jobs
		mockAPIService.EXPECT().
			GetLeadStatus(gomock.Eq("https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/failures.json")).
			Return([]map[string]string{
				{
					"email":                 "test1@example.com",
					"firstName":             "Test1",
					"lastName":              "User1",
					"Import Failure Reason": "Invalid email format",
				},
			}, nil)

		// Mock the API responses for warning jobs
		mockAPIService.EXPECT().
			GetLeadStatus(gomock.Eq("https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/warnings.json")).
			Return([]map[string]string{
				{
					"email":                 "test2@example.com",
					"firstName":             "Test2",
					"lastName":              "User2",
					"Import Warning Reason": "Duplicate record",
				},
			}, nil)

		importingList := []*jobsdb.JobT{
			{JobID: 1}, // Failed job
			{JobID: 2}, // Warning job
			{JobID: 3}, // Successful job
		}

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			ImportingList:        importingList,
			FailedJobParameters:  "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/failures.json",
			WarningJobParameters: "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/warnings.json",
			Parameters:           json.RawMessage(`{"importId":"test-import-123"}`),
		})

		assert.Equal(t, 200, result.StatusCode)
		assert.NotNil(t, result.Metadata)

		metadata := result.Metadata

		// Verify failed jobs
		assert.Contains(t, metadata.AbortedKeys, int64(1))
		assert.Equal(t, "Invalid email format", metadata.AbortedReasons[1])

		// Verify failed jobs from warnings
		assert.Contains(t, metadata.AbortedKeys, int64(2))
		assert.Equal(t, "Duplicate record", metadata.AbortedReasons[2])

		// Verify succeeded jobs
		assert.Contains(t, metadata.SucceededKeys, int64(3))

		// Verify counts
		assert.Equal(t, 2, len(metadata.AbortedKeys))
		assert.Equal(t, 1, len(metadata.SucceededKeys))
		assert.Equal(t, 0, len(metadata.WarningKeys))
	})

	t.Run("get upload stats with API error", func(t *testing.T) {
		// Mock successful upload to set up the state
		mockAPIService.EXPECT().
			PollImportStatus("test-import-123").
			Return(&mbu.MarketoResponse{
				Success: true,
				Result: []mbu.Result{
					{
						Status:               "Complete",
						ImportID:             "test-import-123",
						NumOfLeadsProcessed:  1,
						NumOfRowsFailed:      1,
						NumOfRowsWithWarning: 1,
					},
				},
			}, nil).
			AnyTimes()

		// Do a poll to set up the internal state (importId)
		uploader.Poll(common.AsyncPoll{
			ImportId: "test-import-123",
		})

		// Mock API error response
		mockAPIService.EXPECT().
			GetLeadStatus(gomock.Any()).
			Return(nil, &mbu.APIError{
				StatusCode: 500,
				Category:   "Retryable",
				Message:    "Internal Server Error",
			})

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			ImportingList:       []*jobsdb.JobT{{JobID: 1}},
			FailedJobParameters: "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/failures.json",
			Parameters:          json.RawMessage(`{"importId":"test-import-123"}`),
		})

		assert.Equal(t, 500, result.StatusCode)
		assert.Contains(t, result.Error, "Failed to fetch failed jobs")
	})
}

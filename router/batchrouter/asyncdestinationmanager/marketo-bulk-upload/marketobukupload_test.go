package marketobulkupload_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockmarketoservice "github.com/rudderlabs/rudder-server/mocks/router/marketo_bulk_upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	mbu "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
)

// Mirror the batch router's per-job metadata merge into importing status parameters.
func importingJobsFromUpload(t *testing.T, output common.AsyncUploadOutput) []*jobsdb.JobT {
	t.Helper()
	jobs := make([]*jobsdb.JobT, 0, len(output.ImportingJobIDs))
	for _, jobID := range output.ImportingJobIDs {
		params, ok := output.JobImportingParameters[jobID]
		require.True(t, ok, "missing metadata for job %d", jobID)
		statusParams, err := sjson.SetBytes(output.ImportingParameters, "metadata", params)
		require.NoError(t, err)
		jobs = append(jobs, &jobsdb.JobT{
			JobID: jobID,
			LastJobStatus: jobsdb.JobStatusT{
				Parameters: statusParams,
			},
		})
	}
	return jobs
}

func writeMarketoJobs(t *testing.T, jobs []common.AsyncJob) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "marketo-jobs-*.json")
	require.NoError(t, err)
	for _, job := range jobs {
		line, err := jsonrs.Marshal(job)
		require.NoError(t, err)
		_, err = file.Write(append(line, '\n'))
		require.NoError(t, err)
	}
	require.NoError(t, file.Close())
	return file.Name()
}

func marketoJob(id int64, fields map[string]any) common.AsyncJob {
	return common.AsyncJob{
		Message:  fields,
		Metadata: map[string]any{"job_id": float64(id)},
	}
}

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
			Message: map[string]any{
				"email":     "test1@example.com",
				"firstName": "Test1",
				"lastName":  "User1",
			},
			Metadata: map[string]any{
				"job_id": float64(1),
			},
		},
		{
			Message: map[string]any{
				"email":     "test2@example.com",
				"firstName": "Test2",
				"lastName":  "User2",
			},
			Metadata: map[string]any{
				"job_id": float64(2),
			},
		},
	}

	for _, job := range testData {
		jobBytes, _ := jsonrs.Marshal(job)
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

		result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
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

		result := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
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

func TestMarketoBulkUploader_PersistedUploadMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	// Leave room for short rows but not the longer middle row under the CSV limit.
	largeHeader := strings.Repeat("h", int(float64(10*1024*1024)*0.95)-100)
	uploader := mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		DestinationName: "MARKETO_BULK_UPLOAD",
		DestinationConfig: mbu.MarketoConfig{
			FieldsMapping: map[string]string{"email": "email", "unused": largeHeader},
		},
		Logger:       logger.NewLogger(),
		StatsFactory: stats.NOP,
		APIService:   api,
	})
	jobs := []common.AsyncJob{
		marketoJob(1, map[string]any{"email": "one@example.com"}),
		marketoJob(2, map[string]any{"email": strings.Repeat("x", 200)}),
		marketoJob(3, map[string]any{"email": "last@example.com"}),
	}
	file := writeMarketoJobs(t, jobs)
	api.EXPECT().ImportLeads(gomock.Any(), "").Return("import-123", nil)
	output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination: &backendconfig.DestinationT{
			ID: "marketo-persisted-metadata",
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: "MARKETO_BULK_UPLOAD",
			},
		},
		FileName: file,
	})

	require.Equal(t, []int64{1, 3}, output.ImportingJobIDs)
	require.Equal(t, []int64{2}, output.FailedJobIDs)
	require.Equal(t, "import-123", gjson.GetBytes(output.ImportingParameters, "importId").String())
	require.Equal(t, int64(output.ImportingCount), gjson.GetBytes(output.ImportingParameters, "importCount").Int())
	headers := gjson.GetBytes(output.ImportingParameters, "csvHeaders").Array()
	require.Len(t, headers, 2)
	require.ElementsMatch(t, []string{"email", largeHeader}, []string{headers[0].String(), headers[1].String()})
	require.Len(t, output.JobImportingParameters, 2)
	require.NotContains(t, output.JobImportingParameters, int64(2))
	for _, job := range importingJobsFromUpload(t, output) {
		email := "one@example.com"
		if job.JobID == 3 {
			email = "last@example.com"
		}
		row := email + ","
		if headers[0].String() != "email" {
			row = "," + email
		}
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(row)))
		require.JSONEq(t, fmt.Sprintf(`{"dataHash":%q}`, hash), string(output.JobImportingParameters[job.JobID]))
		require.Equal(t, hash, gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.dataHash").String())
	}
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

		result := uploader.Poll(context.Background(), common.AsyncPoll{
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

		result := uploader.Poll(context.Background(), common.AsyncPoll{
			ImportId: "test-import-123",
		})

		assert.True(t, result.Complete)
		assert.Equal(t, 200, result.StatusCode)
		assert.True(t, result.HasFailed)
		assert.True(t, result.HasWarning)
	})
}

func TestMarketoBulkUploader_PersistedStatsAcrossManagers(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	newUploader := func(config mbu.MarketoConfig) *mbu.MarketoBulkUploader {
		return mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
			DestinationName:   "MARKETO_BULK_UPLOAD",
			DestinationConfig: config,
			Logger:            logger.NewLogger(),
			StatsFactory:      stats.NOP,
			APIService:        api,
		})
	}
	originalConfig := mbu.MarketoConfig{
		MunchkinId: "old-munchkin",
		FieldsMapping: map[string]string{
			"email": "email", "firstName": "firstName", "lastName": "lastName",
		},
	}
	file := writeMarketoJobs(t, []common.AsyncJob{
		marketoJob(1, map[string]any{"email": "one@example.com", "firstName": "Ada", "lastName": "One"}),
		marketoJob(2, map[string]any{"email": "two@example.com", "firstName": "Grace", "lastName": "Two"}),
		marketoJob(3, map[string]any{"email": "three@example.com", "firstName": "Lin", "lastName": "Three"}),
	})
	api.EXPECT().ImportLeads(gomock.Any(), "").Return("original-import", nil)
	output := newUploader(originalConfig).Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination: &backendconfig.DestinationT{
			ID:                    "marketo-restarted-stats",
			DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "MARKETO_BULK_UPLOAD"},
		},
		FileName: file,
	})
	require.Equal(t, []int64{1, 2, 3}, output.ImportingJobIDs)
	jobs := importingJobsFromUpload(t, output)

	// Rebuild with changed mapping and credentials: the persisted CSV headers,
	// not today's mapping, must be used to correlate Marketo's result rows.
	uploader := newUploader(mbu.MarketoConfig{
		MunchkinId:    "new-munchkin",
		FieldsMapping: map[string]string{"other": "other"},
	})
	api.EXPECT().PollImportStatus("original-import").Return(&mbu.MarketoResponse{
		Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1, NumOfRowsWithWarning: 1}},
	}, nil)
	poll := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "original-import"})
	require.True(t, poll.Complete)
	require.True(t, poll.HasFailed)
	require.True(t, poll.HasWarning)
	api.EXPECT().GetLeadStatus(poll.FailedJobParameters).Return([]map[string]string{{
		"email": "one@example.com", "firstName": "Ada", "lastName": "One",
		mbu.MARKETO_FAILED_HEADER: "invalid email",
	}}, nil)
	api.EXPECT().GetLeadStatus(poll.WarningJobParameters).Return([]map[string]string{{
		"email": "two@example.com", "firstName": "Grace", "lastName": "Two",
		mbu.MARKETO_WARNING_HEADER: "duplicate",
	}}, nil)
	input := common.GetUploadStatsInput{
		ImportingList: jobs, Parameters: output.ImportingParameters,
		FailedJobParameters: poll.FailedJobParameters, WarningJobParameters: poll.WarningJobParameters,
	}
	result := uploader.GetUploadStats(input)
	require.Equal(t, 200, result.StatusCode)
	require.Equal(t, []int64{1, 2}, result.Metadata.AbortedKeys)
	require.Equal(t, map[int64]string{1: "invalid email", 2: "duplicate"}, result.Metadata.AbortedReasons)
	require.Equal(t, []int64{3}, result.Metadata.SucceededKeys)
}

func TestMarketoBulkUploader_QueuedThenComplete(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	uploader := mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		Logger: logger.NewLogger(), StatsFactory: stats.NOP, APIService: api,
		DestinationConfig: mbu.MarketoConfig{FieldsMapping: map[string]string{"email": "email"}},
	})
	api.EXPECT().ImportLeads(gomock.Any(), "").Return("queued-import", nil)
	output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination: &backendconfig.DestinationT{
			ID: "marketo-queued", DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "MARKETO_BULK_UPLOAD"},
		},
		FileName: writeMarketoJobs(t, []common.AsyncJob{marketoJob(17, map[string]any{"email": "queued@example.com"})}),
	})
	require.Equal(t, []int64{17}, output.ImportingJobIDs)
	api.EXPECT().PollImportStatus("queued-import").Return(&mbu.MarketoResponse{
		Result: []mbu.Result{{Status: "Queued"}},
	}, nil)
	queued := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "queued-import"})
	require.True(t, queued.InProgress)
	require.False(t, queued.Complete)

	api.EXPECT().PollImportStatus("queued-import").Return(&mbu.MarketoResponse{
		Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}},
	}, nil)
	complete := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "queued-import"})
	require.True(t, complete.Complete)
	require.True(t, complete.HasFailed)
	api.EXPECT().GetLeadStatus(complete.FailedJobParameters).Return([]map[string]string{{
		"email": "queued@example.com", mbu.MARKETO_FAILED_HEADER: "invalid",
	}}, nil)
	result := uploader.GetUploadStats(common.GetUploadStatsInput{
		Parameters:          output.ImportingParameters,
		ImportingList:       importingJobsFromUpload(t, output),
		FailedJobParameters: complete.FailedJobParameters,
	})
	require.Equal(t, 200, result.StatusCode)
	require.Equal(t, []int64{17}, result.Metadata.AbortedKeys)
	require.Equal(t, map[int64]string{17: "invalid"}, result.Metadata.AbortedReasons)
}

func TestMarketoBulkUploader_GetUploadStatsRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	uploader := mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		Logger: logger.NewLogger(), StatsFactory: stats.NOP, APIService: api,
	})
	params := json.RawMessage(`{"importId":"retry-import","importCount":2,"csvHeaders":["email"]}`)
	jobOneHash := fmt.Sprintf("%x", sha256.Sum256([]byte("one@example.com")))
	jobTwoHash := fmt.Sprintf("%x", sha256.Sum256([]byte("two@example.com")))
	jobs := []*jobsdb.JobT{
		{JobID: 1, LastJobStatus: jobsdb.JobStatusT{Parameters: json.RawMessage(fmt.Sprintf(`{"metadata":{"dataHash":%q}}`, jobOneHash))}},
		{JobID: 2, LastJobStatus: jobsdb.JobStatusT{Parameters: json.RawMessage(fmt.Sprintf(`{"metadata":{"dataHash":%q}}`, jobTwoHash))}},
	}
	api.EXPECT().PollImportStatus("retry-import").Return(&mbu.MarketoResponse{
		Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}},
	}, nil)
	poll := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "retry-import"})
	require.True(t, poll.HasFailed)
	input := common.GetUploadStatsInput{
		Parameters: params, ImportingList: jobs, FailedJobParameters: poll.FailedJobParameters,
	}
	api.EXPECT().GetLeadStatus(poll.FailedJobParameters).Return([]map[string]string{{
		"email": "one@example.com", mbu.MARKETO_FAILED_HEADER: "invalid",
	}}, nil)
	first := uploader.GetUploadStats(input)
	require.Equal(t, 200, first.StatusCode)
	require.Equal(t, []int64{1}, first.Metadata.AbortedKeys)
	require.Equal(t, map[int64]string{1: "invalid"}, first.Metadata.AbortedReasons)
	require.Equal(t, []int64{2}, first.Metadata.SucceededKeys)

	// A failed final status update leaves these importing statuses unchanged.
	// A new poll and stats pass must yield the same job IDs and reasons.
	api.EXPECT().PollImportStatus("retry-import").Return(&mbu.MarketoResponse{
		Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}},
	}, nil)
	poll = uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "retry-import"})
	require.True(t, poll.HasFailed)
	api.EXPECT().GetLeadStatus(poll.FailedJobParameters).Return([]map[string]string{{
		"email": "one@example.com", mbu.MARKETO_FAILED_HEADER: "invalid",
	}}, nil)
	input.FailedJobParameters = poll.FailedJobParameters
	require.Equal(t, first, uploader.GetUploadStats(input))
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
			Message: map[string]any{
				"email":     "test1@example.com",
				"firstName": "Test1",
				"lastName":  "User1",
			},
			Metadata: map[string]any{
				"job_id": float64(1),
			},
		},
		{
			Message: map[string]any{
				"email":     "test2@example.com",
				"firstName": "Test2",
				"lastName":  "User2",
			},
			Metadata: map[string]any{
				"job_id": float64(2),
			},
		},
		{
			Message: map[string]any{
				"email":     "test3@example.com",
				"firstName": "Test3",
				"lastName":  "User3",
			},
			Metadata: map[string]any{
				"job_id": float64(3),
			},
		},
	}

	// Write test data to temp file
	for _, job := range testData {
		jobBytes, _ := jsonrs.Marshal(job)
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

		// Persist the upload metadata on the importing statuses, as the batch router does.
		output := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
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
		uploader.Poll(context.Background(), common.AsyncPoll{
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

		importingList := importingJobsFromUpload(t, output)

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			ImportingList:        importingList,
			FailedJobParameters:  "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/failures.json",
			WarningJobParameters: "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/warnings.json",
			Parameters:           output.ImportingParameters,
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
		uploader.Poll(context.Background(), common.AsyncPoll{
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

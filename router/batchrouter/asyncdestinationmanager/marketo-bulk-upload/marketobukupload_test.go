package marketobulkupload_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
		require.Len(t, result.JobImportingParameters, 2)
		for _, id := range result.ImportingJobIDs {
			assert.NotEmpty(t, gjson.GetBytes(result.JobImportingParameters[id], "rowHash").String())
		}
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

		// Persist the per-job metadata as the batch router does.
		upload := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
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

		importingList := make([]*jobsdb.JobT, 0, 3)
		for _, id := range upload.ImportingJobIDs {
			params, err := sjson.SetBytes(upload.ImportingParameters, "metadata", upload.JobImportingParameters[id])
			require.NoError(t, err)
			importingList = append(importingList, &jobsdb.JobT{JobID: id, LastJobStatus: jobsdb.JobStatusT{Parameters: params}})
		}

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

		result := uploader.GetUploadStats(common.GetUploadStatsInput{
			ImportingList:        importingList,
			FailedJobParameters:  "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/failures.json",
			WarningJobParameters: "https://123-ABC-456.mktorest.com/bulk/v1/leads/batch/test-import-123/warnings.json",
			Parameters:           json.RawMessage(`{"importId":"test-import-123","importCount":3}`),
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
	t.Run("restarted manager and changed mapping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		api.EXPECT().ImportLeads(gomock.Any(), "").Return("import-1", nil)
		original := reconciliationUploader(api, map[string]string{"sourceEmail": "email", "sourceName": "name"})
		upload, jobs := uploadedJobs(t, original,
			job(10, map[string]any{"email": "one@example.com", "name": "Ada"}),
			job(11, map[string]any{"email": "two@example.com", "name": "Bob"}),
		)
		require.Len(t, upload.JobImportingParameters, 2)
		require.NotEqual(t, gjson.GetBytes(upload.JobImportingParameters[10], "rowHash").String(), gjson.GetBytes(upload.JobImportingParameters[11], "rowHash").String())

		// No Poll on this uploader, no CSV headers, no old field mapping.
		restarted := reconciliationUploader(api, map[string]string{"anotherField": "other"})
		api.EXPECT().PollImportStatus("import-1").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfLeadsProcessed: 1, NumOfRowsFailed: 1}}}, nil)
		api.EXPECT().GetLeadStatus("failures").Return([]map[string]string{{"name": "Ada", "email": "one@example.com", mbu.MARKETO_FAILED_HEADER: "invalid"}}, nil)
		got := restarted.GetUploadStats(common.GetUploadStatsInput{Parameters: upload.ImportingParameters, ImportingList: jobs, FailedJobParameters: "failures"})
		require.Equal(t, 200, got.StatusCode)
		require.Equal(t, []int64{10}, got.Metadata.AbortedKeys)
		require.Equal(t, "invalid", got.Metadata.AbortedReasons[10])
		require.Equal(t, []int64{11}, got.Metadata.SucceededKeys)
		require.NotContains(t, got.Metadata.AbortedReasons, int64(0))
	})

	t.Run("queued then complete and stats retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		api.EXPECT().ImportLeads(gomock.Any(), "").Return("first", nil)
		uploader := reconciliationUploader(api, map[string]string{"email": "email"})
		first, jobs := uploadedJobs(t, uploader, job(1, map[string]any{"email": "first@example.com"}))
		api.EXPECT().PollImportStatus("first").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Queued"}}}, nil)
		queued := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "first"})
		require.True(t, queued.InProgress)
		require.False(t, queued.Complete)
		api.EXPECT().PollImportStatus("first").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}}}, nil)
		failed := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "first"})
		require.Equal(t, 200, failed.StatusCode)
		require.True(t, failed.HasFailed)

		api.EXPECT().PollImportStatus("first").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}}}, nil)
		api.EXPECT().GetLeadStatus(failed.FailedJobParameters).Return([]map[string]string{{"email": "first@example.com", mbu.MARKETO_FAILED_HEADER: "bad"}}, nil)
		got := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: first.ImportingParameters, ImportingList: jobs, FailedJobParameters: failed.FailedJobParameters})
		require.Equal(t, []int64{1}, got.Metadata.AbortedKeys)
		require.Empty(t, got.Metadata.SucceededKeys)
		// A failed status update leaves the same importing job for another stats call.
		api.EXPECT().PollImportStatus("first").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}}}, nil)
		api.EXPECT().GetLeadStatus(failed.FailedJobParameters).Return([]map[string]string{{"email": "first@example.com", mbu.MARKETO_FAILED_HEADER: "bad"}}, nil)
		again := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: first.ImportingParameters, ImportingList: jobs, FailedJobParameters: failed.FailedJobParameters})
		require.Equal(t, []int64{1}, again.Metadata.AbortedKeys)
		require.Empty(t, again.Metadata.SucceededKeys)
	})

	t.Run("warning applies to all jobs sharing key", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		api.EXPECT().ImportLeads(gomock.Any(), "").Return("warnings", nil)
		uploader := reconciliationUploader(api, map[string]string{"email": "email"})
		upload, jobs := uploadedJobs(t, uploader,
			job(1, map[string]any{"email": "same@example.com"}),
			job(2, map[string]any{"email": "same@example.com"}),
		)
		api.EXPECT().PollImportStatus("warnings").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfRowsWithWarning: 1}}}, nil)
		api.EXPECT().GetLeadStatus("warnings-url").Return([]map[string]string{{"email": "same@example.com", mbu.MARKETO_WARNING_HEADER: "duplicate"}}, nil)
		got := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: upload.ImportingParameters, ImportingList: jobs, WarningJobParameters: "warnings-url"})
		require.ElementsMatch(t, []int64{1, 2}, got.Metadata.AbortedKeys)
		require.Equal(t, "duplicate", got.Metadata.AbortedReasons[1])
		require.Equal(t, "duplicate", got.Metadata.AbortedReasons[2])
		require.Empty(t, got.Metadata.SucceededKeys)
	})

	t.Run("unmatched result aborts unproved remainder", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		uploader := reconciliationUploader(api, nil)
		jobStatus := &jobsdb.JobT{JobID: 7, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(`{"metadata":{"rowHash":"hash"}}`)}}
		api.EXPECT().PollImportStatus("i").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfRowsFailed: 1}}}, nil)
		api.EXPECT().GetLeadStatus("failures").Return([]map[string]string{{"email": "other@example.com", mbu.MARKETO_FAILED_HEADER: "invalid"}}, nil)
		got := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"i"}`), ImportingList: []*jobsdb.JobT{jobStatus}, FailedJobParameters: "failures"})
		require.Equal(t, 200, got.StatusCode)
		require.Equal(t, []int64{7}, got.Metadata.AbortedKeys)
		require.Empty(t, got.Metadata.SucceededKeys)
	})

	t.Run("partial metadata aborts only missing job", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		uploader := reconciliationUploader(api, nil)
		jobs := []*jobsdb.JobT{
			{JobID: 1, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(`{"importId":"i","metadata":{"rowHash":"hash"}}`)}},
			{JobID: 2, LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(`{"importId":"i"}`)}},
		}
		api.EXPECT().PollImportStatus("i").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete"}}}, nil)
		result := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"i","importCount":2}`), ImportingList: jobs})
		require.Equal(t, []int64{1}, result.Metadata.SucceededKeys)
		require.Equal(t, []int64{2}, result.Metadata.AbortedKeys)
	})

	t.Run("no correlation metadata aborts batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		uploader := reconciliationUploader(api, nil)
		api.EXPECT().PollImportStatus("i").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete"}}}, nil)
		result := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"i"}`), ImportingList: []*jobsdb.JobT{{JobID: 1}, {JobID: 2}}})
		require.Equal(t, 200, result.StatusCode)
		require.ElementsMatch(t, []int64{1, 2}, result.Metadata.AbortedKeys)
		require.Empty(t, result.Metadata.SucceededKeys)
	})

	t.Run("empty import result retries", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
		uploader := reconciliationUploader(api, nil)
		api.EXPECT().PollImportStatus("i").Return(&mbu.MarketoResponse{}, nil)
		result := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: []byte(`{"importId":"i"}`), ImportingList: []*jobsdb.JobT{{JobID: 1}}})
		require.Equal(t, 500, result.StatusCode)
		require.Contains(t, result.Error, "No result found")
		require.Empty(t, result.Metadata.AbortedKeys)
	})
}

func reconciliationUploader(api mbu.MarketoAPIServiceInterface, mapping map[string]string) *mbu.MarketoBulkUploader {
	return mbu.NewMarketoBulkUploaderWithOptions(mbu.MarketoBulkUploaderOptions{
		DestinationName: "MARKETO_BULK_UPLOAD", DestinationConfig: mbu.MarketoConfig{MunchkinId: "old", FieldsMapping: mapping},
		Logger: logger.NewLogger(), StatsFactory: stats.NOP, APIService: api,
	})
}

func uploadedJobs(t *testing.T, uploader *mbu.MarketoBulkUploader, input ...common.AsyncJob) (common.AsyncUploadOutput, []*jobsdb.JobT) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "marketo-*.json")
	require.NoError(t, err)
	for _, job := range input {
		data, err := jsonrs.Marshal(job)
		require.NoError(t, err)
		_, err = fmt.Fprintln(file, string(data))
		require.NoError(t, err)
	}
	require.NoError(t, file.Close())
	upload := uploader.Upload(context.Background(), &common.AsyncDestinationStruct{
		Destination: &backendconfig.DestinationT{ID: "reconciliation", DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "MARKETO_BULK_UPLOAD"}},
		FileName:    file.Name(),
	})
	jobs := make([]*jobsdb.JobT, 0, len(upload.ImportingJobIDs))
	for _, id := range upload.ImportingJobIDs {
		params, err := sjson.SetBytes(upload.ImportingParameters, "metadata", upload.JobImportingParameters[id])
		require.NoError(t, err)
		jobs = append(jobs, &jobsdb.JobT{JobID: id, LastJobStatus: jobsdb.JobStatusT{Parameters: params}})
	}
	return upload, jobs
}

func job(id int, message map[string]any) common.AsyncJob {
	return common.AsyncJob{Message: message, Metadata: map[string]any{"job_id": float64(id)}}
}

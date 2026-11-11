package marketobulkupload_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
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

func TestReconciliationAfterRestartAndMappingReplacement(t *testing.T) {
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
}

func TestReconciliationQueuedThenCompleteAndRetry(t *testing.T) {
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

	// Upload another batch with the same manager. The first batch still has its
	// own durable correlation, even though the second is now in flight.
	api.EXPECT().ImportLeads(gomock.Any(), "").Return("second", nil)
	_, _ = uploadedJobs(t, uploader, job(2, map[string]any{"email": "second@example.com"}))
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
}

func TestReconciliationWarningsAndDuplicateRows(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicate=%t", duplicate), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
			api.EXPECT().ImportLeads(gomock.Any(), "").Return("warnings", nil)
			uploader := reconciliationUploader(api, map[string]string{"email": "email"})
			email := "one@example.com"
			if duplicate {
				email = "same@example.com"
			}
			upload, jobs := uploadedJobs(t, uploader, job(1, map[string]any{"email": email}), job(2, map[string]any{"email": "same@example.com"}))
			api.EXPECT().PollImportStatus("warnings").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete", NumOfLeadsProcessed: 1, NumOfRowsWithWarning: 1}}}, nil)
			api.EXPECT().GetLeadStatus("warnings-url").Return([]map[string]string{{"email": "same@example.com", mbu.MARKETO_WARNING_HEADER: "duplicate"}}, nil)
			got := uploader.GetUploadStats(common.GetUploadStatsInput{Parameters: upload.ImportingParameters, ImportingList: jobs, WarningJobParameters: "warnings-url"})
			if duplicate {
				require.ElementsMatch(t, []int64{1, 2}, got.Metadata.AbortedKeys)
				require.Equal(t, "duplicate", got.Metadata.AbortedReasons[1])
				require.Empty(t, got.Metadata.SucceededKeys)
			} else {
				require.Equal(t, []int64{2}, got.Metadata.AbortedKeys)
				require.Equal(t, "duplicate", got.Metadata.AbortedReasons[2])
				require.Equal(t, []int64{1}, got.Metadata.SucceededKeys)
			}
		})
	}
}

func TestMarketoUnmatchedResultAbortsRemainder(t *testing.T) {
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
}

func TestMarketoPartialMetadataAbortsOnlyMissingJob(t *testing.T) {
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
}

func TestMarketoCleanPollKeepsRouterSuccessPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mockmarketoservice.NewMockMarketoAPIServiceInterface(ctrl)
	uploader := reconciliationUploader(api, nil)
	api.EXPECT().PollImportStatus("i").Return(&mbu.MarketoResponse{Result: []mbu.Result{{Status: "Complete"}}}, nil)
	result := uploader.Poll(context.Background(), common.AsyncPoll{ImportId: "i"})
	require.True(t, result.Complete)
	require.False(t, result.HasFailed)
	require.False(t, result.HasWarning)
}

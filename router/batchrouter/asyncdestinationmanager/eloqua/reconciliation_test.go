package eloqua_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
)

const sharedEloquaParams = `{"importId":"/syncs/384:/contacts/imports/384","importCount":2,"uniqueKeys":["C_EmailAddress"]}`

func statsJobs() []*jobsdb.JobT {
	return importingJobsWithRows([]*jobsdb.JobT{{JobID: 11}, {JobID: 12}})
}

func statsInput(jobs []*jobsdb.JobT) common.GetUploadStatsInput {
	return common.GetUploadStatsInput{WarningJobParameters: "/syncs/384", Parameters: []byte(sharedEloquaParams), ImportingList: jobs}
}

func rejectPage(offset, total int, items ...eloqua.RejectedItem) *eloqua.RejectResponse {
	return &eloqua.RejectResponse{Offset: offset, TotalResults: total, Limit: 1000, Count: len(items), Items: items}
}

func TestEloquaReconciliationAfterRestartAndConfigChange(t *testing.T) {
	f := newEloquaFixture(t)
	f.bulkUploader = eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "", "", f.eloquaService)
	input := statsInput(statsJobs())
	for _, job := range input.ImportingList {
		job.EventPayload = nil
	}
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 2,
		eloqua.RejectedItem{RecordIndex: 1, StatusCode: "ELQ-00040", InvalidFields: []string{"C_FirstName"}},
		eloqua.RejectedItem{RecordIndex: 2, StatusCode: "ELQ-00040", InvalidFields: []string{"C_EmailAddress"}}), nil)
	result := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, result.StatusCode)
	require.Equal(t, []int64{11}, result.Metadata.SucceededKeys)
	require.Equal(t, []int64{12}, result.Metadata.AbortedKeys)
	require.Contains(t, result.Metadata.AbortedReasons[12], "ELQ-00040")
}

func TestEloquaRejectFetchRetryAfterDefinitionDeletion(t *testing.T) {
	f := newEloquaFixture(t)
	input := statsInput(statsJobs())
	f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("warning", nil)
	f.eloquaService.EXPECT().DeleteImportDefinition(gomock.Any()).Return(nil)
	poll := f.bulkUploader.Poll(t.Context(), common.AsyncPoll{ImportId: "/syncs/384:/contacts/imports/384"})
	require.True(t, poll.HasWarning)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(nil, errors.New("temporary API failure"))
	first := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 500, first.StatusCode)
	require.Empty(t, first.Metadata.SucceededKeys)
	f.bulkUploader = eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "", "", f.eloquaService)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 1, eloqua.RejectedItem{RecordIndex: 2, StatusCode: "INVALID"}), nil)
	second := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, second.StatusCode)
	require.Equal(t, []int64{11}, second.Metadata.SucceededKeys)
	require.Equal(t, []int64{12}, second.Metadata.AbortedKeys)
}

func TestEloquaPartialAndDuplicateRowMetadata(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
			f := newEloquaFixture(t)
			jobs := statsJobs()
			if duplicate {
				jobs[1].LastJobStatus.Parameters = jobs[0].LastJobStatus.Parameters
			} else {
				jobs[1].LastJobStatus.Parameters = []byte(sharedEloquaParams)
			}
			f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 1, eloqua.RejectedItem{RecordIndex: 1, StatusCode: "INVALID"}), nil)
			result := f.bulkUploader.GetUploadStats(statsInput(jobs))
			require.Equal(t, []int64{11, 12}, result.Metadata.AbortedKeys)
			require.Empty(t, result.Metadata.SucceededKeys)
			if duplicate {
				require.Equal(t, result.Metadata.AbortedReasons[11], result.Metadata.AbortedReasons[12])
			}
		})
	}
}

func TestEloquaUnknownRejectAbortsRemainder(t *testing.T) {
	f := newEloquaFixture(t)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 1, eloqua.RejectedItem{RecordIndex: 3, StatusCode: "INVALID"}), nil)
	result := f.bulkUploader.GetUploadStats(statsInput(statsJobs()))
	require.Equal(t, 200, result.StatusCode)
	require.ElementsMatch(t, []int64{11, 12}, result.Metadata.AbortedKeys)
	require.Empty(t, result.Metadata.SucceededKeys)
}

func TestEloquaRejectPagination(t *testing.T) {
	f := newEloquaFixture(t)
	jobs := make([]*jobsdb.JobT, 1001)
	first := make([]eloqua.RejectedItem, 1000)
	for i := range 1001 {
		jobs[i] = &jobsdb.JobT{JobID: int64(i + 1), LastJobStatus: jobsdb.JobStatusT{Parameters: []byte(fmt.Sprintf(`{"importId":"/syncs/384:/contacts/imports/384","metadata":{"csvRowIndex":%d}}`, i+1))}}
		if i < 1000 {
			first[i] = eloqua.RejectedItem{RecordIndex: int64(i + 1), StatusCode: "INVALID"}
		}
	}
	input := statsInput(jobs)
	input.Parameters = []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":1001,"uniqueKeys":[]}`)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).DoAndReturn(func(data *eloqua.HttpRequestData) (*eloqua.RejectResponse, error) {
		require.Zero(t, data.Offset)
		return rejectPage(0, 1001, first...), nil
	})
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).DoAndReturn(func(data *eloqua.HttpRequestData) (*eloqua.RejectResponse, error) {
		require.Equal(t, 1000, data.Offset)
		return rejectPage(1000, 1001, eloqua.RejectedItem{RecordIndex: 1001, StatusCode: "INVALID"}), nil
	})
	result := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, result.StatusCode)
	require.Len(t, result.Metadata.AbortedKeys, 1001)
	require.Empty(t, result.Metadata.SucceededKeys)
}

func TestEloquaCleanPollUsesRouterSuccessPath(t *testing.T) {
	f := newEloquaFixture(t)
	f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("success", nil)
	f.eloquaService.EXPECT().DeleteImportDefinition(gomock.Any()).Return(nil)
	poll := f.bulkUploader.Poll(t.Context(), common.AsyncPoll{ImportId: "/syncs/384:/contacts/imports/384"})
	require.True(t, poll.Complete)
	require.False(t, poll.HasWarning)
	require.False(t, poll.HasFailed)
}

func TestEloquaLegacyPolicyStillRequiresRowMetadata(t *testing.T) {
	config.Set("BatchRouter.ELOQUA.strictResultReconciliation", false)
	t.Cleanup(config.Reset)
	f := newEloquaFixture(t)
	jobs := statsJobs()
	jobs[1].LastJobStatus.Parameters = []byte(sharedEloquaParams)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 0), nil)
	result := f.bulkUploader.GetUploadStats(statsInput(jobs))
	require.Equal(t, []int64{11}, result.Metadata.SucceededKeys)
	require.Equal(t, []int64{12}, result.Metadata.AbortedKeys)
}

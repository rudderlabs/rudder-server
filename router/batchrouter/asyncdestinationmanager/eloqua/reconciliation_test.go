package eloqua_test

import (
	"errors"
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
	return common.GetUploadStatsInput{
		WarningJobParameters: "/syncs/384",
		Parameters:           []byte(sharedEloquaParams),
		ImportingList:        jobs,
	}
}

func rejectPage(offset, total int, hasMore bool, items ...eloqua.RejectedItem) *eloqua.RejectResponse {
	return &eloqua.RejectResponse{Offset: offset, TotalResults: total, Limit: 1, Count: len(items), HasMore: hasMore, Items: items}
}

func TestEloquaReconciliationAfterRestart(t *testing.T) {
	f := newEloquaFixture(t) // only a fresh manager sees the importing statuses
	input := statsInput(statsJobs())
	input.ImportingList[0].EventPayload = nil
	input.ImportingList[1].EventPayload = nil

	f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("warning", nil)
	poll := f.bulkUploader.Poll(t.Context(), common.AsyncPoll{ImportId: "/syncs/384:/contacts/imports/384"})
	require.True(t, poll.HasWarning)
	input.WarningJobParameters = poll.WarningJobParameters

	// ELQ-00040 is only acceptable when none of the invalid fields are unique.
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).DoAndReturn(func(data *eloqua.HttpRequestData) (*eloqua.RejectResponse, error) {
		require.Equal(t, 0, data.Offset)
		return rejectPage(0, 2, true, eloqua.RejectedItem{RecordIndex: 1, StatusCode: "ELQ-00040", InvalidFields: []string{"C_FirstName"}}), nil
	})
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).DoAndReturn(func(data *eloqua.HttpRequestData) (*eloqua.RejectResponse, error) {
		require.Equal(t, 1, data.Offset)
		return rejectPage(1, 2, false, eloqua.RejectedItem{RecordIndex: 2, StatusCode: "ELQ-00040", InvalidFields: []string{"C_EmailAddress"}}), nil
	})
	result := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, result.StatusCode)
	require.Equal(t, []int64{11}, result.Metadata.SucceededKeys)
	require.Equal(t, []int64{12}, result.Metadata.AbortedKeys)
	require.Contains(t, result.Metadata.AbortedReasons[12], "ELQ-00040")
}

func TestEloquaRejectFetchRetryPreservesDefinition(t *testing.T) {
	f := newEloquaFixture(t)
	input := statsInput(statsJobs())
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 2, true, eloqua.RejectedItem{RecordIndex: 1}), nil)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(nil, errors.New("temporary API failure"))
	first := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 500, first.StatusCode)
	require.Contains(t, first.Error, "temporary API failure")
	require.Empty(t, first.Metadata.SucceededKeys)

	// A subsequent stats call on the same manager must start fetching at offset 0.
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).DoAndReturn(func(data *eloqua.HttpRequestData) (*eloqua.RejectResponse, error) {
		require.Zero(t, data.Offset)
		return rejectPage(0, 2, true, eloqua.RejectedItem{RecordIndex: 1}), nil
	})
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(1, 2, false, eloqua.RejectedItem{RecordIndex: 2}), nil)
	second := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, second.StatusCode)
	require.Equal(t, []int64{11, 12}, second.Metadata.AbortedKeys)
	// A status update can fail after stats succeeds; a replacement manager must
	// still be able to fetch the same definition on its next poll.
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 0, false), nil)
	third := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, third.StatusCode)
}

func TestEloquaCleanSyncRequiresStats(t *testing.T) {
	f := newEloquaFixture(t)
	f.eloquaService.EXPECT().CheckSyncStatus(gomock.Any()).Return("success", nil)
	poll := f.bulkUploader.Poll(t.Context(), common.AsyncPoll{ImportId: "/syncs/384:/contacts/imports/384"})
	require.True(t, poll.HasWarning) // routes clean syncs through GetUploadStats
	input := statsInput(statsJobs())
	input.WarningJobParameters = poll.WarningJobParameters
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 0, false), nil)
	result := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, result.StatusCode)
	require.Equal(t, []int64{11, 12}, result.Metadata.SucceededKeys)
}

func TestEloquaAmbiguousResultsAbort(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*common.GetUploadStatsInput)
		page   *eloqua.RejectResponse
		calls  int
	}{
		{"missing row", func(in *common.GetUploadStatsInput) {
			in.ImportingList[1].LastJobStatus.Parameters = []byte(sharedEloquaParams)
		}, nil, 0},
		{"duplicate row", func(in *common.GetUploadStatsInput) {
			in.ImportingList[1].LastJobStatus.Parameters = in.ImportingList[0].LastJobStatus.Parameters
		}, nil, 0},
		{"missing policy", func(in *common.GetUploadStatsInput) {
			in.Parameters = []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":2}`)
		}, nil, 0},
		{"unknown ELQ-00040 row", func(*common.GetUploadStatsInput) {}, rejectPage(0, 1, false, eloqua.RejectedItem{RecordIndex: 3, StatusCode: "ELQ-00040"}), 1},
		{"pagination incomplete", func(*common.GetUploadStatsInput) {}, rejectPage(0, 2, false, eloqua.RejectedItem{RecordIndex: 1}), 1},
		{"duplicate reject", func(*common.GetUploadStatsInput) {}, &eloqua.RejectResponse{Offset: 0, TotalResults: 2, Limit: 2, Count: 2, Items: []eloqua.RejectedItem{{RecordIndex: 1}, {RecordIndex: 1}}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEloquaFixture(t)
			input := statsInput(statsJobs())
			tc.change(&input)
			if tc.calls > 0 {
				f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(tc.page, nil).Times(tc.calls)
			}
			result := f.bulkUploader.GetUploadStats(input)
			require.Equal(t, 200, result.StatusCode)
			require.Empty(t, result.Metadata.SucceededKeys)
			require.Equal(t, []int64{11, 12}, result.Metadata.AbortedKeys)
			for _, id := range result.Metadata.AbortedKeys {
				require.Contains(t, result.Metadata.AbortedReasons[id], "cannot be reconciled")
			}
		})
	}
}

func TestEloquaIncompleteImportingSetRetries(t *testing.T) {
	f := newEloquaFixture(t)
	input := statsInput(statsJobs()[:1])
	result := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 500, result.StatusCode)
	require.Contains(t, result.Error, "importing list incomplete")
	require.Empty(t, result.Metadata.AbortedKeys)
}

func TestEloquaFailedSyncValidatesImportingSet(t *testing.T) {
	for _, tc := range []struct {
		name, parameters string
		jobs             []*jobsdb.JobT
		wantCode         int
	}{
		{"short list", sharedEloquaParams, statsJobs()[:1], 500},
		{"missing count", `{"importId":"/syncs/384:/contacts/imports/384"}`, statsJobs()[:1], 500},
		{"invalid count", `{"importId":"/syncs/384:/contacts/imports/384","importCount":"2"}`, statsJobs()[:1], 500},
		{"mixed sync IDs", sharedEloquaParams, statsJobs(), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEloquaFixture(t)
			if tc.name == "mixed sync IDs" {
				tc.jobs[1].LastJobStatus.Parameters = []byte(`{"importId":"/syncs/other:/contacts/imports/other","metadata":{"csvRowIndex":2}}`)
			}
			input := statsInput(tc.jobs)
			input.WarningJobParameters = ""
			input.FailedJobParameters = "/syncs/384"
			input.Parameters = []byte(tc.parameters)
			result := f.bulkUploader.GetUploadStats(input)
			require.Equal(t, tc.wantCode, result.StatusCode)
			require.Empty(t, result.Metadata.SucceededKeys)
			require.Empty(t, result.Metadata.AbortedKeys)
			if tc.name == "mixed sync IDs" {
				require.Contains(t, result.Error, "mixed")
			}
		})
	}
}

func TestEloquaIncompleteCountRetriesEvenWithStrictDisabled(t *testing.T) {
	f := newEloquaFixture(t)
	config.Set("BatchRouter.ELOQUA.strictResultReconciliation", false)
	f.bulkUploader = eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "", "", f.eloquaService)
	for _, parameters := range []string{`{"importId":"/syncs/384:/contacts/imports/384"}`, `{"importId":"/syncs/384:/contacts/imports/384","importCount":"bad"}`} {
		input := statsInput(statsJobs()[:1])
		input.Parameters = []byte(parameters)
		result := f.bulkUploader.GetUploadStats(input)
		require.Equal(t, 500, result.StatusCode)
		require.Empty(t, result.Metadata.AbortedKeys)
	}
}

func TestEloquaPolicyChangesOnExistingManager(t *testing.T) {
	f := newEloquaFixture(t)
	input := statsInput(statsJobs())
	input.Parameters = []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":2}`)
	strict := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, []int64{11, 12}, strict.Metadata.AbortedKeys)
	config.Set("BatchRouter.ELOQUA.strictResultReconciliation", false)
	t.Cleanup(config.Reset)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 0, false), nil)
	legacy := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, 200, legacy.StatusCode)
	require.Equal(t, []int64{11, 12}, legacy.Metadata.SucceededKeys)
}

func TestEloquaDisabledStrictPolicyStillRequiresRows(t *testing.T) {
	f := newEloquaFixture(t)
	config.Set("BatchRouter.ELOQUA.strictResultReconciliation", false)
	f.bulkUploader = eloqua.NewEloquaBulkUploader(logger.NOP, stats.NOP, "Eloqua", "", "", f.eloquaService)
	input := statsInput(statsJobs())
	input.Parameters = []byte(`{"importId":"/syncs/384:/contacts/imports/384","importCount":2}`)
	f.eloquaService.EXPECT().CheckRejectedData(gomock.Any()).Return(rejectPage(0, 1, false, eloqua.RejectedItem{RecordIndex: 3, StatusCode: "ELQ-00040"}), nil)
	result := f.bulkUploader.GetUploadStats(input)
	require.Equal(t, []int64{11, 12}, result.Metadata.AbortedKeys)
	require.Empty(t, result.Metadata.SucceededKeys)
}

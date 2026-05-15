package batchrouter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

// fakeDrainer implements routerutils.Drainer with a static (drain, reason)
// answer so tests can exercise the drain branch in scheduleJobs deterministically.
type fakeDrainer struct {
	drain  bool
	reason string
}

func (d *fakeDrainer) Drain(_ time.Time, _, _ string) (bool, string) {
	return d.drain, d.reason
}

// newDrainTestBatchRouter assembles a *Handle with the minimum wiring needed to
// drive scheduleJobs's drain branch end-to-end: a mock jobsDB to capture the
// status update, a configurable drainer, a noop rsources service, and a
// destinationsMap that lets `sourceFound` be controlled by the caller.
func newDrainTestBatchRouter(
	t *testing.T,
	destType string,
	sources []backendconfig.SourceT,
	drainer routerutils.Drainer,
) (*Handle, *mocksJobsDB.MockJobsDB) {
	t.Helper()

	mockCtrl := gomock.NewController(t)
	mockJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)

	brt := defaultHandle(destType)
	brt.jobsDB = mockJobsDB
	brt.rsourcesService = rsources.NewNoOpService()
	brt.jobsDBCommandTimeout = config.SingleValueLoader(time.Second)
	brt.jobdDBMaxRetries = config.SingleValueLoader(1)
	brt.destinationsMap["destinationID"] = &routerutils.DestinationWithSources{
		Destination: backendconfig.DestinationT{ID: "destinationID", Enabled: true},
		Sources:     sources,
	}
	brt.drainer = drainer

	return brt, mockJobsDB
}

// newDrainTestWorker builds a *worker that wraps brt with just enough fields
// for scheduleJobs to run. The drain branch never touches the PartitionWorker
// (`pw`) or the circuit breaker (`cb`), so we leave them unset.
func newDrainTestWorker(brt *Handle) *worker {
	return &worker{
		partition: "destinationID",
		logger:    logger.NOP,
		brt:       brt,
	}
}

// destinationJobs builds a DestinationJobs payload pointed at "destinationID"
// with the given pre-existing job. The destWithSources mirrors what's in
// brt.destinationsMap so scheduleJobs's `sourceFound` check passes when the
// sources list contains the job's source_id.
func destinationJobs(sources []backendconfig.SourceT, jobs ...*jobsdb.JobT) *DestinationJobs {
	return &DestinationJobs{
		destWithSources: routerutils.DestinationWithSources{
			Destination: backendconfig.DestinationT{ID: "destinationID", Enabled: true},
			Sources:     sources,
		},
		jobs: jobs,
	}
}

func TestScheduleJobsDrainBranch(t *testing.T) {
	const destType = "MARKETO_BULK_UPLOAD"

	sources := []backendconfig.SourceT{
		{ID: "sourceID", WorkspaceID: "workspaceID"},
	}

	t.Run("preserves prior LastJobStatus error fields and stamps the drain reason", func(t *testing.T) {
		brt, mockJobsDB := newDrainTestBatchRouter(
			t, destType, sources,
			&fakeDrainer{drain: true, reason: routerutils.DrainReasonJobExpired},
		)

		priorError := []byte(`{"error":"500 internal server error","response":"upstream timed out","firstAttemptedAt":"2024-01-01T00:00:00.000Z"}`)

		jobs := []*jobsdb.JobT{
			{
				JobID:     401,
				UserID:    "u1",
				CreatedAt: time.Now().Add(-30 * 24 * time.Hour), // older than any retention
				ExpireAt:  time.Now().Add(24 * time.Hour),
				CustomVal: destType,
				LastJobStatus: jobsdb.JobStatusT{
					AttemptNum:    3,
					ErrorResponse: priorError,
				},
				Parameters:  []byte(`{"source_id":"sourceID","destination_id":"destinationID"}`),
				WorkspaceId: "workspaceID",
				PartitionID: "destinationID",
			},
		}

		var captured []*jobsdb.JobStatusT
		mockJobsDB.EXPECT().
			WithUpdateSafeTx(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) error {
				return f(jobsdb.EmptyUpdateSafeTx())
			})
		mockJobsDB.EXPECT().
			UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).
			Times(1).
			Do(func(ctx context.Context, tx any, statuses []*jobsdb.JobStatusT) {
				captured = statuses
			}).
			Return(nil)

		newDrainTestWorker(brt).scheduleJobs(destinationJobs(sources, jobs...))

		require.Len(t, captured, 1)
		got := captured[0]
		require.Equal(t, int64(401), got.JobID)
		require.Equal(t, jobsdb.Aborted.State, got.JobState)
		require.Equal(t, routerutils.DRAIN_ERROR_CODE, got.ErrorCode)

		// The prior LastJobStatus.ErrorResponse fields must survive the drain —
		// the in-place edit in worker.go now bases the response on
		// job.LastJobStatus.ErrorResponse instead of an empty `{}` payload.
		require.Equal(t, "500 internal server error",
			gjson.GetBytes(got.ErrorResponse, "error").String(),
			"expected the upstream error to be preserved from LastJobStatus")
		require.Equal(t, "upstream timed out",
			gjson.GetBytes(got.ErrorResponse, "response").String(),
			"expected the upstream response body to be preserved from LastJobStatus")
		require.Equal(t, "2024-01-01T00:00:00.000Z",
			gjson.GetBytes(got.ErrorResponse, "firstAttemptedAt").String(),
			"expected firstAttemptedAt to be preserved from LastJobStatus")

		// The drain reason is added/overwritten on top of the preserved fields.
		require.Equal(t, routerutils.DrainReasonJobExpired,
			gjson.GetBytes(got.ErrorResponse, "reason").String(),
			"expected the drain reason to be stamped onto the response")
	})

	t.Run("synthesizes reason when LastJobStatus has no prior error response", func(t *testing.T) {
		brt, mockJobsDB := newDrainTestBatchRouter(
			t, destType, sources,
			&fakeDrainer{drain: true, reason: routerutils.DrainReasonJobExpired},
		)

		jobs := []*jobsdb.JobT{
			{
				JobID:     402,
				UserID:    "u1",
				CreatedAt: time.Now().Add(-30 * 24 * time.Hour),
				ExpireAt:  time.Now().Add(24 * time.Hour),
				CustomVal: destType,
				LastJobStatus: jobsdb.JobStatusT{
					AttemptNum: 0,
					// no ErrorResponse - first time the job is being looked at
				},
				Parameters:  []byte(`{"source_id":"sourceID","destination_id":"destinationID"}`),
				WorkspaceId: "workspaceID",
				PartitionID: "destinationID",
			},
		}

		var captured []*jobsdb.JobStatusT
		mockJobsDB.EXPECT().
			WithUpdateSafeTx(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) error {
				return f(jobsdb.EmptyUpdateSafeTx())
			})
		mockJobsDB.EXPECT().
			UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).
			Times(1).
			Do(func(ctx context.Context, tx any, statuses []*jobsdb.JobStatusT) {
				captured = statuses
			}).
			Return(nil)

		newDrainTestWorker(brt).scheduleJobs(destinationJobs(sources, jobs...))

		require.Len(t, captured, 1)
		got := captured[0]
		require.Equal(t, jobsdb.Aborted.State, got.JobState)
		require.Equal(t, routerutils.DRAIN_ERROR_CODE, got.ErrorCode)
		require.Equal(t, routerutils.DrainReasonJobExpired,
			gjson.GetBytes(got.ErrorResponse, "reason").String())
	})

	t.Run("uses source_not_found reason when the job's source is missing from the destination", func(t *testing.T) {
		// drainer says no drain, but the source isn't in the destination's
		// sources list, so scheduleJobs should still drain with
		// reason=source_not_found.
		brt, mockJobsDB := newDrainTestBatchRouter(
			t, destType, sources,
			&fakeDrainer{drain: false},
		)

		priorError := []byte(`{"error":"prior boom","firstAttemptedAt":"2024-01-01T00:00:00.000Z"}`)

		jobs := []*jobsdb.JobT{
			{
				JobID:     403,
				UserID:    "u1",
				CreatedAt: time.Now(),
				ExpireAt:  time.Now().Add(24 * time.Hour),
				CustomVal: destType,
				LastJobStatus: jobsdb.JobStatusT{
					AttemptNum:    1,
					ErrorResponse: priorError,
				},
				// source_id doesn't match anything in sources => sourceFound=false
				Parameters:  []byte(`{"source_id":"unknownSource","destination_id":"destinationID"}`),
				WorkspaceId: "workspaceID",
				PartitionID: "destinationID",
			},
		}

		var captured []*jobsdb.JobStatusT
		mockJobsDB.EXPECT().
			WithUpdateSafeTx(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) error {
				return f(jobsdb.EmptyUpdateSafeTx())
			})
		mockJobsDB.EXPECT().
			UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).
			Times(1).
			Do(func(ctx context.Context, tx any, statuses []*jobsdb.JobStatusT) {
				captured = statuses
			}).
			Return(nil)

		newDrainTestWorker(brt).scheduleJobs(destinationJobs(sources, jobs...))

		require.Len(t, captured, 1)
		got := captured[0]
		require.Equal(t, jobsdb.Aborted.State, got.JobState)
		require.Equal(t, routerutils.DRAIN_ERROR_CODE, got.ErrorCode)
		require.Equal(t, "source_not_found",
			gjson.GetBytes(got.ErrorResponse, "reason").String())
		// And the prior error should still be preserved alongside the new reason.
		require.Equal(t, "prior boom",
			gjson.GetBytes(got.ErrorResponse, "error").String(),
			"expected prior LastJobStatus error to be preserved on source-not-found drains")
	})
}

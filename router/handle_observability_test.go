package router

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

// capturingJobService is a minimal rsources.JobService fake that records the
// FailedRecords handed to AddFailedRecords. The embedded (nil) interface
// satisfies every method this test never exercises; updateRudderSourcesStats's
// call to Publish only ever reaches IncrementStats and AddFailedRecords, both
// overridden below, so the embedded nil is never dereferenced.
type capturingJobService struct {
	rsources.JobService
	failedRecords []rsources.FailedRecord
}

func (c *capturingJobService) IncrementStats(context.Context, *sql.Tx, string, rsources.JobTargetKey, rsources.Stats) error {
	return nil
}

func (c *capturingJobService) AddFailedRecords(_ context.Context, _ *sql.Tx, _ string, _ rsources.JobTargetKey, records []rsources.FailedRecord) error {
	c.failedRecords = append(c.failedRecords, records...)
	return nil
}

// retlJobAndStatus builds a minimal aborted rETL job/status pair carrying the
// parameters the failed-records path indexes on (source_job_run_id, source_id,
// destination_id, record_id) with a non-empty ErrorResponse, so that
// CollectFailedRecords both finds the record and asks the SyncSettingDelegate
// for its capture decision.
func retlJobAndStatus(jobID int64) (*jobsdb.JobT, *jobsdb.JobStatusT) {
	params := []byte(`{"source_job_run_id":"run-1","source_id":"src-1","destination_id":"dest-1","record_id":"rec-1"}`)
	job := &jobsdb.JobT{JobID: jobID, Parameters: params}
	status := &jobsdb.JobStatusT{
		JobID:         jobID,
		JobState:      jobsdb.Aborted.State,
		ErrorResponse: []byte(`{"reason":"x"}`),
	}
	return job, status
}

// TestUpdateRudderSourcesStats_PropagatesSyncSettingError proves that an error
// from the SyncSettingDelegate surfaces out of updateRudderSourcesStats instead
// of being swallowed: silently continuing would publish a failed record with an
// empty error_response indistinguishable from "the destination said nothing".
func TestUpdateRudderSourcesStats_PropagatesSyncSettingError(t *testing.T) {
	boom := errors.New("boom")
	job, status := retlJobAndStatus(1)

	rt := &Handle{
		logger:               logger.NOP,
		rsourcesService:      rsources.NewNoOpService(),
		rsourcesSyncSettings: rsources.NewStaticSyncSettingDelegate("", boom),
	}

	err := rt.updateRudderSourcesStats(
		context.Background(),
		jobsdb.EmptyUpdateSafeTx(),
		[]*jobsdb.JobT{job},
		[]*jobsdb.JobStatusT{status},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "boom")
}

// TestUpdateRudderSourcesStats_CapturesErrorResponse proves the happy path end
// to end: whatever text the delegate answers with is exactly what reaches the
// published FailedRecord.
func TestUpdateRudderSourcesStats_CapturesErrorResponse(t *testing.T) {
	job, status := retlJobAndStatus(2)
	jobService := &capturingJobService{}

	rt := &Handle{
		logger:               logger.NOP,
		rsourcesService:      jobService,
		rsourcesSyncSettings: rsources.NewStaticSyncSettingDelegate("captured-text", nil),
	}

	err := rt.updateRudderSourcesStats(
		context.Background(),
		jobsdb.EmptyUpdateSafeTx(),
		[]*jobsdb.JobT{job},
		[]*jobsdb.JobStatusT{status},
	)

	require.NoError(t, err)
	require.Len(t, jobService.failedRecords, 1)
	require.Equal(t, "captured-text", jobService.failedRecords[0].Error)
}

// TestTrackFailureMetrics proves that a new error response for an event adds to
// that event's counts instead of replacing the counts collected so far.
func TestTrackFailureMetrics(t *testing.T) {
	rt := &Handle{telemetry: &Diagnostic{failuresMetric: make(map[string]map[string]int)}}

	rt.trackFailureMetrics(diagnostics.RouterFailed, "timeout")
	rt.trackFailureMetrics(diagnostics.RouterFailed, "timeout")
	rt.trackFailureMetrics(diagnostics.RouterFailed, "connection refused")
	rt.trackFailureMetrics(diagnostics.RouterFailed, "timeout")
	rt.trackFailureMetrics(diagnostics.RouterAborted, "bad request")

	require.Equal(t, map[string]map[string]int{
		diagnostics.RouterFailed:  {"timeout": 3, "connection refused": 1},
		diagnostics.RouterAborted: {"bad request": 1},
	}, rt.telemetry.failuresMetric)
}

// TestTakeRequestMetrics proves the swap hands over everything collected so far
// and leaves the next batch empty, and that concurrent callers neither lose nor
// double-count a metric.
func TestTakeRequestMetrics(t *testing.T) {
	rt := &Handle{telemetry: &Diagnostic{}}

	rt.telemetry.requestsMetric = []requestMetric{{RequestRetries: 1}, {RequestSuccess: 2}}
	require.Len(t, rt.takeRequestMetrics(), 2)
	require.Empty(t, rt.telemetry.requestsMetric)
	require.Empty(t, rt.takeRequestMetrics())

	const (
		writers   = 4
		takers    = 2
		perWriter = 250
	)
	var (
		writersWG   sync.WaitGroup
		takersWG    sync.WaitGroup
		writersDone = make(chan struct{})
		taken       atomic.Int64
	)
	for range takers {
		takersWG.Go(func() {
			for {
				taken.Add(int64(len(rt.takeRequestMetrics())))
				select {
				case <-writersDone: // drain whatever the writers added last
					taken.Add(int64(len(rt.takeRequestMetrics())))
					return
				default:
				}
			}
		})
	}
	for range writers {
		writersWG.Go(func() {
			for range perWriter {
				rt.telemetry.requestsMetricLock.Lock()
				rt.telemetry.requestsMetric = append(rt.telemetry.requestsMetric, requestMetric{RequestSuccess: 1})
				rt.telemetry.requestsMetricLock.Unlock()
			}
		})
	}
	writersWG.Wait()
	close(writersDone)
	takersWG.Wait()

	require.Equal(t, int64(writers*perWriter), taken.Load())
	require.Empty(t, rt.telemetry.requestsMetric)
}

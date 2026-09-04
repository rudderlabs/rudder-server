package batchrouter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestEmitAsyncEventDeliveryTimeMetrics_NoAsyncDestinationStruct(t *testing.T) {
	brt := &Handle{
		logger:                 logger.NOP,
		destType:               "MARKETO_BULK_UPLOAD",
		asyncDestinationStruct: make(map[string]*common.AsyncDestinationStruct),
	}
	destinationID := "dest-123"
	sourceID := "source-456"
	workspaceID := "workspace-789"
	jobID := int64(12345)

	statusList := []*jobsdb.JobStatusT{
		{
			JobID:       jobID,
			JobState:    jobsdb.Succeeded.State,
			WorkspaceId: workspaceID,
		},
	}
	require.NotPanics(t, func() {
		brt.emitAsyncEventDeliveryTimeMetrics(sourceID, destinationID, statusList)
	})
}

func TestEmitAsyncEventDeliveryTimeMetrics(t *testing.T) {
	// Set up the batch router handle
	brt := &Handle{
		logger:                 logger.NOP,
		destType:               "MARKETO_BULK_UPLOAD",
		asyncDestinationStruct: make(map[string]*common.AsyncDestinationStruct),
	}

	// Create test data
	destinationID := "dest-123"
	sourceID := "source-456"
	workspaceID := "workspace-789"
	jobID := int64(12345)

	// Create a receivedAt time (2 hours ago)
	receivedAt := time.Now().Add(-2 * time.Hour)
	receivedAtStr := receivedAt.Format(misc.RFC3339Milli)

	// Create job parameters with receivedAt
	jobParams := map[string]any{
		"source_id":       sourceID,
		"destination_id":  destinationID,
		"received_at":     receivedAtStr,
		"source_category": "cloud",
		"workspace_id":    workspaceID,
	}
	jobParamsBytes, _ := jsonrs.Marshal(jobParams)

	// Create destination config
	destination := &backendconfig.DestinationT{
		ID:   destinationID,
		Name: "Test Marketo Destination",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "MARKETO_BULK_UPLOAD",
		},
	}

	// Set up async destination struct
	brt.asyncDestinationStruct[destinationID] = &common.AsyncDestinationStruct{
		Destination: destination,
		JobParameters: map[int64]json.RawMessage{
			jobID: jobParamsBytes,
		},
	}

	// Create job status list with successful job
	statusList := []*jobsdb.JobStatusT{
		{
			JobID:       jobID,
			JobState:    jobsdb.Succeeded.State,
			WorkspaceId: workspaceID,
		},
	}

	// Use NOP stats to avoid actual metric emission during test
	originalStats := stats.Default
	stats.Default = stats.NOP
	defer func() {
		stats.Default = originalStats
	}()

	// Call the function under test - should not panic
	require.NotPanics(t, func() {
		brt.emitAsyncEventDeliveryTimeMetrics(sourceID, destinationID, statusList)
	})
}

func TestEmitAsyncEventDeliveryTimeMetrics_NoSuccessfulJobs(t *testing.T) {
	brt := &Handle{
		logger:                 logger.NOP,
		destType:               "MARKETO_BULK_UPLOAD",
		asyncDestinationStruct: make(map[string]*common.AsyncDestinationStruct),
	}

	destinationID := "dest-123"
	sourceID := "source-456"
	workspaceID := "workspace-789"

	// Create destination
	destination := &backendconfig.DestinationT{
		ID:   destinationID,
		Name: "Test Marketo Destination",
	}

	brt.asyncDestinationStruct[destinationID] = &common.AsyncDestinationStruct{
		Destination:   destination,
		JobParameters: make(map[int64]json.RawMessage),
	}

	// Create job status list with only failed jobs
	statusList := []*jobsdb.JobStatusT{
		{
			JobID:       12345,
			JobState:    jobsdb.Failed.State,
			WorkspaceId: workspaceID,
		},
	}

	// Use NOP stats to avoid actual metric emission during test
	originalStats := stats.Default
	stats.Default = stats.NOP
	defer func() {
		stats.Default = originalStats
	}()

	// Call the function under test - should not panic and should not emit metrics for failed jobs
	require.NotPanics(t, func() {
		brt.emitAsyncEventDeliveryTimeMetrics(sourceID, destinationID, statusList)
	})
}

func TestEmitAsyncEventDeliveryTimeMetrics_MissingReceivedAt(t *testing.T) {
	brt := &Handle{
		logger:                 logger.NOP,
		destType:               "MARKETO_BULK_UPLOAD",
		asyncDestinationStruct: make(map[string]*common.AsyncDestinationStruct),
	}

	destinationID := "dest-123"
	sourceID := "source-456"
	workspaceID := "workspace-789"
	jobID := int64(12345)

	// Create job parameters WITHOUT receivedAt
	jobParams := map[string]any{
		"source_id":      sourceID,
		"destination_id": destinationID,
		// "received_at" is missing
		"source_category": "cloud",
	}
	jobParamsBytes, _ := jsonrs.Marshal(jobParams)

	destination := &backendconfig.DestinationT{
		ID:   destinationID,
		Name: "Test Marketo Destination",
	}

	brt.asyncDestinationStruct[destinationID] = &common.AsyncDestinationStruct{
		Destination: destination,
		JobParameters: map[int64]json.RawMessage{
			jobID: jobParamsBytes,
		},
	}

	statusList := []*jobsdb.JobStatusT{
		{
			JobID:       jobID,
			JobState:    jobsdb.Succeeded.State,
			WorkspaceId: workspaceID,
		},
	}

	// Use NOP stats to avoid actual metric emission during test
	originalStats := stats.Default
	stats.Default = stats.NOP
	defer func() {
		stats.Default = originalStats
	}()

	// Call the function under test - should not panic when receivedAt is missing
	require.NotPanics(t, func() {
		brt.emitAsyncEventDeliveryTimeMetrics(sourceID, destinationID, statusList)
	})
}

func TestEmitAsyncEventDeliveryTimeMetrics_InvalidReceivedAt(t *testing.T) {
	brt := &Handle{
		logger:                 logger.NOP,
		destType:               "MARKETO_BULK_UPLOAD",
		asyncDestinationStruct: make(map[string]*common.AsyncDestinationStruct),
	}

	destinationID := "dest-123"
	sourceID := "source-456"
	workspaceID := "workspace-789"
	jobID := int64(12345)

	// Create job parameters with invalid receivedAt format
	jobParams := map[string]any{
		"source_id":       sourceID,
		"destination_id":  destinationID,
		"received_at":     "invalid-time-format",
		"source_category": "cloud",
	}
	jobParamsBytes, _ := jsonrs.Marshal(jobParams)

	destination := &backendconfig.DestinationT{
		ID:   destinationID,
		Name: "Test Marketo Destination",
	}

	brt.asyncDestinationStruct[destinationID] = &common.AsyncDestinationStruct{
		Destination: destination,
		JobParameters: map[int64]json.RawMessage{
			jobID: jobParamsBytes,
		},
	}

	statusList := []*jobsdb.JobStatusT{
		{
			JobID:       jobID,
			JobState:    jobsdb.Succeeded.State,
			WorkspaceId: workspaceID,
		},
	}

	// Use NOP stats to avoid actual metric emission during test
	originalStats := stats.Default
	stats.Default = stats.NOP
	defer func() {
		stats.Default = originalStats
	}()

	// Call the function under test - should not panic when receivedAt format is invalid
	require.NotPanics(t, func() {
		brt.emitAsyncEventDeliveryTimeMetrics(sourceID, destinationID, statusList)
	})
}

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

	brt := &Handle{
		logger:               logger.NOP,
		rsourcesService:      rsources.NewNoOpService(),
		rsourcesSyncSettings: rsources.NewStaticSyncSettingDelegate("", boom),
	}

	err := brt.updateRudderSourcesStats(
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

	brt := &Handle{
		logger:               logger.NOP,
		rsourcesService:      jobService,
		rsourcesSyncSettings: rsources.NewStaticSyncSettingDelegate("captured-text", nil),
	}

	err := brt.updateRudderSourcesStats(
		context.Background(),
		jobsdb.EmptyUpdateSafeTx(),
		[]*jobsdb.JobT{job},
		[]*jobsdb.JobStatusT{status},
	)

	require.NoError(t, err)
	require.Len(t, jobService.failedRecords, 1)
	require.Equal(t, "captured-text", jobService.failedRecords[0].Error)
}

package batchrouter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func replaceDefaultStatsWithMemStats(t *testing.T) *memstats.Store {
	t.Helper()

	statsStore, err := memstats.New()
	require.NoError(t, err)

	originalStats := stats.Default
	stats.Default = statsStore
	t.Cleanup(func() {
		stats.Default = originalStats
	})

	return statsStore
}

func TestBatchRouterMetricTagsUseRawDestinationID(t *testing.T) {
	destination := Connection{
		Source: backendconfig.SourceT{
			ID:          "source-full-id-123456",
			Name:        "Readable Source",
			WorkspaceID: "workspace-123",
			SourceDefinition: backendconfig.SourceDefinitionT{
				Category: "cloud",
			},
		},
		Destination: backendconfig.DestinationT{
			ID:   "dest-full-id-abcdef",
			Name: "Readable Destination",
		},
	}
	destType := "S3"

	t.Run("destination status tags include raw destinationId", func(t *testing.T) {
		tags := batchRouterDestinationStatusTags(&destination, destType, true)
		require.Equal(t, destination.Destination.ID, tags["destinationId"])
		require.NotContains(t, tags["destinationId"], destination.Destination.Name)
	})

	t.Run("upload stats tags include raw destination ID without derived destination label", func(t *testing.T) {
		statsStore := replaceDefaultStatsWithMemStats(t)
		brt := &Handle{destType: destType}

		brt.recordUploadStats(destination, UploadResult{
			FirstEventAt: time.Now().Add(-time.Minute).UTC().Format(misc.RFC3339Milli),
			TotalEvents:  2,
		})

		eventDeliveryMetrics := statsStore.GetByName("event_delivery")
		require.Len(t, eventDeliveryMetrics, 1)
		require.Equal(t, destination.Destination.ID, eventDeliveryMetrics[0].Tags["destID"])
		_, hasDerivedDestinationTag := eventDeliveryMetrics[0].Tags["destination"]
		require.False(t, hasDerivedDestinationTag)

		eventDeliveryTimeMetrics := statsStore.GetByName("event_delivery_time")
		require.Len(t, eventDeliveryTimeMetrics, 1)
		require.Equal(t, destination.Destination.ID, eventDeliveryTimeMetrics[0].Tags["destID"])
		_, hasDerivedDestinationTag = eventDeliveryTimeMetrics[0].Tags["destination"]
		require.False(t, hasDerivedDestinationTag)
	})
}

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

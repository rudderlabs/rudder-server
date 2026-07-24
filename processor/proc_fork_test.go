package processor

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestShouldForkDestination(t *testing.T) {
	t.Run("no procDB configured never forks", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.conf = config.New()
		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.all", true)
		require.Nil(t, proc.procDB)
		require.False(t, proc.shouldForkDestination("d1"))
	})

	t.Run("instance-wide default", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.procDB = &jobsdb.Handle{} // non-nil sentinel; shouldForkDestination only nil-checks it
		proc.conf = config.New()
		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.all", true)

		require.True(t, proc.shouldForkDestination("d1"))
		require.True(t, proc.shouldForkDestination("d2"))
	})

	t.Run("per-destination override wins over all", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.procDB = &jobsdb.Handle{} // non-nil sentinel; shouldForkDestination only nil-checks it
		proc.conf = config.New()
		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.all", true)
		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.d2", false)

		require.True(t, proc.shouldForkDestination("d1"))
		require.False(t, proc.shouldForkDestination("d2"))
	})

	t.Run("resolved value is cached per destination", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.procDB = &jobsdb.Handle{} // non-nil sentinel; shouldForkDestination only nil-checks it
		proc.conf = config.New()
		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.all", true)

		require.True(t, proc.shouldForkDestination("d1"))

		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.all", false)
		proc.conf.Set("Processor.DestinationIsolation.enabledDestinations.d1", false)

		require.True(t, proc.shouldForkDestination("d1"))
		require.False(t, proc.shouldForkDestination("d2"))
	})
}

func TestNewForkedJob(t *testing.T) {
	proc := newTestProcHandle()
	event := &types.TransformerEvent{
		Message: types.SingularEventT{"event": "track", "userId": "u1"},
		Metadata: types.Metadata{
			JobID:       42,
			WorkspaceID: "ws1",
			RudderID:    "u1",
			ReceivedAt:  "2026-07-22T00:00:00.000Z",
			PartitionID: "p1",
			MessageID:   "m1",
			SourceID:    "src1",
			SourceName:  "source-1",
			// destination-specific fields must NOT leak into a multi-destination forked job
			DestinationID:           "should-not-appear",
			DestinationName:         "should-not-appear",
			DestinationType:         "should-not-appear",
			DestinationDefinitionID: "should-not-appear",
			TransformationID:        "should-not-appear",
			TransformationVersionID: "should-not-appear",
		},
	}
	steps := SourcePipelineSteps{srcHydration: true, trackingPlanValidation: true}
	gwParams := json.RawMessage(`{"source_id":"src1","source_job_run_id":"jr1","traceparent":"tp"}`)

	job, err := proc.newForkedJob(event, []string{"d1", "d2"}, steps, gwParams)
	require.NoError(t, err)

	require.Equal(t, forkedJobCustomVal, job.CustomVal)
	require.Equal(t, []string{"d1", "d2"}, job.Consumers)
	require.Equal(t, "ws1", job.WorkspaceId)
	require.Equal(t, "u1", job.UserID)
	require.Equal(t, "p1", job.PartitionID)
	require.NotEqual(t, [16]byte{}, [16]byte(job.UUID))

	// payload carries source-level message + metadata; procRebuildStage re-hydrates the
	// destination per consumer, so the stored metadata keeps the source-level view.
	var payload procJobPayload
	require.NoError(t, jsonrs.Unmarshal(job.EventPayload, &payload))
	require.Equal(t, event.Message, payload.Message)
	require.Equal(t, "src1", payload.Metadata.SourceID)
	require.True(t, payload.SrcHydration)
	require.True(t, payload.TrackingPlanValidation)

	// destination-specific metadata must be stripped: a forked job fans out to multiple
	// destinations, re-hydrated per consumer at drain time.
	require.Empty(t, payload.Metadata.DestinationID)
	require.Empty(t, payload.Metadata.DestinationName)
	require.Empty(t, payload.Metadata.DestinationType)
	require.Empty(t, payload.Metadata.DestinationDefinitionID)
	require.Empty(t, payload.Metadata.TransformationID)
	require.Empty(t, payload.Metadata.TransformationVersionID)
	// the caller's event metadata must not be mutated (newForkedJob works on a copy)
	require.Equal(t, "should-not-appear", event.Metadata.DestinationID)

	// the parent gw job's params are reused verbatim (carrying source_job_run_id etc.
	// for the eventual rsources resolution), never a synthetic copy
	require.JSONEq(t, string(gwParams), string(job.Parameters))
}

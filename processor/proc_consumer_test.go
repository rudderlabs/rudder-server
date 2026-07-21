package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// newTestProcHandle builds a minimal *Handle wired only with what procRebuildStage
// reads: a live backend config (source→destinations, connections, libraries, credentials),
// a NOP logger and stats. No jobsdb, transformer or workerpool involved.
func newTestProcHandle() *Handle {
	proc := &Handle{}
	proc.statsFactory = stats.NOP
	proc.logger = logger.NOP
	proc.config.sourceIdDestinationMap = map[string][]backendconfig.DestinationT{}
	proc.config.connectionConfigMap = map[connection]backendconfig.Connection{}
	proc.config.workspaceLibrariesMap = map[string]backendconfig.LibrariesT{}
	proc.config.credentialsMap = map[string][]types.Credential{}
	return proc
}

func testDestination(id, name string, enabled bool) backendconfig.DestinationT { //nolint: unparam
	return backendconfig.DestinationT{
		ID:      id,
		Name:    name,
		Enabled: enabled,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:   "defID-" + id,
			Name: "WEBHOOK",
		},
	}
}

// newProcJob serializes a payload the way the (out-of-scope) gw-pool siphon would.
func newProcJob(jobID int64, payload procJobPayload) *jobsdb.JobT {
	b, err := jsonrs.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return &jobsdb.JobT{
		JobID:        jobID,
		UserID:       payload.Metadata.RudderID,
		EventPayload: b,
		Parameters:   []byte(`{}`),
	}
}

func procRebuildInput(jobs ...*jobsdb.JobT) subJob {
	return subJob{ctx: context.Background(), subJobs: jobs}
}

func TestProcRebuildStage(t *testing.T) {
	const (
		srcID = "src-1"
		dstID = "dest-1"
		wsID  = "ws-1"
	)

	t.Run("happy path re-hydrates destination and groups per (source,destination)", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.config.sourceIdDestinationMap[srcID] = []backendconfig.DestinationT{testDestination(dstID, "my-webhook", true)}
		proc.config.connectionConfigMap[connection{sourceID: srcID, destinationID: dstID}] = backendconfig.Connection{SourceID: srcID, DestinationID: dstID}
		proc.config.workspaceLibrariesMap[wsID] = backendconfig.LibrariesT{{VersionID: "lib-1"}}
		proc.config.credentialsMap[wsID] = []types.Credential{{Key: "k", Value: "v"}}

		job := newProcJob(1, procJobPayload{
			Message: types.SingularEventT{"type": "track", "event": "clicked"},
			Metadata: types.Metadata{
				SourceID: srcID, WorkspaceID: wsID, MessageID: "msg-1",
				RudderID: "user-1", ReceivedAt: "2026-07-21T10:00:00.000Z",
			},
			SrcHydration:           true,
			TrackingPlanValidation: true,
		})

		out, err := proc.procRebuildStage(dstID, procRebuildInput(job))
		require.NoError(t, err)

		key := getKeyFromSourceAndDest(srcID, dstID)
		require.Len(t, out.groupedEvents, 1)
		require.Contains(t, out.groupedEvents, key)
		require.Len(t, out.groupedEvents[key], 1)

		ev := out.groupedEvents[key][0]
		require.Equal(t, dstID, ev.Destination.ID)
		require.Equal(t, dstID, ev.Metadata.DestinationID)
		require.Equal(t, "my-webhook", ev.Metadata.DestinationName)
		require.Equal(t, "WEBHOOK", ev.Metadata.DestinationType)
		require.Equal(t, "defID-"+dstID, ev.Metadata.DestinationDefinitionID)
		require.Equal(t, srcID, ev.Connection.SourceID)
		require.Equal(t, []backendconfig.LibraryT(proc.config.workspaceLibrariesMap[wsID]), ev.Libraries)
		require.Equal(t, proc.config.credentialsMap[wsID], ev.Credentials)

		require.Equal(t, 1, out.totalEvents)
		require.Len(t, out.statusList, 1)
		require.Equal(t, jobsdb.Succeeded.State, out.statusList[0].JobState)
		require.Equal(t, dstID, out.statusList[0].Consumer)
		require.Contains(t, out.eventsByMessageID, "msg-1")
		require.Equal(t, SourcePipelineSteps{srcHydration: true, trackingPlanValidation: true}, out.srcPipelineSteps[SourceIDT(srcID)])
		require.Nil(t, out.trackedUsersReports, "tracked users stay in gw pool")
	})

	t.Run("destination metadata is rehydrated even when the persisted metadata has no destination_id", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.config.sourceIdDestinationMap[srcID] = []backendconfig.DestinationT{testDestination(dstID, "my-webhook", true)}

		// Persisted metadata deliberately carries NO destination fields (the siphon does
		// not store them); the destination comes from the partition/consumer instead.
		job := newProcJob(1, procJobPayload{
			Message:  types.SingularEventT{"type": "track"},
			Metadata: types.Metadata{SourceID: srcID, WorkspaceID: wsID, MessageID: "msg-1"},
		})
		require.Contains(t, string(job.EventPayload), `"destinationId":""`, "precondition: payload's destination_id is empty")

		out, err := proc.procRebuildStage(dstID, procRebuildInput(job))
		require.NoError(t, err)

		ev := out.groupedEvents[getKeyFromSourceAndDest(srcID, dstID)][0]
		require.Equal(t, dstID, ev.Metadata.DestinationID)
		require.Equal(t, "my-webhook", ev.Metadata.DestinationName)
		require.Equal(t, "WEBHOOK", ev.Metadata.DestinationType)
		require.Equal(t, "defID-"+dstID, ev.Metadata.DestinationDefinitionID)
	})

	t.Run("config drift: disabled destination is dropped (filtered), not paniced", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.config.sourceIdDestinationMap[srcID] = []backendconfig.DestinationT{testDestination(dstID, "my-webhook", false)}

		job := newProcJob(1, procJobPayload{Metadata: types.Metadata{SourceID: srcID, MessageID: "msg-1"}})
		out, err := proc.procRebuildStage(dstID, procRebuildInput(job))
		require.NoError(t, err)

		require.Empty(t, out.groupedEvents)
		require.Len(t, out.statusList, 1)
		require.Equal(t, jobsdb.Filtered.State, out.statusList[0].JobState)
		require.Equal(t, dstID, out.statusList[0].Consumer)
	})

	t.Run("config drift: deleted destination is dropped (filtered)", func(t *testing.T) {
		proc := newTestProcHandle() // no destinations at all

		job := newProcJob(1, procJobPayload{Metadata: types.Metadata{SourceID: srcID, MessageID: "msg-1"}})
		out, err := proc.procRebuildStage(dstID, procRebuildInput(job))
		require.NoError(t, err)

		require.Empty(t, out.groupedEvents)
		require.Len(t, out.statusList, 1)
		require.Equal(t, jobsdb.Filtered.State, out.statusList[0].JobState)
	})

	t.Run("unparseable payload is aborted, not paniced", func(t *testing.T) {
		proc := newTestProcHandle()

		job := &jobsdb.JobT{JobID: 1, EventPayload: []byte(`{not json`), Parameters: []byte(`{}`)}
		out, err := proc.procRebuildStage(dstID, procRebuildInput(job))
		require.NoError(t, err)

		require.Empty(t, out.groupedEvents)
		require.Equal(t, 0, out.totalEvents)
		require.Len(t, out.statusList, 1)
		require.Equal(t, jobsdb.Aborted.State, out.statusList[0].JobState)
	})

	t.Run("multiple sources for the same destination are grouped separately", func(t *testing.T) {
		proc := newTestProcHandle()
		proc.config.sourceIdDestinationMap["src-1"] = []backendconfig.DestinationT{testDestination(dstID, "wh", true)}
		proc.config.sourceIdDestinationMap["src-2"] = []backendconfig.DestinationT{testDestination(dstID, "wh", true)}

		job1 := newProcJob(1, procJobPayload{Metadata: types.Metadata{SourceID: "src-1", MessageID: "m1"}})
		job2 := newProcJob(2, procJobPayload{Metadata: types.Metadata{SourceID: "src-2", MessageID: "m2"}})

		out, err := proc.procRebuildStage(dstID, procRebuildInput(job1, job2))
		require.NoError(t, err)

		require.Len(t, out.groupedEvents, 2)
		require.Contains(t, out.groupedEvents, getKeyFromSourceAndDest("src-1", dstID))
		require.Contains(t, out.groupedEvents, getKeyFromSourceAndDest("src-2", dstID))
		require.Len(t, out.statusList, 2)
	})
}

package processor

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// forkedJobCustomVal is the CustomVal carried by an intermediate (proc) job. A single
// forked job fans out to multiple destinations of possibly different types, so no single
// destination type applies.
const forkedJobCustomVal = "MULTI"

// shouldForkDestination reports whether events for the given destination should be
// siphoned to the intermediate (proc) jobsdb instead of being transformed inline in the
// gw pool. It is gated on procDB being configured and resolved per destination via
// hierarchical config, falling back to an instance-wide default:
//
//	Processor.DestinationIsolation.enabledDestinations.<destinationID>
//	Processor.DestinationIsolation.enabledDestinations.all
func (proc *Handle) shouldForkDestination(destinationID string) bool {
	if proc.procDB == nil {
		return false
	}
	// Fast path: the resolved value is cached permanently, so the common case is a
	// lock-free read under a shared lock (a nil map reads safely as a miss).
	proc.destinationIsolationMu.RLock()
	enabled, ok := proc.destinationIsolationCache[destinationID]
	proc.destinationIsolationMu.RUnlock()
	if ok {
		return enabled
	}

	proc.destinationIsolationMu.Lock()
	defer proc.destinationIsolationMu.Unlock()
	// Re-check: another goroutine may have resolved this destination between the two locks.
	if enabled, ok := proc.destinationIsolationCache[destinationID]; ok {
		return enabled
	}
	if proc.destinationIsolationCache == nil {
		proc.destinationIsolationCache = make(map[string]bool)
	}
	enabled = proc.conf.GetBoolVar(false,
		"Processor.DestinationIsolation.enabledDestinations."+destinationID,
		"Processor.DestinationIsolation.enabledDestinations.all",
	)
	proc.destinationIsolationCache[destinationID] = enabled
	return enabled
}

// newForkedJob builds a single intermediate (proc) job for one source event fanned out to
// forkedDestIDs. The payload carries the source-level message + metadata (destination is
// re-hydrated per consumer at drain time, see procRebuildStage), the forked destination
// IDs are stored as the job's consumers, and gwParams are the parent gateway job's
// parameters reused verbatim so the proc job resolves back to the same source/jobRun.
func (proc *Handle) newForkedJob(event *types.TransformerEvent, forkedDestIDs []string, steps SourcePipelineSteps, gwParams json.RawMessage) (*jobsdb.JobT, error) {
	// metadata is a struct copy, so clearing the destination-specific fields here does not
	// affect the caller's event. A forked job fans out to multiple destinations of possibly
	// different types; the destination is re-hydrated per consumer at drain time
	// (procRebuildStage), so no single destination's identity may be baked into the payload.
	metadata := event.Metadata
	metadata.DestinationID = ""
	metadata.DestinationName = ""
	metadata.DestinationType = ""
	metadata.DestinationDefinitionID = ""
	metadata.TransformationID = ""
	metadata.TransformationVersionID = ""
	payload, err := jsonrs.Marshal(procJobPayload{
		Message:                event.Message,
		Metadata:               metadata,
		SrcHydration:           steps.srcHydration,
		TrackingPlanValidation: steps.trackingPlanValidation,
	})
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return &jobsdb.JobT{
		UUID:         uuid.New(),
		UserID:       metadata.RudderID,
		CreatedAt:    now,
		ExpireAt:     now,
		CustomVal:    forkedJobCustomVal,
		EventCount:   1,
		EventPayload: payload,
		Parameters:   gwParams,
		WorkspaceId:  metadata.WorkspaceID,
		PartitionID:  metadata.PartitionID,
		Consumers:    forkedDestIDs,
	}, nil
}

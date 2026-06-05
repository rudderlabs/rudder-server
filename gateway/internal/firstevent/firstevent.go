package firstevent

import (
	"sync"

	"github.com/rudderlabs/analytics-go"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// Event name as defined in the data-gov catalog (pipeline domain).
const eventName = "pipelineFirstEventReceived"

// Tracker emits the pipelineFirstEventReceived activation milestone the first time a source
// ingests an event. Dedup is per-pod (in-memory); the warehouse/Lookout collapses to
// first-occurrence-per-workspace. Disabled unless a product-analytics writekey is configured,
// so it is a no-op in environments without that key.
type Tracker struct {
	enabled bool
	client  analytics.Client
	fired   sync.Map // sourceID -> struct{}
	log     logger.Logger
}

// NewTracker reads the product-analytics destination from config. Separate from the diagnostics
// write key on purpose: this event must land in the funnel dataset the data-gov plan governs.
func NewTracker(conf *config.Config, log logger.Logger) *Tracker {
	writeKey := conf.GetStringVar("", "PipelineActivation.writekey")
	endpoint := conf.GetStringVar("https://rudderstack-dataplane.rudderstack.com", "PipelineActivation.endpoint")
	enabled := conf.GetBoolVar(false, "PipelineActivation.enabled") && writeKey != ""

	t := &Tracker{enabled: enabled, log: log.Child("firstevent")}
	if enabled {
		t.client = analytics.New(writeKey, endpoint)
	}
	return t
}

// ShouldFire reports whether this is the first event seen for sourceID on this pod, marking it
// fired as a side effect. Cheap (atomic) and allocation-free, so the hot ingest path can guard
// the source-config lookup behind it. Returns false when disabled. Call Track right after a true.
func (t *Tracker) ShouldFire(sourceID string) bool {
	if !t.enabled || sourceID == "" {
		return false
	}
	_, alreadyFired := t.fired.LoadOrStore(sourceID, struct{}{})
	return !alreadyFired
}

// Track emits pipelineFirstEventReceived. Only call after ShouldFire returned true. The SDK
// enqueues asynchronously, so this is safe to call from the ingest path.
// isFirstSourceEventInWorkspace is always true here (we only emit on the first seen event);
// the warehouse collapses cross-pod duplicates to the first occurrence per workspace.
func (t *Tracker) Track(sourceID, workspaceID, sourceType, sourceCategory, receivedAt string) {
	if t.client == nil {
		return
	}
	if err := t.client.Enqueue(analytics.Track{
		Event:       eventName,
		AnonymousId: workspaceID,
		Properties: map[string]any{
			"workspaceId":                   workspaceID,
			"sourceId":                      sourceID,
			"sourceType":                    sourceType,
			"sourceCategory":                sourceCategory,
			"isFirstSourceEventInWorkspace": true,
			"receivedAt":                    receivedAt,
		},
	}); err != nil {
		t.log.Warnn("failed to enqueue pipelineFirstEventReceived", logger.NewStringField("sourceId", sourceID))
	}
}

package types

import (
	"reflect"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// SingularEventT single event structure
type SingularEventT map[string]interface{}

// GetRudderEventVal returns the value corresponding to the key in the message structure
func GetRudderEventVal(key string, rudderEvent SingularEventT) (interface{}, bool) {
	rudderVal, ok := rudderEvent[key]
	if !ok {
		return nil, false
	}
	return rudderVal, true
}

type SingularEventWithReceivedAt struct {
	SingularEvent SingularEventT
	ReceivedAt    time.Time
}

// GatewayBatchRequest batch request structure
type GatewayBatchRequest struct {
	Batch      []SingularEventT `json:"batch"`
	RequestIP  string           `json:"requestIP"`
	ReceivedAt time.Time        `json:"receivedAt"`
}

type TransformerEvent struct {
	Message     SingularEventT             `json:"message"`
	Metadata    Metadata                   `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
	Connection  backendconfig.Connection   `json:"connection"`
	Libraries   []backendconfig.LibraryT   `json:"libraries"`
	Credentials []Credential               `json:"credentials"`
}

// GetVersionsOnly removes the connection and credentials from the event
// along with pruning the destination to only include the transformation versionID
// before sending it to the transformer thereby reducing the payload size
func (e *TransformerEvent) GetVersionsOnly() *TransformerEvent {
	tmCopy := *e
	transformations := make([]backendconfig.TransformationT, 0, len(e.Destination.Transformations))
	for _, t := range e.Destination.Transformations {
		transformations = append(transformations, backendconfig.TransformationT{
			VersionID: t.VersionID,
		})
	}
	tmCopy.Destination = backendconfig.DestinationT{
		Transformations: transformations,
	}
	tmCopy.Connection = backendconfig.Connection{}
	return &tmCopy
}

type Credential struct {
	ID       string `json:"id"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	IsSecret bool   `json:"isSecret"`
}

type Metadata struct {
	SourceID            string                            `json:"sourceId"`
	SourceName          string                            `json:"sourceName"`
	OriginalSourceID    string                            `json:"originalSourceId"`
	WorkspaceID         string                            `json:"workspaceId"`
	Namespace           string                            `json:"namespace"`
	InstanceID          string                            `json:"instanceId"`
	SourceType          string                            `json:"sourceType"`
	SourceCategory      string                            `json:"sourceCategory"`
	TrackingPlanID      string                            `json:"trackingPlanId"`
	TrackingPlanVersion int                               `json:"trackingPlanVersion"`
	SourceTpConfig      map[string]map[string]interface{} `json:"sourceTpConfig"`
	MergedTpConfig      map[string]interface{}            `json:"mergedTpConfig"`
	DestinationID       string                            `json:"destinationId"`
	JobID               int64                             `json:"jobId"`
	SourceJobID         string                            `json:"sourceJobId"`
	SourceJobRunID      string                            `json:"sourceJobRunId"`
	SourceTaskRunID     string                            `json:"sourceTaskRunId"`
	RecordID            interface{}                       `json:"recordId"`
	DestinationType     string                            `json:"destinationType"`
	DestinationName     string                            `json:"destinationName"`
	MessageID           string                            `json:"messageId"`
	OAuthAccessToken    string                            `json:"oauthAccessToken"`
	TraceParent         string                            `json:"traceparent"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs              []string `json:"messageIds"`
	RudderID                string   `json:"rudderId"`
	ReceivedAt              string   `json:"receivedAt"`
	EventName               string   `json:"eventName"`
	EventType               string   `json:"eventType"`
	SourceDefinitionID      string   `json:"sourceDefinitionId"`
	DestinationDefinitionID string   `json:"destinationDefinitionId"`
	TransformationID        string   `json:"transformationId"`
	TransformationVersionID string   `json:"transformationVersionId"`
	SourceDefinitionType    string   `json:"-"`
}

func (m Metadata) GetMessagesIDs() []string {
	if len(m.MessageIDs) > 0 {
		return m.MessageIDs
	}
	return []string{m.MessageID}
}

type TransformerResponse struct {
	// Not marking this Singular Event, since this not a RudderEvent
	Output           map[string]interface{} `json:"output"`
	Metadata         Metadata               `json:"metadata"`
	StatusCode       int                    `json:"statusCode"`
	Error            string                 `json:"error"`
	ValidationErrors []ValidationError      `json:"validationErrors"`
	StatTags         map[string]string      `json:"statTags"`
}

type ValidationError struct {
	Type     string            `json:"type"`
	Message  string            `json:"message"`
	Meta     map[string]string `json:"meta"`
	Property string            `json:"property"`
}

// Response represents a Transformer response
type Response struct {
	Events       []TransformerResponse
	FailedEvents []TransformerResponse
}

// Equal compares two Response structs and returns true if they are equal
// regardless of the order of elements in the Events and FailedEvents slices
func (r *Response) Equal(v *Response) bool {
	// Check if both are nil or both are not nil
	if (r == nil) != (v == nil) {
		return false
	}

	// If both are nil, they're equal
	if r == nil {
		return true
	}

	// Check if Events slices have the same length
	if len(r.Events) != len(v.Events) {
		return false
	}

	// Check if FailedEvents slices have the same length
	if len(r.FailedEvents) != len(v.FailedEvents) {
		return false
	}

	// Check if each event in r.Events is present in v.Events
	if !containsSameEvents(r.Events, v.Events) {
		return false
	}

	// Check if each failed event in r.FailedEvents is present in v.FailedEvents
	if !containsSameEvents(r.FailedEvents, v.FailedEvents) {
		return false
	}

	return true
}

// containsSameEvents checks if two slices contain the same TransformerResponse elements,
// regardless of order
func containsSameEvents(a, b []TransformerResponse) bool {
	// Create a map to mark events in b that have been matched
	matched := make([]bool, len(b))

	// For each event in a, find a matching event in b
	for _, eventA := range a {
		found := false
		for j, eventB := range b {
			if !matched[j] && eventsEqual(eventA, eventB) {
				matched[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Ensure all events in b have been matched
	for _, m := range matched {
		if !m {
			return false
		}
	}

	return true
}

// eventsEqual checks if two TransformerResponse objects are equal
func eventsEqual(a, b TransformerResponse) bool {
	// TODO we might have to skip fields containing dates and times, double check this.
	return reflect.DeepEqual(a, b)
}

type EventParams struct {
	SourceJobRunId  string `json:"source_job_run_id"`
	SourceId        string `json:"source_id"`
	SourceTaskRunId string `json:"source_task_run_id"`
	TraceParent     string `json:"traceparent"`
	DestinationID   string `json:"destination_id"`
}

type TransformerMetricLabels struct {
	Endpoint         string // hostname of the service
	DestinationType  string // BQ, etc.
	SourceType       string // webhook
	Language         string // js, python
	Stage            string // processor, router, gateway
	WorkspaceID      string // workspace identifier
	SourceID         string // source identifier
	DestinationID    string // destination identifier
	TransformationID string // transformation identifier
	Mirroring        bool
}

// ToStatsTag converts transformerMetricLabels to stats.Tags and includes legacy tags for backwards compatibility
func (t TransformerMetricLabels) ToStatsTag() stats.Tags {
	tags := stats.Tags{
		"endpoint":         t.Endpoint,
		"destinationType":  t.DestinationType,
		"sourceType":       t.SourceType,
		"language":         t.Language,
		"stage":            t.Stage,
		"workspaceId":      t.WorkspaceID,
		"destinationId":    t.DestinationID,
		"sourceId":         t.SourceID,
		"transformationId": t.TransformationID,
		"mirroring":        strconv.FormatBool(t.Mirroring),

		// Legacy tags: to be removed
		"dest_type": t.DestinationType,
		"dest_id":   t.DestinationID,
		"src_id":    t.SourceID,
	}

	return tags
}

// ToLoggerFields converts the metric labels to a slice of logger.Fields
func (t TransformerMetricLabels) ToLoggerFields() []logger.Field {
	return []logger.Field{
		logger.NewStringField("endpoint", t.Endpoint),
		logger.NewStringField("stage", t.Stage),
		obskit.DestinationType(t.DestinationType),
		obskit.SourceType(t.SourceType),
		obskit.WorkspaceID(t.WorkspaceID),
		obskit.DestinationID(t.DestinationID),
		obskit.SourceID(t.SourceID),
		logger.NewStringField("transformationId", t.TransformationID),
		logger.NewBoolField("mirroring", t.Mirroring),
	}
}

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

// UserTransformerEvent is the event sent to the user transformer, which is similar to a
// [TransformerEvent] but without the connection and with a simplified destination structure
type UserTransformerEvent struct {
	Message     SingularEventT `json:"message"`
	Metadata    Metadata       `json:"metadata"`
	Destination struct {
		Transformations []struct {
			VersionID string
		}
	} `json:"destination"`
	Libraries   []backendconfig.LibraryT `json:"libraries,omitempty"`
	Credentials []Credential             `json:"credentials,omitempty"`
}

// CompactedTransformerEvent is similar to a [TransformerEvent] but without the connection and destination fields
type CompactedTransformerEvent struct {
	Message     SingularEventT           `json:"message"`
	Metadata    Metadata                 `json:"metadata"`
	Libraries   []backendconfig.LibraryT `json:"libraries,omitempty"`
	Credentials []Credential             `json:"credentials,omitempty"`
}

// CompactedTransformRequest is the request structure for a compacted transformer request payload.
// It contains a list of [CompactedTransformerEvent]s and two lookup maps,
// one for destinations and one for connections.
//
// The destinations and connections are used to look up the actual destination and connection
// objects based on their IDs, which are included in the [CompactedTransformerEvent]s.
//
// This allows for a more compact representation of the request payload, as the full destination
// and connection objects do not need to be included in each event.
type CompactedTransformRequest struct {
	Input        []CompactedTransformerEvent           `json:"input"`
	Destinations map[string]backendconfig.DestinationT `json:"destinations"`
	Connections  map[string]backendconfig.Connection   `json:"connections"`
}

func (ctr *CompactedTransformRequest) ToTransformerEvents() []TransformerEvent {
	events := make([]TransformerEvent, len(ctr.Input))
	for i, input := range ctr.Input {
		events[i] = TransformerEvent{
			Message:     input.Message,
			Metadata:    input.Metadata,
			Destination: ctr.Destinations[input.Metadata.DestinationID],
			Connection:  ctr.Connections[input.Metadata.SourceID+":"+input.Metadata.DestinationID],
			Libraries:   input.Libraries,
			Credentials: input.Credentials,
		}
	}
	return events
}

// ToUserTransformerEvent removes the connection from the event
// along with pruning the destination to only include the transformation ID and VersionID
// before sending it to the transformer thereby reducing the payload size
func (e *TransformerEvent) ToUserTransformerEvent() *UserTransformerEvent {
	ute := &UserTransformerEvent{
		Message:     e.Message,
		Metadata:    e.Metadata,
		Libraries:   e.Libraries,
		Credentials: e.Credentials,
	}
	for _, t := range e.Destination.Transformations {
		ute.Destination.Transformations = append(ute.Destination.Transformations, struct {
			VersionID string
		}{
			VersionID: t.VersionID,
		})
	}
	return ute
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
	Namespace           string                            `json:"namespace,omitempty"`
	InstanceID          string                            `json:"instanceId,omitempty"`
	SourceType          string                            `json:"sourceType"`
	SourceCategory      string                            `json:"sourceCategory"`
	TrackingPlanID      string                            `json:"trackingPlanId,omitempty"`
	TrackingPlanVersion int                               `json:"trackingPlanVersion,omitempty"`
	SourceTpConfig      map[string]map[string]interface{} `json:"sourceTpConfig,omitempty"`
	MergedTpConfig      map[string]interface{}            `json:"mergedTpConfig,omitempty"`
	DestinationID       string                            `json:"destinationId"`
	JobID               int64                             `json:"jobId"`
	SourceJobID         string                            `json:"sourceJobId,omitempty"`
	SourceJobRunID      string                            `json:"sourceJobRunId,omitempty"`
	SourceTaskRunID     string                            `json:"sourceTaskRunId,omitempty"`
	RecordID            interface{}                       `json:"recordId,omitempty"`
	DestinationType     string                            `json:"destinationType"`
	DestinationName     string                            `json:"destinationName"`
	MessageID           string                            `json:"messageId"`
	OAuthAccessToken    string                            `json:"oauthAccessToken,omitempty"`
	TraceParent         string                            `json:"traceparent,omitempty"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs              []string `json:"messageIds,omitempty"`
	RudderID                string   `json:"rudderId,omitempty"`
	ReceivedAt              string   `json:"receivedAt,omitempty"`
	EventName               string   `json:"eventName,omitempty"`
	EventType               string   `json:"eventType,omitempty"`
	SourceDefinitionID      string   `json:"sourceDefinitionId,omitempty"`
	DestinationDefinitionID string   `json:"destinationDefinitionId,omitempty"`
	TransformationID        string   `json:"transformationId,omitempty"`
	TransformationVersionID string   `json:"transformationVersionId,omitempty"`
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

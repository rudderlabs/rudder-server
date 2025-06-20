package types

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

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

// TrackingPlanValidationEvent is the event sent to the trackingplan transformer,
// whose fields are a subset of the [TransformerEvent]
type TrackingPlanValidationEvent struct {
	Message  SingularEventT `json:"message"`
	Metadata Metadata       `json:"metadata"`
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

// ToTrackingPlanValidationEvent only keeps the message and metadata fields from the event
// before sending it to the trackingplan validator thereby reducing the payload size
func (e *TransformerEvent) ToTrackingPlanValidationEvent() *TrackingPlanValidationEvent {
	return &TrackingPlanValidationEvent{
		Message:  e.Message,
		Metadata: e.Metadata,
	}
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
func (r *Response) Equal(v *Response) (string, bool) {
	if len(r.Events) != len(v.Events) {
		return fmt.Sprintf("Expected Events length %d, got %d", len(r.Events), len(v.Events)), false
	}

	if len(r.FailedEvents) != len(v.FailedEvents) {
		return fmt.Sprintf("Expected FailedEvents length %d, got %d", len(r.FailedEvents), len(v.FailedEvents)), false
	}

	extraA, extraB := diffLists(r.Events, v.Events)
	if len(extraA) > 0 || len(extraB) > 0 {
		diff := formatListDiff(r.Events, v.Events, extraA, extraB)
		return diff, false
	}

	extraA, extraB = diffLists(r.FailedEvents, v.FailedEvents)
	if len(extraA) > 0 || len(extraB) > 0 {
		diff := formatListDiff(r.FailedEvents, v.FailedEvents, extraA, extraB)
		return diff, false
	}

	return "", true
}

type EventParams struct {
	SourceJobRunId      string `json:"source_job_run_id"`
	SourceId            string `json:"source_id"`
	SourceTaskRunId     string `json:"source_task_run_id"`
	TraceParent         string `json:"traceparent"`
	DestinationID       string `json:"destination_id"`
	IsBot               bool   `json:"is_bot,omitempty"`
	BotName             string `json:"bot_name,omitempty"`
	BotURL              string `json:"bot_url,omitempty"`
	BotIsInvalidBrowser bool   `json:"bot_is_invalid_browser,omitempty"`
	BotAction           string `json:"bot_action,omitempty"`
	IsEventBlocked      bool   `json:"is_event_blocked,omitempty"`
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

func diffLists(listA, listB interface{}) (extraA, extraB []interface{}) {
	aValue := reflect.ValueOf(listA)
	bValue := reflect.ValueOf(listB)

	aLen := aValue.Len()
	bLen := bValue.Len()

	// Mark indexes in bValue that we already used
	visited := make([]bool, bLen)
	for i := 0; i < aLen; i++ {
		element := aValue.Index(i).Interface()
		found := false
		for j := 0; j < bLen; j++ {
			if visited[j] {
				continue
			}
			if assert.ObjectsAreEqual(bValue.Index(j).Interface(), element) {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			extraA = append(extraA, element)
		}
	}

	for j := 0; j < bLen; j++ {
		if visited[j] {
			continue
		}
		extraB = append(extraB, bValue.Index(j).Interface())
	}

	return
}

var spewConfig = spew.ConfigState{
	Indent:                  " ",
	DisablePointerAddresses: true,
	DisableCapacities:       true,
	SortKeys:                true,
	DisableMethods:          true,
	MaxDepth:                10,
}

func formatListDiff(listA, listB interface{}, extraA, extraB []interface{}) string {
	var msg bytes.Buffer

	msg.WriteString("elements differ")
	if len(extraA) > 0 {
		msg.WriteString("\n\nextra elements in list A:\n")
		msg.WriteString(spewConfig.Sdump(extraA))
	}
	if len(extraB) > 0 {
		msg.WriteString("\n\nextra elements in list B:\n")
		msg.WriteString(spewConfig.Sdump(extraB))
	}
	msg.WriteString("\n\nlistA:\n")
	msg.WriteString(spewConfig.Sdump(listA))
	msg.WriteString("\n\nlistB:\n")
	msg.WriteString(spewConfig.Sdump(listB))

	return msg.String()
}

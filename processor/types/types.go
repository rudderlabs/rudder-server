package types

import (
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
	}
}

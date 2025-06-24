package types

import (
	"database/sql"
	"encoding/json"
	"time"
)

type SyncerConfig struct {
	ConnInfo string
	Label    string
}

const (
	CoreReportingLabel      = "core"
	WarehouseReportingLabel = "warehouse"

	SupportedTransformerApiVersion = 2

	DefaultReportingEnabled = true
	DefaultReplayEnabled    = false
)

const (
	DiffStatus = "diff"

	// Module names
	DEDUPLICATION          = "deduplication"
	GATEWAY                = "gateway"
	DESTINATION_FILTER     = "destination_filter"
	EVENT_BLOCKING         = "event_blocking"
	TRACKINGPLAN_VALIDATOR = "tracking_plan_validator"
	USER_TRANSFORMER       = "user_transformer"
	EVENT_FILTER           = "event_filter"
	DEST_TRANSFORMER       = "dest_transformer"
	ROUTER                 = "router"
	BATCH_ROUTER           = "batch_router"
	WAREHOUSE              = "warehouse"
)

var (
	// Tracking plan validation states
	SUCCEEDED_WITHOUT_VIOLATIONS = "succeeded_without_violations"
	SUCCEEDED_WITH_VIOLATIONS    = "succeeded_with_violations"
)

type SyncSource struct {
	SyncerConfig
	DbHandle *sql.DB
}

type StatusDetail struct {
	Status         string            `json:"state"`
	Count          int64             `json:"count"`
	StatusCode     int               `json:"statusCode"`
	SampleResponse string            `json:"sampleResponse"`
	SampleEvent    json.RawMessage   `json:"sampleEvent"`
	EventName      string            `json:"eventName"`
	EventType      string            `json:"eventType"`
	ErrorType      string            `json:"errorType"`
	ViolationCount int64             `json:"violationCount"`
	ErrorDetails   ErrorDetails      `json:"-"`
	StatTags       map[string]string `json:"-"`
	FailedMessages []*FailedMessage  `json:"-"`
}

type ErrorDetails struct {
	Code    string
	Message string
}

type FailedMessage struct {
	MessageID  string    `json:"messageId"`
	ReceivedAt time.Time `json:"receivedAt"`
}

type ReportByStatus struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetail *StatusDetail
}

type InstanceDetails struct {
	WorkspaceID string `json:"workspaceId"`
	Namespace   string `json:"namespace"`
	InstanceID  string `json:"instanceId"`
}

type ReportMetadata struct {
	ReportedAt        int64 `json:"reportedAt"`
	SampleEventBucket int64 `json:"bucket"`
}

type Metric struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetails []*StatusDetail `json:"reports"`
}

// EDMetric => ErrorDetailMetric
type EDConnectionDetails struct {
	SourceID                string `json:"sourceId"`
	DestinationID           string `json:"destinationId"`
	SourceDefinitionId      string `json:"sourceDefinitionId"`
	DestinationDefinitionId string `json:"destinationDefinitionId"`
	DestType                string `json:"destinationDefinitionName"`
}

type EDInstanceDetails struct {
	WorkspaceID string `json:"workspaceId"`
	Namespace   string `json:"namespace"`
	InstanceID  string `json:"-"`
}

type EDErrorDetailsKey struct {
	StatusCode   int    `json:"statusCode"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
	EventType    string `json:"eventType"`
	EventName    string `json:"eventName"`
}

type EDErrorDetails struct {
	EDErrorDetailsKey
	SampleResponse string          `json:"sampleResponse"`
	SampleEvent    json.RawMessage `json:"sampleEvent"`
	ErrorCount     int64           `json:"count"`
}

type EDReportsDB struct {
	EDErrorDetails
	EDInstanceDetails
	EDConnectionDetails
	ReportMetadata

	PU    string `json:"reportedBy"`
	Count int64  `json:"-"`
}

type EDReportMapValue struct {
	Count          int64
	SampleResponse string
	SampleEvent    json.RawMessage
}

// EDMetric The structure in which the error detail data is being sent to reporting service
type EDMetric struct {
	EDInstanceDetails
	PU string `json:"reportedBy"`

	ReportMetadata

	EDConnectionDetails

	Errors []EDErrorDetails `json:"errors"`

	Count int64 `json:"-"`
}

type ConnectionDetails struct {
	SourceID                string `json:"sourceId"`
	DestinationID           string `json:"destinationId"`
	SourceTaskRunID         string `json:"sourceTaskRunId"`
	SourceJobID             string `json:"sourceJobId"`
	SourceJobRunID          string `json:"sourceJobRunId"`
	SourceDefinitionID      string `json:"sourceDefinitionId"`
	DestinationDefinitionID string `json:"DestinationDefinitionId"`
	SourceCategory          string `json:"sourceCategory"`
	TransformationID        string `json:"transformationId"`
	TransformationVersionID string `json:"transformationVersionId"`
	TrackingPlanID          string `json:"trackingPlanId"`
	TrackingPlanVersion     int    `json:"trackingPlanVersion"`
}
type PUDetails struct {
	InPU       string `json:"inReportedBy"`
	PU         string `json:"reportedBy"`
	TerminalPU bool   `json:"terminalState"`
	InitialPU  bool   `json:"initialState"`
	NextPU     string `json:nextStage`
}

type PUReportedMetric struct {
	ConnectionDetails
	PUDetails
	StatusDetail *StatusDetail
}

func CreatePUDetails(inPU, pu string, terminalPU, initialPU bool) *PUDetails {
	return &PUDetails{
		InPU:       inPU,
		PU:         pu,
		TerminalPU: terminalPU,
		InitialPU:  initialPU,
	}
}

func AssertSameKeys[V1, V2 any](m1 map[string]V1, m2 map[string]V2) {
	if len(m1) != len(m2) {
		panic("maps length don't match") // TODO improve msg
	}
	for k := range m1 {
		if _, ok := m2[k]; !ok {
			panic("key in map1 not found in map2") // TODO improve msg
		}
	}
}

type SourceLabels struct {
	SourceCategory     string `json:"sourceCategory"`
	SourceDefinitionID string `json:"sourceDefinitionId"`
	SourceID           string `json:"sourceId"`
	SourceJobID        string `json:"sourceJobId"`
	SourceJobRunID     string `json:"sourceJobRunId"`
	SourceTaskRunID    string `json:"sourceTaskRunId"`
}

type DestinationLabels struct {
	DestinationID           string `json:"destinationId"`
	DestinationDefinitionID string `json:"destinationDefinitionId"`
}

type TransformationLabels struct {
	TransformationID        string `json:"transformationId"`
	TransformationVersionID string `json:"transformationVersionId"`
}

type TrackingPlanLabels struct {
	TrackingPlanID      string `json:"trackingPlanId"`
	TrackingPlanVersion int    `json:"trackingPlanVersion"`
}

type ConnectionLabels struct {
	SourceLabels         SourceLabels         `json:"sourceLabels"`
	DestinationLabels    DestinationLabels    `json:"destinationLabels"`
	TransformationLabels TransformationLabels `json:"transformationLabels"`
	TrackingPlanLabels   TrackingPlanLabels   `json:"trackingPlanLabels"`
}

type EventLabels struct {
	EventType string `json:"eventType"`
	EventName string `json:"eventName"`
}

type StatusLabels struct {
	StatusCode int    `json:"statusCode"`
	Status     string `json:"status"`
	ErrorType  string `json:"errorCode"`
}

type Event struct {
	ID      string
	Payload map[string]interface{}
}

type InMetricEvent struct {
	ConnectionLabels ConnectionLabels `json:"connectionLabels"`
	EventLabels      EventLabels      `json:"eventLabels"`
	Event            Event            `json:"event"`
}
type OutMetricEvent struct {
	ConnectionLabels ConnectionLabels `json:"connectionLabels"`
	EventLabels      EventLabels      `json:"eventLabels"`
	StatusLabels     StatusLabels     `json:"statusLabels"`
	Event            Event            `json:"event"`
}

type StageDetails struct {
	Stage    string `json:"stage"`
	Initial  bool   `json:"initial"`
	Terminal bool   `json:"terminal"`
}

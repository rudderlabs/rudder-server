package types

import (
	"database/sql"
	"encoding/json"
)

type Config struct {
	ClientName string
	ConnInfo   string
}

const (
	CoreReportingClient      = "core"
	WarehouseReportingClient = "warehouse"

	SupportedTransformerApiVersion = 2

	DefaultReportingEnabled = true
	DefaultReplayEnabled    = false
)

var (
	DiffStatus = "diff"

	// Module names
	GATEWAY                = "gateway"
	DESTINATION_FILTER     = "destination_filter"
	TRACKINGPLAN_VALIDATOR = "tracking_plan_validator"
	USER_TRANSFORMER       = "user_transformer"
	EVENT_FILTER           = "event_filter"
	DEST_TRANSFORMER       = "dest_transformer"
	ROUTER                 = "router"
	BATCH_ROUTER           = "batch_router"
	WAREHOUSE              = "warehouse"
)

type Client struct {
	Config
	DbHandle *sql.DB
}

type StatusDetail struct {
	Status         string          `json:"state"`
	Count          int64           `json:"count"`
	StatusCode     int             `json:"statusCode"`
	SampleResponse string          `json:"sampleResponse"`
	SampleEvent    json.RawMessage `json:"sampleEvent"`
	EventName      string          `json:"eventName"`
	EventType      string          `json:"eventType"`
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
	ReportedAt int64 `json:"reportedAt"`
}

type Metric struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetails []*StatusDetail `json:"reports"`
}

type ConnectionDetails struct {
	SourceID                string `json:"sourceId"`
	DestinationID           string `json:"destinationId"`
	SourceTaskRunID         string `json:"sourceTaskRunId"`
	SourceJobID             string `json:"sourceJobId"`
	SourceJobRunID          string `json:"sourceJobRunId"`
	SourceDefinitionId      string `json:"sourceDefinitionId"`
	DestinationDefinitionId string `string:"destinationDefinitionId"`
	SourceCategory          string `json:"sourceCategory"`
}
type PUDetails struct {
	InPU       string `json:"inReportedBy"`
	PU         string `json:"reportedBy"`
	TerminalPU bool   `json:"terminalState"`
	InitialPU  bool   `json:"initialState"`
}

type PUReportedMetric struct {
	ConnectionDetails
	PUDetails
	StatusDetail *StatusDetail
}

func CreateConnectionDetail(sid, did, strid, sjid, sjrid, sdid, ddid, sc string) *ConnectionDetails {
	return &ConnectionDetails{
		SourceID:                sid,
		DestinationID:           did,
		SourceTaskRunID:         strid,
		SourceJobID:             sjid,
		SourceJobRunID:          sjrid,
		SourceDefinitionId:      sdid,
		DestinationDefinitionId: ddid,
		SourceCategory:          sc,
	}
}

func CreateStatusDetail(status string, count int64, code int, resp string, event json.RawMessage, eventName, eventType string) *StatusDetail {
	return &StatusDetail{
		Status:         status,
		Count:          count,
		StatusCode:     code,
		SampleResponse: resp,
		SampleEvent:    event,
		EventName:      eventName,
		EventType:      eventType,
	}
}

func CreatePUDetails(inPU, pu string, terminalPU, initialPU bool) *PUDetails {
	return &PUDetails{
		InPU:       inPU,
		PU:         pu,
		TerminalPU: terminalPU,
		InitialPU:  initialPU,
	}
}

func AssertSameKeys(m1 map[string]*ConnectionDetails, m2 map[string]*StatusDetail) {
	if len(m1) != len(m2) {
		panic("maps length don't match") // TODO improve msg
	}
	for k := range m1 {
		if _, ok := m2[k]; !ok {
			panic("key in map1 not found in map2") // TODO improve msg
		}
	}
}

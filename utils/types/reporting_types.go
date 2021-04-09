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
	CORE_REPORTING_CLIENT      = "core"
	WAREHOUSE_REPORTING_CLIENT = "warehouse"
)

var (
	DiffStatus = "diff"

	//Module names
	GATEWAY          = "gateway"
	USER_TRANSFORMER = "user_transformer"
	DEST_TRANSFORMER = "dest_transformer"
	ROUTER           = "router"
	BATCH_ROUTER     = "batch_router"
	WAREHOUSE        = "warehouse"
)

type Client struct {
	Config
	DbHandle *sql.DB
}

type StatusDetail struct {
	Status         string          `json:"state"`
	Count          int64           `json:"count"`
	StatusCode     int             `json:"status_code"`
	SampleResponse string          `json:"sample_response"`
	SampleEvent    json.RawMessage `json:"sample_event"`
}

type ReportByStatus struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetail *StatusDetail
}

type InstanceDetails struct {
	WorksapceID string `json:"workspace_id"`
	Namespace   string `json:"namespace"`
	InstanceID  string `json:"instance_id"`
}

type ReportMetadata struct {
	ReportedMin int64 `json:"reported_min"`
}

type Metric struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetails []*StatusDetail `json:"reports"`
}

type ConnectionDetails struct {
	SourceID        string `json:"source_id"`
	DestinationID   string `json:"destination_id"`
	SourceBatchID   string `json:"source_batch_id"`
	SourceTaskID    string `json:"source_task_id"`
	SourceTaskRunID string `json:"source_task_run_id"`
	SourceJobID     string `json:"source_job_id"`
	SourceJobRunID  string `json:"source_job_run_id"`
}
type PUDetails struct {
	InPU       string `json:"in_reported_by"`
	PU         string `json:"reported_by"`
	TerminalPU bool   `json:"terminal_state"`
	InitialPU  bool   `json:"initial_state"`
}

type PUReportedMetric struct {
	ConnectionDetails
	PUDetails
	StatusDetail *StatusDetail
}

func CreateConnectionDetail(sid, did, sbid, stid, strid, sjid, sjrid string) *ConnectionDetails {
	return &ConnectionDetails{SourceID: sid,
		DestinationID:   did,
		SourceBatchID:   sbid,
		SourceTaskID:    stid,
		SourceTaskRunID: strid,
		SourceJobID:     sjid,
		SourceJobRunID:  sjrid}
}

func CreateStatusDetail(status string, count int64, code int, resp string, event json.RawMessage) *StatusDetail {
	return &StatusDetail{
		Status:         status,
		Count:          count,
		StatusCode:     code,
		SampleResponse: resp,
		SampleEvent:    event}
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
		panic("maps length don't match") //TODO improve msg
	}
	for k := range m1 {
		if _, ok := m2[k]; !ok {
			panic("key in map1 not found in map2") //TODO improve msg
		}
	}
}

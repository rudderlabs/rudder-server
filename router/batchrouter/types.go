package batchrouter

import (
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
)

type Connection struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type JobParameters struct {
	SourceID                string `json:"source_id"`
	DestinationID           string `json:"destination_id"`
	ReceivedAt              string `json:"received_at"`
	TransformAt             string `json:"transform_at"`
	SourceTaskRunID         string `json:"source_task_run_id"`
	SourceJobID             string `json:"source_job_id"`
	SourceJobRunID          string `json:"source_job_run_id"`
	SourceDefinitionID      string `json:"source_definition_id"`
	DestinationDefinitionID string `json:"destination_definition_id"`
	SourceCategory          string `json:"source_category"`
	EventName               string `json:"event_name"`
	EventType               string `json:"event_type"`
	MessageID               string `json:"message_id"`
}

type DestinationJobs struct {
	destWithSources router_utils.DestinationWithSources
	jobs            []*jobsdb.JobT
}

type ObjectStorageDefinition struct {
	Config          map[string]interface{}
	Key             string
	Provider        string
	DestinationID   string
	DestinationType string
}

type batchRequestMetric struct {
	batchRequestSuccess int
	batchRequestFailed  int
}

type UploadResult struct {
	Config           map[string]interface{}
	Key              string
	FileLocation     string
	LocalFilePaths   []string
	JournalOpID      int64
	Error            error
	FirstEventAt     string
	LastEventAt      string
	TotalEvents      int
	TotalBytes       int
	UseRudderStorage bool
}

type ErrorResponse struct {
	Error string
}

type WarningResponse struct {
	Remarks string
}

type BatchedJobs struct {
	Jobs       []*jobsdb.JobT
	Connection *Connection
	TimeWindow time.Time
}

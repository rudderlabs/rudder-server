package router

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/types"
)

// JobParameters struct holds source id and destination id of a job
type JobParameters struct {
	SourceID                string      `json:"source_id"`
	DestinationID           string      `json:"destination_id"`
	ReceivedAt              string      `json:"received_at"`
	TransformAt             string      `json:"transform_at"`
	SourceTaskRunID         string      `json:"source_task_run_id"`
	SourceJobID             string      `json:"source_job_id"`
	SourceJobRunID          string      `json:"source_job_run_id"`
	SourceDefinitionID      string      `json:"source_definition_id"`
	DestinationDefinitionID string      `json:"destination_definition_id"`
	SourceCategory          string      `json:"source_category"`
	RecordID                interface{} `json:"record_id"`
	MessageID               string      `json:"message_id"`
	WorkspaceID             string      `json:"workspaceId"`
	RudderAccountID         string      `json:"rudderAccountId"`
}

type workerJobStatus struct {
	userID string
	worker *worker
	job    *jobsdb.JobT
	status *jobsdb.JobStatusT
}

type HandleDestOAuthRespParams struct {
	ctx            context.Context
	destinationJob types.DestinationJobT
	workerID       int
	trRespStCd     int
	trRespBody     string
	secret         json.RawMessage
}

type Diagnostic struct {
	diagnosisTicker    *time.Ticker
	requestsMetricLock sync.RWMutex
	requestsMetric     []requestMetric
	failureMetricLock  sync.RWMutex
	failuresMetric     map[string]map[string]int
}

type requestMetric struct {
	RequestRetries       int
	RequestAborted       int
	RequestSuccess       int
	RequestCompletedTime time.Duration
}

type JobResponse struct {
	jobID                  int64
	destinationJob         *types.DestinationJobT
	destinationJobMetadata *types.JobMetadataT
	respStatusCode         int
	respBody               string
	errorAt                string
	status                 *jobsdb.JobStatusT
}

type reloadableConfig struct {
	jobQueryBatchSize                       int
	updateStatusBatchSize                   int
	readSleep                               time.Duration
	maxStatusUpdateWait                     time.Duration
	minRetryBackoff                         time.Duration
	maxRetryBackoff                         time.Duration
	jobsBatchTimeout                        time.Duration
	toAbortDestinationIDs                   string
	noOfJobsToBatchInAWorker                int
	jobsDBCommandTimeout                    time.Duration
	jobdDBMaxRetries                        int
	maxFailedCountForJob                    int
	payloadLimit                            int64
	routerTimeout                           time.Duration
	retryTimeWindow                         time.Duration
	pickupFlushInterval                     time.Duration
	maxDSQuerySize                          int
	jobIteratorMaxQueries                   int
	jobIteratorDiscardedPercentageTolerance int
	savePayloadOnError                      bool
	transformerProxy                        bool
	skipRtAbortAlertForTransformation       bool // represents if event delivery(via transformerProxy) should be alerted via router-aborted-count alert def
	skipRtAbortAlertForDelivery             bool // represents if transformation(router or batch) should be alerted via router-aborted-count alert def
}

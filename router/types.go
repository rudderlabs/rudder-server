package router

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
	jobQueryBatchSize                       misc.ValueLoader[int]
	updateStatusBatchSize                   misc.ValueLoader[int]
	readSleep                               misc.ValueLoader[time.Duration]
	maxStatusUpdateWait                     misc.ValueLoader[time.Duration]
	minRetryBackoff                         misc.ValueLoader[time.Duration]
	maxRetryBackoff                         misc.ValueLoader[time.Duration]
	jobsBatchTimeout                        misc.ValueLoader[time.Duration]
	failingJobsPenaltyThreshold             misc.ValueLoader[float64]
	failingJobsPenaltySleep                 misc.ValueLoader[time.Duration]
	toAbortDestinationIDs                   misc.ValueLoader[string]
	noOfJobsToBatchInAWorker                misc.ValueLoader[int]
	jobsDBCommandTimeout                    misc.ValueLoader[time.Duration]
	jobdDBMaxRetries                        misc.ValueLoader[int]
	maxFailedCountForJob                    misc.ValueLoader[int]
	maxFailedCountForSourcesJob             misc.ValueLoader[int]
	payloadLimit                            misc.ValueLoader[int64]
	routerTimeout                           misc.ValueLoader[time.Duration]
	retryTimeWindow                         misc.ValueLoader[time.Duration]
	sourcesRetryTimeWindow                  misc.ValueLoader[time.Duration]
	pickupFlushInterval                     misc.ValueLoader[time.Duration]
	maxDSQuerySize                          misc.ValueLoader[int]
	jobIteratorMaxQueries                   misc.ValueLoader[int]
	jobIteratorDiscardedPercentageTolerance misc.ValueLoader[int]
	savePayloadOnError                      misc.ValueLoader[bool]
	transformerProxy                        misc.ValueLoader[bool]
	skipRtAbortAlertForTransformation       misc.ValueLoader[bool] // represents if event delivery(via transformerProxy) should be alerted via router-aborted-count alert def
	skipRtAbortAlertForDelivery             misc.ValueLoader[bool] // represents if transformation(router or batch) should be alerted via router-aborted-count alert def
}

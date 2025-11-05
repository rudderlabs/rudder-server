package router

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

type workerJobStatus struct {
	userID     string
	worker     *worker
	job        *jobsdb.JobT
	status     *jobsdb.JobStatusT
	payload    json.RawMessage
	statTags   map[string]string
	parameters routerutils.JobParameters
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
	jobQueryBatchSize                      config.ValueLoader[int]
	maxJobQueryBatchSize                   config.ValueLoader[int] // absolute max limit on job query batch size when adapting based on throttling limits
	updateStatusBatchSize                  config.ValueLoader[int]
	readSleep                              config.ValueLoader[time.Duration]
	maxStatusUpdateWait                    config.ValueLoader[time.Duration]
	minRetryBackoff                        config.ValueLoader[time.Duration]
	maxRetryBackoff                        config.ValueLoader[time.Duration]
	jobsBatchTimeout                       config.ValueLoader[time.Duration]
	failingJobsPenaltyThreshold            config.ValueLoader[float64]
	failingJobsPenaltySleep                config.ValueLoader[time.Duration]
	noOfJobsToBatchInAWorker               config.ValueLoader[int]
	jobsDBCommandTimeout                   config.ValueLoader[time.Duration]
	jobdDBMaxRetries                       config.ValueLoader[int]
	maxFailedCountForJob                   config.ValueLoader[int]
	maxFailedCountForSourcesJob            config.ValueLoader[int]
	payloadLimit                           config.ValueLoader[int64]
	retryTimeWindow                        config.ValueLoader[time.Duration]
	sourcesRetryTimeWindow                 config.ValueLoader[time.Duration]
	pickupFlushInterval                    config.ValueLoader[time.Duration]
	maxDSQuerySize                         config.ValueLoader[int]
	transformerProxy                       config.ValueLoader[bool]
	skipRtAbortAlertForTransformation      config.ValueLoader[bool] // represents if event delivery(via transformerProxy) should be alerted via router-aborted-count alert def
	skipRtAbortAlertForDelivery            config.ValueLoader[bool] // represents if transformation(router or batch) should be alerted via router-aborted-count alert def
	oauthV2ExpirationTimeDiff              config.ValueLoader[time.Duration]
	enableExperimentalBufferSizeCalculator config.ValueLoader[bool]    // whether to use the experimental worker buffer size calculator or not
	experimentalBufferSizeScalingFactor    config.ValueLoader[float64] // scaling factor to scale up the buffer size in the experimental calculator
}

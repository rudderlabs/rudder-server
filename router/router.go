package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/internal/jobiterator"
	rtThrottler "github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

type reporter interface {
	WaitForSetup(ctx context.Context, clientName string) error
	Report(metrics []*utilTypes.PUReportedMetric, txn *sql.Tx)
}

type tenantStats interface {
	CalculateSuccessFailureCounts(workspace, destType string, isSuccess, isDrained bool)
	GetRouterPickupJobs(
		destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int,
	) map[string]int
	ReportProcLoopAddStats(stats map[string]map[string]int, tableType string)
	UpdateWorkspaceLatencyMap(destType, workspaceID string, val float64)
}

type HandleDestOAuthRespParamsT struct {
	ctx            context.Context
	destinationJob types.DestinationJobT
	workerID       int
	trRespStCd     int
	trRespBody     string
	secret         json.RawMessage
}

type DiagnosticT struct {
	requestsMetricLock sync.RWMutex
	failureMetricLock  sync.RWMutex
	diagnosisTicker    *time.Ticker
	requestsMetric     []requestMetric
	failuresMetric     map[string]map[string]int
}

// HandleT is the handle to this module.
type HandleT struct {
	responseQ                               chan jobResponseT
	jobsDB                                  jobsdb.MultiTenantJobsDB
	errorDB                                 jobsdb.JobsDB
	netHandle                               NetHandleI
	MultitenantI                            tenantStats
	destName                                string
	destinationId                           string
	workers                                 []*workerT
	telemetry                               *DiagnosticT
	customDestinationManager                customDestinationManager.DestinationManager
	throttlingCosts                         atomic.Pointer[types.EventTypeThrottlingCost]
	throttlerFactory                        *rtThrottler.Factory
	guaranteeUserEventOrder                 bool
	netClientTimeout                        time.Duration
	backendProxyTimeout                     time.Duration
	jobsDBCommandTimeout                    time.Duration
	jobdDBMaxRetries                        int
	enableBatching                          bool
	transformer                             transformer.Transformer
	configSubscriberLock                    sync.RWMutex
	destinationsMap                         map[string]*routerutils.BatchDestinationT // destinationID -> destination
	logger                                  logger.Logger
	batchInputCountStat                     stats.Measurement
	batchOutputCountStat                    stats.Measurement
	routerTransformInputCountStat           stats.Measurement
	routerTransformOutputCountStat          stats.Measurement
	batchInputOutputDiffCountStat           stats.Measurement
	routerResponseTransformStat             stats.Measurement
	throttlingErrorStat                     stats.Measurement
	throttledStat                           stats.Measurement
	noOfWorkers                             int
	allowAbortedUserJobsCountForProcessing  int
	isBackendConfigInitialized              bool
	backendConfig                           backendconfig.BackendConfig
	backendConfigInitialized                chan bool
	maxFailedCountForJob                    int
	noOfJobsToBatchInAWorker                int
	retryTimeWindow                         time.Duration
	routerTimeout                           time.Duration
	destinationResponseHandler              ResponseHandlerI
	saveDestinationResponse                 bool
	Reporting                               reporter
	savePayloadOnError                      bool
	oauth                                   oauth.Authorizer
	transformerProxy                        bool
	skipRtAbortAlertForDelivery             bool // represents if transformation(router or batch) should be alerted via router-aborted-count alert def
	skipRtAbortAlertForTransformation       bool // represents if event delivery(via transformerProxy) should be alerted via router-aborted-count alert def
	workspaceSet                            map[string]struct{}
	sourceIDWorkspaceMap                    map[string]string
	maxDSQuerySize                          int
	jobIteratorMaxQueries                   int
	jobIteratorDiscardedPercentageTolerance int

	backgroundGroup  *errgroup.Group
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
	backgroundWait   func() error
	startEnded       chan struct{}
	lastQueryRunTime time.Time

	payloadLimit     int64
	transientSources transientsource.Service
	rsourcesService  rsources.JobService
	debugger         destinationdebugger.DestinationDebugger
	adaptiveLimit    func(int64) int64
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
	JobT   *jobsdb.JobT
}

// JobParametersT struct holds source id and destination id of a job
type JobParametersT struct {
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

type workerMessageT struct {
	job                *jobsdb.JobT
	workerAssignedTime time.Time
}

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	channel                   chan workerMessageT // the worker job channel
	workerID                  int                 // identifies the worker
	failedJobs                int                 // counts the failed jobs of a worker till it gets reset by external channel
	barrier                   *eventorder.Barrier
	retryForJobMap            map[int64]time.Time     // jobID to next retry time map
	retryForJobMapMutex       sync.RWMutex            // lock to protect structure above
	routerJobs                []types.RouterJobT      // slice to hold router jobs to send to destination transformer
	destinationJobs           []types.DestinationJobT // slice to hold destination jobs
	rt                        *HandleT                // handle to router
	deliveryTimeStat          stats.Measurement
	routerDeliveryLatencyStat stats.Measurement
	routerProxyStat           stats.Measurement
	batchTimeStat             stats.Measurement
	latestAssignedTime        time.Time
	processingStartTime       time.Time
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfJobsPerChannel int
	readSleep, maxStatusUpdateWait, diagnosisTickerTime          time.Duration
	minRetryBackoff, maxRetryBackoff, jobsBatchTimeout           time.Duration
	pkgLogger                                                    logger.Logger
	Diagnostics                                                  diagnostics.DiagnosticsI
	fixedLoopSleep                                               time.Duration
	toAbortDestinationIDs                                        string
	disableEgress                                                bool
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type requestMetric struct {
	RequestRetries       int
	RequestAborted       int
	RequestSuccess       int
	RequestCompletedTime time.Duration
}

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}

func isJobTerminated(status int) bool {
	if status == 429 {
		return false
	}
	return status >= 200 && status < 500
}

func loadConfig() {
	config.RegisterIntConfigVariable(10000, &jobQueryBatchSize, true, 1, "Router.jobQueryBatchSize")
	config.RegisterIntConfigVariable(1000, &updateStatusBatchSize, true, 1, "Router.updateStatusBatchSize")
	config.RegisterDurationConfigVariable(1000, &readSleep, true, time.Millisecond, []string{"Router.readSleep", "Router.readSleepInMS"}...)
	config.RegisterIntConfigVariable(1000, &noOfJobsPerChannel, false, 1, "Router.noOfJobsPerChannel")
	config.RegisterDurationConfigVariable(5, &jobsBatchTimeout, true, time.Second, []string{"Router.jobsBatchTimeout", "Router.jobsBatchTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(5, &maxStatusUpdateWait, true, time.Second, []string{"Router.maxStatusUpdateWait", "Router.maxStatusUpdateWaitInS"}...)
	config.RegisterBoolConfigVariable(false, &disableEgress, false, "disableEgress")
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(60, &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.routerTimePeriod", "Diagnostics.routerTimePeriodInS"}...)
	config.RegisterDurationConfigVariable(10, &minRetryBackoff, true, time.Second, []string{"Router.minRetryBackoff", "Router.minRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(300, &maxRetryBackoff, true, time.Second, []string{"Router.maxRetryBackoff", "Router.maxRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(0, &fixedLoopSleep, true, time.Millisecond, []string{"Router.fixedLoopSleep", "Router.fixedLoopSleepInMS"}...)
	config.RegisterStringConfigVariable("", &toAbortDestinationIDs, true, "Router.toAbortDestinationIDs")
}

func sendRetryStoreStats(attempt int) {
	pkgLogger.Warnf("Timeout during store jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func sendRetryUpdateStats(attempt int) {
	pkgLogger.Warnf("Timeout during update job status in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func sendQueryRetryStats(attempt int) {
	pkgLogger.Warnf("Timeout during query jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func (worker *workerT) trackStuckDelivery() chan struct{} {
	var d time.Duration
	if worker.rt.transformerProxy {
		d = (worker.rt.backendProxyTimeout + worker.rt.netClientTimeout) * 2
	} else {
		d = worker.rt.netClientTimeout * 2
	}

	ch := make(chan struct{}, 1)
	rruntime.Go(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(d):
			worker.rt.logger.Infof("[%s Router] Delivery to destination exceeded the 2 * configured timeout ", worker.rt.destName)
			stat := stats.Default.NewTaggedStat("router_delivery_exceeded_timeout", stats.CountType, stats.Tags{
				"destType": worker.rt.destName,
			})
			stat.Increment()
		}
	})
	return ch
}

func (worker *workerT) recordStatsForFailedTransforms(transformType string, transformedJobs []types.DestinationJobT) {
	for _, destJob := range transformedJobs {
		// Input Stats for batch/router transformation
		stats.Default.NewTaggedStat("router_transform_num_jobs", stats.CountType, stats.Tags{
			"destType":      worker.rt.destName,
			"transformType": transformType,
			"statusCode":    strconv.Itoa(destJob.StatusCode),
			"workspaceId":   destJob.Destination.WorkspaceID,
			"destinationId": destJob.Destination.ID,
		}).Count(1)
		if destJob.StatusCode != http.StatusOK {
			transformFailedCountStat := stats.Default.NewTaggedStat("router_transform_num_failed_jobs", stats.CountType, stats.Tags{
				"destType":      worker.rt.destName,
				"transformType": transformType,
				"statusCode":    strconv.Itoa(destJob.StatusCode),
				"destination":   destJob.Destination.ID,
			})
			transformFailedCountStat.Count(1)
		}
	}
}

func (worker *workerT) transform(routerJobs []types.RouterJobT) []types.DestinationJobT {
	worker.rt.routerTransformInputCountStat.Count(len(routerJobs))
	destinationJobs := worker.rt.transformer.Transform(
		transformer.ROUTER_TRANSFORM,
		&types.TransformMessageT{Data: routerJobs, DestType: strings.ToLower(worker.rt.destName)},
	)
	worker.rt.routerTransformOutputCountStat.Count(len(destinationJobs))
	worker.recordStatsForFailedTransforms("routerTransform", destinationJobs)
	return destinationJobs
}

func (worker *workerT) batchTransform(routerJobs []types.RouterJobT) []types.DestinationJobT {
	inputJobsLength := len(routerJobs)
	worker.rt.batchInputCountStat.Count(inputJobsLength)
	destinationJobs := worker.rt.transformer.Transform(
		transformer.BATCH,
		&types.TransformMessageT{
			Data:     routerJobs,
			DestType: strings.ToLower(worker.rt.destName),
		},
	)
	worker.rt.batchOutputCountStat.Count(len(destinationJobs))
	worker.recordStatsForFailedTransforms("batch", destinationJobs)
	return destinationJobs
}

func (worker *workerT) workerProcess() {
	timeout := time.After(jobsBatchTimeout)
	for {
		select {
		case message, hasMore := <-worker.channel:
			if !hasMore {
				if len(worker.routerJobs) == 0 {
					worker.rt.logger.Debugf("[%s Router] :: Worker channel closed, processed %d jobs", worker.rt.destName, len(worker.routerJobs))
					return
				}

				if worker.rt.enableBatching {
					worker.destinationJobs = worker.batchTransform(worker.routerJobs)
				} else {
					worker.destinationJobs = worker.transform(worker.routerJobs)
				}
				worker.processDestinationJobs()
				worker.rt.logger.Debugf("[%s Router] :: Worker channel closed, processed %d jobs", worker.rt.destName, len(worker.routerJobs))
				return
			}

			worker.rt.logger.Debugf("[%v Router] :: performing checks to send payload.", worker.rt.destName)

			job := message.job
			userID := job.UserID

			var parameters JobParametersT
			if err := json.Unmarshal(job.Parameters, &parameters); err != nil {
				panic(fmt.Errorf("unmarshalling of job parameters failed for job %d (%s): %w", job.JobID, string(job.Parameters), err))
			}
			worker.rt.configSubscriberLock.RLock()
			drain, drainReason := routerutils.ToBeDrained(job, parameters.DestinationID, toAbortDestinationIDs, worker.rt.destinationsMap)
			worker.rt.configSubscriberLock.RUnlock()
			if drain {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.Aborted.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     strconv.Itoa(routerutils.DRAIN_ERROR_CODE),
					Parameters:    routerutils.EmptyPayload,
					ErrorResponse: routerutils.EnhanceJSON(routerutils.EmptyPayload, "reason", drainReason),
					WorkspaceId:   job.WorkspaceId,
				}
				// Enhancing job parameter with the drain reason.
				job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "router")
				job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", drainReason)
				worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID, JobT: job}
				stats.Default.NewTaggedStat(`drained_events`, stats.CountType, stats.Tags{
					"destType":    worker.rt.destName,
					"destId":      parameters.DestinationID,
					"module":      "router",
					"reasons":     drainReason,
					"workspaceId": job.WorkspaceId,
				}).Count(1)
				continue
			}

			if worker.rt.guaranteeUserEventOrder {
				orderKey := fmt.Sprintf(`%s:%s`, userID, parameters.DestinationID)
				if wait, previousFailedJobID := worker.barrier.Wait(orderKey, job.JobID); wait {
					previousFailedJobIDStr := "<nil>"
					if previousFailedJobID != nil {
						previousFailedJobIDStr = strconv.FormatInt(*previousFailedJobID, 10)
					}
					worker.rt.logger.Debugf("EventOrder: [%d] job %d of key %s must wait (previousFailedJobID: %s)",
						worker.workerID, job.JobID, orderKey, previousFailedJobIDStr,
					)

					// mark job as waiting if prev job from same user has not succeeded yet
					worker.rt.logger.Debugf(
						"[%v Router] :: skipping processing job for userID: %v since prev failed job exists, prev id %v, current id %v",
						worker.rt.destName, userID, previousFailedJobID, job.JobID,
					)
					resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "blocking_id", *previousFailedJobID)
					resp = misc.UpdateJSONWithNewKeyVal(resp, "user_id", userID)
					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						JobState:      jobsdb.Waiting.State,
						ErrorResponse: resp, // check
						Parameters:    routerutils.EmptyPayload,
						WorkspaceId:   job.WorkspaceId,
					}
					worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID, JobT: job}
					continue
				}
			}

			firstAttemptedAt := gjson.GetBytes(job.LastJobStatus.ErrorResponse, "firstAttemptedAt").Str
			jobMetadata := types.JobMetadataT{
				UserID:             userID,
				JobID:              job.JobID,
				SourceID:           parameters.SourceID,
				DestinationID:      parameters.DestinationID,
				AttemptNum:         job.LastJobStatus.AttemptNum,
				ReceivedAt:         parameters.ReceivedAt,
				CreatedAt:          job.CreatedAt.Format(misc.RFC3339Milli),
				FirstAttemptedAt:   firstAttemptedAt,
				TransformAt:        parameters.TransformAt,
				JobT:               job,
				WorkspaceID:        parameters.WorkspaceID,
				WorkerAssignedTime: message.workerAssignedTime,
			}

			worker.rt.configSubscriberLock.RLock()
			batchDestination, ok := worker.rt.destinationsMap[parameters.DestinationID]
			worker.rt.configSubscriberLock.RUnlock()
			if !ok {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.Failed.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorResponse: []byte(`{"reason":"failed because destination is not available in the config"}`),
					Parameters:    routerutils.EmptyPayload,
					WorkspaceId:   job.WorkspaceId,
				}
				if worker.rt.guaranteeUserEventOrder {
					orderKey := fmt.Sprintf(`%s:%s`, job.UserID, parameters.DestinationID)
					worker.rt.logger.Debugf("EventOrder: [%d] job %d for key %s failed", worker.workerID, status.JobID, orderKey)
					if err := worker.barrier.StateChanged(orderKey, job.JobID, status.JobState); err != nil {
						panic(err)
					}
				}
				worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID, JobT: job}
				continue
			}
			destination := batchDestination.Destination
			if authType := oauth.GetAuthType(destination.DestinationDefinition.Config); authType == oauth.OAuth {
				rudderAccountID := oauth.GetAccountId(destination.Config, oauth.DeliveryAccountIdKey)

				if routerutils.IsNotEmptyString(rudderAccountID) {
					worker.rt.logger.Debugf(`[%s][FetchToken] Token Fetch Method to be called`, destination.DestinationDefinition.Name)
					// Get Access Token Information to send it as part of the event
					tokenStatusCode, accountSecretInfo := worker.rt.oauth.FetchToken(&oauth.RefreshTokenParams{
						AccountId:       rudderAccountID,
						WorkspaceId:     jobMetadata.WorkspaceID,
						DestDefName:     destination.DestinationDefinition.Name,
						EventNamePrefix: "fetch_token",
					})
					worker.rt.logger.Debugf(`[%s][FetchToken] Token Fetch Method finished (statusCode, value): (%v, %+v)`, destination.DestinationDefinition.Name, tokenStatusCode, accountSecretInfo)
					if tokenStatusCode == http.StatusOK {
						jobMetadata.Secret = accountSecretInfo.Account.Secret
					} else {
						worker.rt.logger.Errorf(`[%s][FetchToken] Token Fetch Method error (statusCode, error): (%d, %s)`, destination.DestinationDefinition.Name, tokenStatusCode, accountSecretInfo.Err)
					}
				}
			}

			if worker.rt.enableBatching {
				worker.routerJobs = append(worker.routerJobs, types.RouterJobT{
					Message:     job.EventPayload,
					JobMetadata: jobMetadata,
					Destination: destination,
				})

				if len(worker.routerJobs) >= worker.rt.noOfJobsToBatchInAWorker {
					worker.destinationJobs = worker.batchTransform(worker.routerJobs)
					worker.processDestinationJobs()
				}
			} else if parameters.TransformAt == "router" {
				worker.routerJobs = append(worker.routerJobs, types.RouterJobT{
					Message:     job.EventPayload,
					JobMetadata: jobMetadata,
					Destination: destination,
				})

				if len(worker.routerJobs) >= worker.rt.noOfJobsToBatchInAWorker {
					worker.destinationJobs = worker.transform(worker.routerJobs)
					worker.processDestinationJobs()
				}
			} else {
				worker.destinationJobs = append(worker.destinationJobs, types.DestinationJobT{
					Message:          job.EventPayload,
					Destination:      destination,
					JobMetadataArray: []types.JobMetadataT{jobMetadata},
				})
				worker.processDestinationJobs()
			}

		case <-timeout:
			timeout = time.After(jobsBatchTimeout)

			if len(worker.routerJobs) > 0 {
				if worker.rt.enableBatching {
					worker.destinationJobs = worker.batchTransform(worker.routerJobs)
				} else {
					worker.destinationJobs = worker.transform(worker.routerJobs)
				}
				worker.processDestinationJobs()
			}
		}
	}
}

func (worker *workerT) processDestinationJobs() {
	ctx := context.TODO()
	defer worker.batchTimeStat.RecordDuration()()

	var respContentType string
	var respStatusCode, prevRespStatusCode int
	var respBody string
	var respBodyTemp string

	var destinationResponseHandler ResponseHandlerI
	worker.rt.configSubscriberLock.RLock()
	destinationResponseHandler = worker.rt.destinationResponseHandler
	worker.rt.configSubscriberLock.RUnlock()

	/*
		Batch
		[u1e1, u2e1, u1e2, u2e2, u1e3, u2e3]
		[b1, b2, b3]
		b1 will send if success
		b2 will send if b2 failed then will drop b3

		Router transform
		[u1e1, u2e1, u1e2, u2e2, u1e3, u2e3]
		200, 200, 500, 200, 200, 200

		Case 1:
		u1e1 will send - success
		u2e1 will send - success
		u1e2 will drop because transformer gave 500
		u2e2 will send - success
		u1e3 should be dropped because u1e2 should be retried
		u2e3 will send

		Case 2:
		u1e1 will send - success
		u2e1 will send - failed 5xx
		u1e2 will send
		u2e2 will drop - because request to destination failed with 5xx
		u1e3 will send
		u2e3 will drop - because request to destination failed with 5xx

		Case 3:
		u1e1 will send - success
		u2e1 will send - failed 4xx
		u1e2 will send
		u2e2 will send - because previous job is aborted
		u1e3 will send
		u2e3 will send
	*/

	failedUserIDsMap := make(map[string]struct{})
	routerJobResponses := make([]*JobResponse, 0)

	sort.Slice(worker.destinationJobs, func(i, j int) bool {
		return worker.destinationJobs[i].JobMetadataArray[0].JobID < worker.destinationJobs[j].JobMetadataArray[0].JobID
	})

	for _, destinationJob := range worker.destinationJobs {
		var errorAt string
		respBodyArr := make([]string, 0)
		if destinationJob.StatusCode == 200 || destinationJob.StatusCode == 0 {
			if worker.canSendJobToDestination(prevRespStatusCode, failedUserIDsMap, &destinationJob) {
				diagnosisStartTime := time.Now()
				destinationID := destinationJob.JobMetadataArray[0].DestinationID
				transformAt := destinationJob.JobMetadataArray[0].TransformAt

				// START: request to destination endpoint
				workspaceID := destinationJob.JobMetadataArray[0].JobT.WorkspaceId
				deliveryLatencyStat := stats.Default.NewTaggedStat("delivery_latency", stats.TimerType, stats.Tags{
					"module":      "router",
					"destType":    worker.rt.destName,
					"destination": misc.GetTagName(destinationJob.Destination.ID, destinationJob.Destination.Name),
					"workspaceId": workspaceID,
				})
				startedAt := time.Now()

				if worker.latestAssignedTime != destinationJob.JobMetadataArray[0].WorkerAssignedTime {
					worker.latestAssignedTime = destinationJob.JobMetadataArray[0].WorkerAssignedTime
					worker.processingStartTime = time.Now()
				}
				// TODO: remove trackStuckDelivery once we verify it is not needed,
				//			router_delivery_exceeded_timeout -> goes to zero
				ch := worker.trackStuckDelivery()

				// Assuming twice the overhead - defensive: 30% was just fine though
				// In fact, the timeout should be more than the maximum latency allowed by these workers.
				// Assuming 10s maximum latency
				elapsed := time.Since(worker.processingStartTime)
				threshold := worker.rt.routerTimeout
				if elapsed > threshold {
					respStatusCode = types.RouterTimedOutStatusCode
					respBody = fmt.Sprintf("Failed with status code %d as the jobs took more time than expected. Will be retried", types.RouterTimedOutStatusCode)
					worker.rt.logger.Debugf(
						"Will drop with %d because of time expiry %v",
						types.RouterTimedOutStatusCode, destinationJob.JobMetadataArray[0].JobID,
					)
				} else if worker.rt.customDestinationManager != nil {
					for _, destinationJobMetadata := range destinationJob.JobMetadataArray {
						if destinationID != destinationJobMetadata.DestinationID {
							panic(fmt.Errorf("different destinations are grouped together"))
						}
					}
					respStatusCode, respBody = worker.rt.customDestinationManager.SendData(destinationJob.Message, destinationID)
					errorAt = routerutils.ERROR_AT_CUST
				} else {
					result, err := getIterableStruct(destinationJob.Message, transformAt)
					if err != nil {
						errorAt = routerutils.ERROR_AT_TF
						respStatusCode, respBody = types.RouterUnMarshalErrorCode, fmt.Errorf("transformer response unmarshal error: %w", err).Error()
					} else {
						for _, val := range result {
							err := integrations.ValidatePostInfo(val)
							if err != nil {
								errorAt = routerutils.ERROR_AT_TF
								respStatusCode, respBodyTemp = http.StatusBadRequest, fmt.Sprintf(`400 GetPostInfoFailed with error: %s`, err.Error())
								respBodyArr = append(respBodyArr, respBodyTemp)
							} else {
								// stat start
								pkgLogger.Debugf(`responseTransform status :%v, %s`, worker.rt.transformerProxy, worker.rt.destName)
								// transformer proxy start
								errorAt = routerutils.ERROR_AT_DEL
								if worker.rt.transformerProxy {
									jobID := destinationJob.JobMetadataArray[0].JobID
									pkgLogger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request started`, worker.rt.destName, jobID)

									// setting metadata
									firstJobMetadata := destinationJob.JobMetadataArray[0]
									proxyReqparams := &transformer.ProxyRequestParams{
										DestName: worker.rt.destName,
										JobID:    jobID,
										ResponseData: transformer.ProxyRequestPayload{
											PostParametersT: val,
											Metadata: transformer.ProxyRequestMetadata{
												SourceID:      firstJobMetadata.SourceID,
												DestinationID: firstJobMetadata.DestinationID,
												WorkspaceID:   firstJobMetadata.WorkspaceID,
												JobID:         firstJobMetadata.JobID,
												AttemptNum:    firstJobMetadata.AttemptNum,
												DestInfo:      firstJobMetadata.DestInfo,
												Secret:        firstJobMetadata.Secret,
											},
										},
									}
									rtlTime := time.Now()
									respStatusCode, respBodyTemp, respContentType = worker.rt.transformer.ProxyRequest(ctx, proxyReqparams)
									worker.routerProxyStat.SendTiming(time.Since(rtlTime))
									pkgLogger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request ended`, worker.rt.destName, jobID)
									authType := oauth.GetAuthType(destinationJob.Destination.DestinationDefinition.Config)
									if routerutils.IsNotEmptyString(string(authType)) && authType == oauth.OAuth {
										pkgLogger.Debugf(`Sending for OAuth destination`)
										// Token from header of the request
										respStatusCode, respBodyTemp = worker.rt.HandleOAuthDestResponse(&HandleDestOAuthRespParamsT{
											ctx:            ctx,
											destinationJob: destinationJob,
											workerID:       worker.workerID,
											trRespStCd:     respStatusCode,
											trRespBody:     respBodyTemp,
											secret:         firstJobMetadata.Secret,
										})
									}
								} else {
									sendCtx, cancel := context.WithTimeout(ctx, worker.rt.netClientTimeout)
									rdlTime := time.Now()
									resp := worker.rt.netHandle.SendPost(sendCtx, val)
									cancel()
									respStatusCode, respBodyTemp, respContentType = resp.StatusCode, string(resp.ResponseBody), resp.ResponseContentType
									// stat end
									worker.routerDeliveryLatencyStat.SendTiming(time.Since(rdlTime))
								}
								// transformer proxy end
								if isSuccessStatus(respStatusCode) {
									respBodyArr = append(respBodyArr, respBodyTemp)
								} else {
									respBodyArr = []string{respBodyTemp}
									break
								}
							}
						}
						respBody = strings.Join(respBodyArr, " ")
						if worker.rt.transformerProxy {
							stats.Default.NewTaggedStat("transformer_proxy.input_events_count", stats.CountType, stats.Tags{
								"destType":      worker.rt.destName,
								"destinationId": destinationJob.Destination.ID,
								"workspace":     workspaceID,
								"workspaceId":   workspaceID,
							}).Count(len(result))

							pkgLogger.Debugf(`[TransformerProxy] (Dest-%v) {Job - %v} Input Router Events: %v, Out router events: %v`, worker.rt.destName,
								destinationJob.JobMetadataArray[0].JobID,
								len(result),
								len(respBodyArr),
							)

							stats.Default.NewTaggedStat("transformer_proxy.output_events_count", stats.CountType, stats.Tags{
								"destType":      worker.rt.destName,
								"destinationId": destinationJob.Destination.ID,
								"workspace":     workspaceID,
								"workspaceId":   workspaceID,
							}).Count(len(respBodyArr))
						}
					}
				}
				ch <- struct{}{}
				timeTaken := time.Since(startedAt)
				if respStatusCode != types.RouterTimedOutStatusCode && respStatusCode != types.RouterUnMarshalErrorCode {
					worker.rt.MultitenantI.UpdateWorkspaceLatencyMap(worker.rt.destName, workspaceID, float64(timeTaken)/float64(time.Second))
				}

				// Using response status code and body to get response code rudder router logic is based on.
				// Works when transformer proxy in disabled
				if !worker.rt.transformerProxy && destinationResponseHandler != nil {
					respStatusCode = destinationResponseHandler.IsSuccessStatus(respStatusCode, respBody)
				}

				worker.deliveryTimeStat.SendTiming(timeTaken)
				deliveryLatencyStat.Since(startedAt)

				// END: request to destination endpoint

				// Failure - Save response body
				// Success - Skip saving response body
				// By default we get some config from dest def
				// We can override via env saveDestinationResponseOverride

				if isSuccessStatus(respStatusCode) && !getRouterConfigBool("saveDestinationResponseOverride", worker.rt.destName, false) && !worker.rt.saveDestinationResponse {
					respBody = ""
				}

				worker.updateReqMetrics(respStatusCode, &diagnosisStartTime)
			} else {
				respStatusCode = http.StatusInternalServerError
				if !worker.rt.enableBatching {
					respBody = "skipping sending to destination because previous job (of user) in batch is failed."
				}
				errorAt = routerutils.ERROR_AT_TF
			}
		} else {
			respStatusCode = destinationJob.StatusCode
			respBody = destinationJob.Error
			errorAt = routerutils.ERROR_AT_TF
		}

		prevRespStatusCode = respStatusCode

		if !isJobTerminated(respStatusCode) {
			for _, metadata := range destinationJob.JobMetadataArray {
				failedUserIDsMap[metadata.UserID] = struct{}{}
			}
		}

		// assigning the destinationJob to a local variable (_destinationJob), so that
		// elements in routerJobResponses have pointer to the right job.
		_destinationJob := destinationJob

		// TODO: remove this once we enforce the necessary validations in the transformer's response
		dedupedJobMetadata := lo.UniqBy(_destinationJob.JobMetadataArray, func(jobMetadata types.JobMetadataT) int64 {
			return jobMetadata.JobID
		})
		for _, destinationJobMetadata := range dedupedJobMetadata {
			_destinationJobMetadata := destinationJobMetadata
			// assigning the destinationJobMetadata to a local variable (_destinationJobMetadata), so that
			// elements in routerJobResponses have pointer to the right destinationJobMetadata.

			routerJobResponses = append(routerJobResponses, &JobResponse{
				jobID:                  destinationJobMetadata.JobID,
				destinationJob:         &_destinationJob,
				destinationJobMetadata: &_destinationJobMetadata,
				respStatusCode:         respStatusCode,
				respBody:               respBody,
				errorAt:                errorAt,
			})
		}
	}

	sort.Slice(routerJobResponses, func(i, j int) bool {
		return routerJobResponses[i].jobID < routerJobResponses[j].jobID
	})

	// Struct to hold unique users in the batch (worker.destinationJobs)
	userToJobIDMap := make(map[string]int64)

	for _, routerJobResponse := range routerJobResponses {
		destinationJobMetadata := routerJobResponse.destinationJobMetadata
		destinationJob := routerJobResponse.destinationJob
		attemptNum := destinationJobMetadata.AttemptNum
		respStatusCode = routerJobResponse.respStatusCode
		status := jobsdb.JobStatusT{
			JobID:       destinationJobMetadata.JobID,
			AttemptNum:  attemptNum,
			ExecTime:    time.Now(),
			RetryTime:   time.Now(),
			Parameters:  routerutils.EmptyPayload,
			WorkspaceId: destinationJobMetadata.WorkspaceID,
		}

		routerJobResponse.status = &status

		if !isJobTerminated(respStatusCode) {
			if prevFailedJobID, ok := userToJobIDMap[destinationJobMetadata.UserID]; ok {
				// This means more than two jobs of the same user are in the batch & the batch job is failed
				// Only one job is marked failed and the rest are marked waiting
				// Job order logic requires that at any point of time, we should have only one failed job per user
				// This is introduced to ensure the above statement
				resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "blocking_id", prevFailedJobID)
				resp = misc.UpdateJSONWithNewKeyVal(resp, "user_id", destinationJobMetadata.UserID)
				resp = misc.UpdateJSONWithNewKeyVal(resp, "moreinfo", "attempted to send in a batch")

				status.JobState = jobsdb.Waiting.State
				status.ErrorResponse = resp
				worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
				continue
			}
			userToJobIDMap[destinationJobMetadata.UserID] = destinationJobMetadata.JobID
		}

		status.AttemptNum++
		status.ErrorResponse = routerutils.EnhanceJSON(routerutils.EmptyPayload, "response", routerJobResponse.respBody)
		status.ErrorCode = strconv.Itoa(respStatusCode)

		worker.postStatusOnResponseQ(respStatusCode, destinationJob.Message, respContentType, destinationJobMetadata, &status, routerJobResponse.errorAt)

		worker.sendEventDeliveryStat(destinationJobMetadata, &status, &destinationJob.Destination)

		worker.sendRouterResponseCountStat(&status, &destinationJob.Destination, routerJobResponse.errorAt)
	}

	// NOTE: Sending live events to config backend after the status objects are built completely.
	destLiveEventSentMap := make(map[*types.DestinationJobT]struct{})
	for _, routerJobResponse := range routerJobResponses {
		// Sending only one destination live event for every destinationJob
		if _, ok := destLiveEventSentMap[routerJobResponse.destinationJob]; !ok {
			payload := routerJobResponse.destinationJob.Message
			if routerJobResponse.destinationJob.Message == nil {
				payload = routerJobResponse.destinationJobMetadata.JobT.EventPayload
			}
			sourcesIDs := make([]string, 0)
			for _, metadata := range routerJobResponse.destinationJob.JobMetadataArray {
				if !misc.Contains(sourcesIDs, metadata.SourceID) {
					sourcesIDs = append(sourcesIDs, metadata.SourceID)
				}
			}
			worker.sendDestinationResponseToConfigBackend(payload, routerJobResponse.destinationJobMetadata, routerJobResponse.status, sourcesIDs)
			destLiveEventSentMap[routerJobResponse.destinationJob] = struct{}{}
		}
	}

	// routerJobs/destinationJobs are processed. Clearing the queues.
	worker.routerJobs = make([]types.RouterJobT, 0)
	worker.destinationJobs = make([]types.DestinationJobT, 0)
}

func (worker *workerT) canSendJobToDestination(prevRespStatusCode int, failedUserIDsMap map[string]struct{}, destinationJob *types.DestinationJobT) bool {
	if prevRespStatusCode == 0 {
		return true
	}

	if !worker.rt.guaranteeUserEventOrder {
		// if guaranteeUserEventOrder is false, letting the next jobs pass
		return true
	}

	// If batching is enabled, we send the request only if the previous one succeeds
	if worker.rt.enableBatching {
		return isSuccessStatus(prevRespStatusCode)
	}

	// If the destinationJob has come through router transform,
	// drop the request if it is of a failed user, else send
	for i := range destinationJob.JobMetadataArray {
		if _, ok := failedUserIDsMap[destinationJob.JobMetadataArray[i].UserID]; ok {
			return false
		}
	}

	return true
}

func getIterableStruct(payload []byte, transformAt string) ([]integrations.PostParametersT, error) {
	var err error
	var response integrations.PostParametersT
	responseArray := make([]integrations.PostParametersT, 0)
	if transformAt == "router" {
		err = json.Unmarshal(payload, &response)
		if err != nil {
			err = json.Unmarshal(payload, &responseArray)
		} else {
			responseArray = append(responseArray, response)
		}
	} else {
		err = json.Unmarshal(payload, &response)
		if err == nil {
			responseArray = append(responseArray, response)
		}
	}

	return responseArray, err
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

func (worker *workerT) updateReqMetrics(respStatusCode int, diagnosisStartTime *time.Time) {
	var reqMetric requestMetric

	if isSuccessStatus(respStatusCode) {
		reqMetric.RequestSuccess++
	} else {
		reqMetric.RequestRetries++
	}
	reqMetric.RequestCompletedTime = time.Since(*diagnosisStartTime)
	worker.rt.trackRequestMetrics(reqMetric)
}

func (worker *workerT) allowRouterAbortedAlert(errorAt string) bool {
	switch errorAt {
	case routerutils.ERROR_AT_CUST:
		return true
	case routerutils.ERROR_AT_TF:
		return !worker.rt.skipRtAbortAlertForTransformation
	case routerutils.ERROR_AT_DEL:
		return !worker.rt.transformerProxy && !worker.rt.skipRtAbortAlertForDelivery
	default:
		return true
	}
}

func (worker *workerT) updateAbortedMetrics(destinationID, workspaceId, statusCode, errorAt string) {
	alert := worker.allowRouterAbortedAlert(errorAt)
	eventsAbortedStat := stats.Default.NewTaggedStat(`router_aborted_events`, stats.CountType, stats.Tags{
		"destType":       worker.rt.destName,
		"respStatusCode": statusCode,
		"destId":         destinationID,
		"workspaceId":    workspaceId,

		// To indicate if the failure should be alerted for router-aborted-count
		"alert": strconv.FormatBool(alert),
		// To specify at which point failure happened
		"errorAt": errorAt,
	})
	eventsAbortedStat.Increment()
}

func (worker *workerT) postStatusOnResponseQ(respStatusCode int, payload json.RawMessage,
	respContentType string, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT,
	errorAt string,
) {
	// Enhancing status.ErrorResponse with firstAttemptedAt
	firstAttemptedAtTime := time.Now()
	if destinationJobMetadata.FirstAttemptedAt != "" {
		t, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.FirstAttemptedAt)
		if err == nil {
			firstAttemptedAtTime = t
		}
	}

	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "firstAttemptedAt", firstAttemptedAtTime.Format(misc.RFC3339Milli))
	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "content-type", respContentType)

	if isSuccessStatus(respStatusCode) {
		status.JobState = jobsdb.Succeeded.State
		worker.rt.logger.Debugf("[%v Router] :: sending success status to response", worker.rt.destName)
		worker.rt.responseQ <- jobResponseT{status: status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}

		// Deleting jobID from retryForJobMap. jobID goes into retryForJobMap if it is failed with 5xx or 429.
		// It's safe to delete from the map, even if jobID is not present.
		worker.retryForJobMapMutex.Lock()
		delete(worker.retryForJobMap, destinationJobMetadata.JobID)
		worker.retryForJobMapMutex.Unlock()
	} else {
		// Saving payload to DB only
		// 1. if job failed and
		// 2. if router job undergoes batching or dest transform.
		if payload != nil && (worker.rt.enableBatching || destinationJobMetadata.TransformAt == "router") {
			if worker.rt.savePayloadOnError {
				status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "payload", string(payload))
			}
		}
		// the job failed
		worker.rt.logger.Debugf("[%v Router] :: Job failed to send, analyzing...", worker.rt.destName)
		worker.failedJobs++

		status.JobState = jobsdb.Failed.State

		if respStatusCode >= 500 {
			timeElapsed := time.Since(firstAttemptedAtTime)
			if respStatusCode != types.RouterTimedOutStatusCode && respStatusCode != types.RouterUnMarshalErrorCode {
				if timeElapsed > worker.rt.retryTimeWindow && status.AttemptNum >= worker.rt.maxFailedCountForJob {
					status.JobState = jobsdb.Aborted.State
					worker.retryForJobMapMutex.Lock()
					delete(worker.retryForJobMap, destinationJobMetadata.JobID)
					worker.retryForJobMapMutex.Unlock()
				} else {
					worker.retryForJobMapMutex.Lock()
					worker.retryForJobMap[destinationJobMetadata.JobID] = time.Now().Add(durationBeforeNextAttempt(status.AttemptNum))
					worker.retryForJobMapMutex.Unlock()
				}
			}
		} else if respStatusCode == 429 {
			worker.retryForJobMapMutex.Lock()
			worker.retryForJobMap[destinationJobMetadata.JobID] = time.Now().Add(durationBeforeNextAttempt(status.AttemptNum))
			worker.retryForJobMapMutex.Unlock()
		} else {
			status.JobState = jobsdb.Aborted.State
		}

		if status.JobState == jobsdb.Aborted.State {
			worker.updateAbortedMetrics(destinationJobMetadata.DestinationID, status.WorkspaceId, status.ErrorCode, errorAt)
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "stage", "router")
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "reason", status.ErrorResponse) // NOTE: Old key used was "error_response"
		}

		if worker.rt.guaranteeUserEventOrder {
			if status.JobState == jobsdb.Failed.State {
				orderKey := fmt.Sprintf(`%s:%s`, destinationJobMetadata.UserID, destinationJobMetadata.DestinationID)
				worker.rt.logger.Debugf("EventOrder: [%d] job %d for key %s failed", worker.workerID, status.JobID, orderKey)
				if err := worker.barrier.StateChanged(orderKey, destinationJobMetadata.JobID, status.JobState); err != nil {
					panic(err)
				}
			}
		}
		worker.rt.logger.Debugf("[%v Router] :: sending failed/aborted state as response", worker.rt.destName)
		worker.rt.responseQ <- jobResponseT{status: status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
	}
}

func (worker *workerT) sendRouterResponseCountStat(status *jobsdb.JobStatusT, destination *backendconfig.DestinationT, errorAt string) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	var alert bool
	alert = worker.allowRouterAbortedAlert(errorAt)
	if status.JobState == jobsdb.Succeeded.State {
		alert = !worker.rt.skipRtAbortAlertForTransformation || !worker.rt.skipRtAbortAlertForDelivery
		errorAt = ""
	}
	routerResponseStat := stats.Default.NewTaggedStat("router_response_counts", stats.CountType, stats.Tags{
		"destType":       worker.rt.destName,
		"respStatusCode": status.ErrorCode,
		"destination":    destinationTag,
		"destId":         destination.ID,
		"attempt_number": strconv.Itoa(status.AttemptNum),
		"workspaceId":    status.WorkspaceId,
		// To indicate if the failure should be alerted for router-aborted-count
		"alert": strconv.FormatBool(alert),
		// To specify at which point failure happened
		"errorAt": errorAt,
	})
	routerResponseStat.Count(1)
}

func (worker *workerT) sendEventDeliveryStat(destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, destination *backendconfig.DestinationT) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	if status.JobState == jobsdb.Succeeded.State {
		eventsDeliveredStat := stats.Default.NewTaggedStat("event_delivery", stats.CountType, stats.Tags{
			"module":         "router",
			"destType":       worker.rt.destName,
			"destID":         destination.ID,
			"destination":    destinationTag,
			"attempt_number": strconv.Itoa(status.AttemptNum),
			"workspaceId":    status.WorkspaceId,
			"source":         destinationJobMetadata.SourceID,
		})
		eventsDeliveredStat.Count(1)
		if destinationJobMetadata.ReceivedAt != "" {
			receivedTime, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.ReceivedAt)
			if err == nil {
				eventsDeliveryTimeStat := stats.Default.NewTaggedStat(
					"event_delivery_time", stats.TimerType, map[string]string{
						"module":         "router",
						"destType":       worker.rt.destName,
						"destID":         destination.ID,
						"destination":    destinationTag,
						"attempt_number": strconv.Itoa(status.AttemptNum),
						"workspaceId":    status.WorkspaceId,
					})

				eventsDeliveryTimeStat.SendTiming(time.Since(receivedTime))
			}
		}
	}
}

func (w *workerT) sendDestinationResponseToConfigBackend(payload json.RawMessage, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, sourceIDs []string) {
	// Sending destination response to config backend
	if status.ErrorCode != fmt.Sprint(types.RouterUnMarshalErrorCode) && status.ErrorCode != fmt.Sprint(types.RouterTimedOutStatusCode) {
		deliveryStatus := destinationdebugger.DeliveryStatusT{
			DestinationID: destinationJobMetadata.DestinationID,
			SourceID:      strings.Join(sourceIDs, ","),
			Payload:       payload,
			AttemptNum:    status.AttemptNum,
			JobState:      status.JobState,
			ErrorCode:     status.ErrorCode,
			ErrorResponse: status.ErrorResponse,
			SentAt:        status.ExecTime.Format(misc.RFC3339Milli),
			EventName:     gjson.GetBytes(destinationJobMetadata.JobT.Parameters, "event_name").String(),
			EventType:     gjson.GetBytes(destinationJobMetadata.JobT.Parameters, "event_type").String(),
		}
		w.rt.debugger.RecordEventDeliveryStatus(destinationJobMetadata.DestinationID, &deliveryStatus)
	}
}

func durationBeforeNextAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(math.Min(float64(maxRetryBackoff), float64(minRetryBackoff)*math.Exp2(float64(attempt-1))))
}

func (rt *HandleT) trackRequestMetrics(reqMetric requestMetric) {
	if diagnostics.EnableRouterMetric {
		rt.telemetry.requestsMetricLock.Lock()
		rt.telemetry.requestsMetric = append(rt.telemetry.requestsMetric, reqMetric)
		rt.telemetry.requestsMetricLock.Unlock()
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*workerT, rt.noOfWorkers)

	g, _ := errgroup.WithContext(context.Background())
	for i := 0; i < rt.noOfWorkers; i++ {
		worker := &workerT{
			channel: make(chan workerMessageT, noOfJobsPerChannel),
			barrier: eventorder.NewBarrier(
				eventorder.WithConcurrencyLimit(rt.allowAbortedUserJobsCountForProcessing),
				eventorder.WithMetadata(map[string]string{
					"destType":         rt.destName,
					"batching":         strconv.FormatBool(rt.enableBatching),
					"transformerProxy": strconv.FormatBool(rt.transformerProxy),
				})),
			retryForJobMap:            make(map[int64]time.Time),
			workerID:                  i,
			failedJobs:                0,
			routerJobs:                make([]types.RouterJobT, 0),
			destinationJobs:           make([]types.DestinationJobT, 0),
			rt:                        rt,
			deliveryTimeStat:          stats.Default.NewTaggedStat("router_delivery_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			batchTimeStat:             stats.Default.NewTaggedStat("router_batch_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			routerDeliveryLatencyStat: stats.Default.NewTaggedStat("router_delivery_latency", stats.TimerType, stats.Tags{"destType": rt.destName}),
			routerProxyStat:           stats.Default.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": rt.destName}),
		}
		rt.workers[i] = worker

		g.Go(misc.WithBugsnag(func() error {
			worker.workerProcess()
			return nil
		}))
	}
	rt.backgroundGroup.Go(func() error {
		err := g.Wait()

		// clean up channels workers are publishing to:
		close(rt.responseQ)
		rt.logger.Debugf("[%v Router] :: closing responseQ", rt.destName)
		return err
	})
}

func (rt *HandleT) stopWorkers() {
	for _, worker := range rt.workers {
		// FIXME remove paused worker, use shutdown instead
		close(worker.channel)
	}
}

func (rt *HandleT) findWorker(job *jobsdb.JobT, throttledUserMap map[string]struct{}) (toSendWorker *workerT) {
	if rt.backgroundCtx.Err() != nil {
		return
	}

	// checking if this job can be throttled
	var parameters JobParametersT
	userID := job.UserID

	// checking if the user is in throttledMap. If yes, returning nil.
	// this check is done to maintain order.
	if _, ok := throttledUserMap[userID]; ok && rt.guaranteeUserEventOrder {
		rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of user:%s as user has earlier jobs in throttled map`, rt.destName, job.JobID, userID)
		return nil
	}

	err := json.Unmarshal(job.Parameters, &parameters)
	if err != nil {
		rt.logger.Errorf(`[%v Router] :: Unmarshalling parameters failed with the error %v . Returning nil worker`, err)
		return
	}

	if !rt.guaranteeUserEventOrder {
		// if guaranteeUserEventOrder is false, assigning worker randomly and returning here.
		if rt.shouldThrottle(job, parameters, throttledUserMap) {
			return
		}
		toSendWorker = rt.workers[rand.Intn(rt.noOfWorkers)] // skipcq: GSC-G404
		return
	}

	//#JobOrder (see other #JobOrder comment)
	index := rt.getWorkerPartition(userID)
	worker := rt.workers[index]
	if worker.canBackoff(job) {
		return
	}
	orderKey := fmt.Sprintf(`%s:%s`, userID, parameters.DestinationID)
	enter, previousFailedJobID := worker.barrier.Enter(orderKey, job.JobID)
	if enter {
		rt.logger.Debugf("EventOrder: job %d of user %s is allowed to be processed", job.JobID, userID)
		if rt.shouldThrottle(job, parameters, throttledUserMap) {
			worker.barrier.Leave(orderKey, job.JobID)
			return
		}
		toSendWorker = worker
		return
	}
	previousFailedJobIDStr := "<nil>"
	if previousFailedJobID != nil {
		previousFailedJobIDStr = strconv.FormatInt(*previousFailedJobID, 10)
	}
	rt.logger.Debugf("EventOrder: job %d of user %s is blocked (previousFailedJobID: %s)", job.JobID, userID, previousFailedJobIDStr)
	return nil
	//#EndJobOrder
}

func (worker *workerT) canBackoff(job *jobsdb.JobT) (shouldBackoff bool) {
	// if the same job has failed before, check for next retry time
	worker.retryForJobMapMutex.RLock()
	defer worker.retryForJobMapMutex.RUnlock()
	if nextRetryTime, ok := worker.retryForJobMap[job.JobID]; ok && time.Until(nextRetryTime) > 0 {
		worker.rt.logger.Debugf("[%v Router] :: Less than next retry time: %v", worker.rt.destName, nextRetryTime)
		return true
	}
	return false
}

func (rt *HandleT) getWorkerPartition(userID string) int {
	return misc.GetHash(userID) % rt.noOfWorkers
}

func (rt *HandleT) shouldThrottle(job *jobsdb.JobT, parameters JobParametersT, throttledUserMap map[string]struct{}) (
	limited bool,
) {
	if rt.throttlerFactory == nil {
		// throttlerFactory could be nil when throttling is disabled or misconfigured.
		// in case of misconfiguration, logging errors are emitted.
		rt.logger.Debugf(`[%v Router] :: ThrottlerFactory is nil. Not throttling destination with ID %s`,
			rt.destName, parameters.DestinationID,
		)
		return false
	}

	throttler := rt.throttlerFactory.Get(rt.destName, parameters.DestinationID)
	throttlingCost := rt.getThrottlingCost(job)

	limited, err := throttler.CheckLimitReached(parameters.DestinationID, throttlingCost)
	if err != nil {
		// we can't throttle, let's hit the destination, worst case we get a 429
		rt.throttlingErrorStat.Count(1)
		rt.logger.Errorf(`[%v Router] :: Throttler error: %v`, rt.destName, err)
		return false
	}
	if limited {
		throttledUserMap[job.UserID] = struct{}{}
		rt.throttledStat.Count(1)
		rt.logger.Debugf(
			"[%v Router] :: Skipping processing of job:%d of user:%s as throttled limits exceeded",
			rt.destName, job.JobID, job.UserID,
		)
	}

	return limited
}

func (rt *HandleT) commitStatusList(responseList *[]jobResponseT) {
	reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	transformedAtMap := make(map[string]string)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)
	routerWorkspaceJobStatusCount := make(map[string]int)
	var completedJobsList []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var routerAbortedJobs []*jobsdb.JobT
	for _, resp := range *responseList {
		var parameters JobParametersT
		err := json.Unmarshal(resp.JobT.Parameters, &parameters)
		if err != nil {
			rt.logger.Error("Unmarshal of job parameters failed. ", string(resp.JobT.Parameters))
		}
		// Update metrics maps
		// REPORTING - ROUTER - START
		workspaceID := resp.status.WorkspaceId
		eventName := gjson.GetBytes(resp.JobT.Parameters, "event_name").String()
		eventType := gjson.GetBytes(resp.JobT.Parameters, "event_type").String()
		key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceJobRunID, resp.status.JobState, resp.status.ErrorCode, eventName, eventType)
		_, ok := connectionDetailsMap[key]
		if !ok {
			cd := utilTypes.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID, parameters.SourceDefinitionID, parameters.DestinationDefinitionID, parameters.SourceCategory)
			connectionDetailsMap[key] = cd
			transformedAtMap[key] = parameters.TransformAt
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
			errorCode, err := strconv.Atoi(resp.status.ErrorCode)
			if err != nil {
				errorCode = 200 // TODO handle properly
			}
			sampleEvent := resp.JobT.EventPayload
			if rt.transientSources.Apply(parameters.SourceID) {
				sampleEvent = routerutils.EmptyPayload
			}
			sd = utilTypes.CreateStatusDetail(resp.status.JobState, 0, errorCode, string(resp.status.ErrorResponse), sampleEvent, eventName, eventType)
			statusDetailsMap[key] = sd
		}

		switch resp.status.JobState {
		case jobsdb.Failed.State:
			if resp.status.ErrorCode != strconv.Itoa(types.RouterTimedOutStatusCode) && resp.status.ErrorCode != strconv.Itoa(types.RouterUnMarshalErrorCode) {
				rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, false, false)
				if resp.status.AttemptNum == 1 {
					sd.Count++
				}
			}
		case jobsdb.Succeeded.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
			rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, true, false)
			completedJobsList = append(completedJobsList, resp.JobT)
		case jobsdb.Aborted.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
			rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, false, true)
			routerAbortedJobs = append(routerAbortedJobs, resp.JobT)
			completedJobsList = append(completedJobsList, resp.JobT)
		}

		// REPORTING - ROUTER - END

		statusList = append(statusList, resp.status)

		// tracking router errors
		if diagnostics.EnableDestinationFailuresMetric {
			if resp.status.JobState == jobsdb.Failed.State || resp.status.JobState == jobsdb.Aborted.State {
				var event string
				if resp.status.JobState == jobsdb.Failed.State {
					event = diagnostics.RouterFailed
				} else {
					event = diagnostics.RouterAborted
				}

				rt.telemetry.failureMetricLock.Lock()
				if _, ok := rt.telemetry.failuresMetric[event][string(resp.status.ErrorResponse)]; !ok {
					rt.telemetry.failuresMetric[event] = make(map[string]int)
				}
				rt.telemetry.failuresMetric[event][string(resp.status.ErrorResponse)] += 1
				rt.telemetry.failureMetricLock.Unlock()
			}
		}
	}

	// REPORTING - ROUTER - START
	utilTypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
	for k, cd := range connectionDetailsMap {
		var inPu string
		if transformedAtMap[k] == "processor" {
			inPu = utilTypes.DEST_TRANSFORMER
		} else {
			inPu = utilTypes.EVENT_FILTER
		}
		m := &utilTypes.PUReportedMetric{
			ConnectionDetails: *cd,
			PUDetails:         *utilTypes.CreatePUDetails(inPu, utilTypes.ROUTER, true, false),
			StatusDetail:      statusDetailsMap[k],
		}
		if m.StatusDetail.Count != 0 {
			reportMetrics = append(reportMetrics, m)
		}
	}
	// REPORTING - ROUTER - END

	if len(statusList) > 0 {
		rt.logger.Debugf("[%v Router] :: flushing batch of %v status", rt.destName, updateStatusBatchSize)

		sort.Slice(statusList, func(i, j int) bool {
			return statusList[i].JobID < statusList[j].JobID
		})
		// Store the aborted jobs to errorDB
		if routerAbortedJobs != nil {
			err := misc.RetryWithNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
				return rt.errorDB.Store(ctx, routerAbortedJobs)
			}, sendRetryStoreStats)
			if err != nil {
				panic(fmt.Errorf("storing jobs into ErrorDB: %w", err))
			}
		}
		// Update the status
		err := misc.RetryWithNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
			return rt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
				err := rt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{rt.destName}, nil)
				if err != nil {
					return fmt.Errorf("updating %s jobs statuses: %w", rt.destName, err)
				}

				// rsources stats
				err = rt.updateRudderSourcesStats(ctx, tx, completedJobsList, statusList)
				if err != nil {
					return err
				}
				rt.Reporting.Report(reportMetrics, tx.SqlTx())
				return nil
			})
		}, sendRetryStoreStats)
		if err != nil {
			panic(err)
		}
		rt.updateProcessedEventsMetrics(statusList)
		for workspace, jobCount := range routerWorkspaceJobStatusCount {
			metric.DecreasePendingEvents(
				"rt",
				workspace,
				rt.destName,
				float64(jobCount),
			)
		}
	}

	if rt.guaranteeUserEventOrder {
		//#JobOrder (see other #JobOrder comment)
		for _, resp := range *responseList {
			status := resp.status.JobState
			userID := resp.userID
			worker := resp.worker
			if status != jobsdb.Failed.State {
				orderKey := fmt.Sprintf(`%s:%s`, userID, gjson.GetBytes(resp.JobT.Parameters, "destination_id").String())
				rt.logger.Debugf("EventOrder: [%d] job %d for key %s %s", worker.workerID, resp.status.JobID, orderKey, status)
				if err := worker.barrier.StateChanged(orderKey, resp.status.JobID, status); err != nil {
					panic(err)
				}
			}
		}
		// End #JobOrder
	}
}

// statusInsertLoop will run in a separate goroutine
// Blocking method, returns when rt.responseQ channel is closed.
func (rt *HandleT) statusInsertLoop() {
	var responseList []jobResponseT

	// Wait for the responses from statusQ
	lastUpdate := time.Now()

	statusStat := stats.Default.NewTaggedStat("router_status_loop", stats.TimerType, stats.Tags{"destType": rt.destName})
	countStat := stats.Default.NewTaggedStat("router_status_events", stats.CountType, stats.Tags{"destType": rt.destName})
	timeout := time.After(maxStatusUpdateWait)

	for {
		select {
		case jobStatus, hasMore := <-rt.responseQ:
			if !hasMore {
				if len(responseList) == 0 {
					rt.logger.Debugf("[%v Router] :: statusInsertLoop exiting", rt.destName)
					return
				}

				start := time.Now()
				rt.commitStatusList(&responseList)
				countStat.Count(len(responseList))
				responseList = nil
				statusStat.Since(start)

				rt.logger.Debugf("[%v Router] :: statusInsertLoop exiting", rt.destName)
				return
			}
			rt.logger.Debugf(
				"[%v Router] :: Got back status error %v and state %v for job %v",
				rt.destName,
				jobStatus.status.ErrorCode,
				jobStatus.status.JobState,
				jobStatus.status.JobID,
			)
			responseList = append(responseList, jobStatus)
		case <-timeout:
			timeout = time.After(maxStatusUpdateWait)
			// Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			// but approx is good enough at the cost of reduced computation.
		}
		if len(responseList) >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {
			start := time.Now()
			rt.commitStatusList(&responseList)
			countStat.Count(len(responseList))
			responseList = nil
			lastUpdate = time.Now()
			statusStat.Since(start)
		}
	}
}

func (rt *HandleT) collectMetrics(ctx context.Context) {
	if !diagnostics.EnableRouterMetric {
		return
	}

	for {
		select {
		case <-ctx.Done():
			rt.logger.Debugf("[%v Router] :: collectMetrics exiting", rt.destName)
			return
		case <-rt.telemetry.diagnosisTicker.C:
		}
		rt.telemetry.requestsMetricLock.RLock()
		var diagnosisProperties map[string]interface{}
		retries := 0
		aborted := 0
		success := 0
		var compTime time.Duration
		for _, reqMetric := range rt.telemetry.requestsMetric {
			retries += reqMetric.RequestRetries
			aborted += reqMetric.RequestAborted
			success += reqMetric.RequestSuccess
			compTime += reqMetric.RequestCompletedTime
		}
		if len(rt.telemetry.requestsMetric) > 0 {
			diagnosisProperties = map[string]interface{}{
				rt.destName: map[string]interface{}{
					diagnostics.RouterAborted:       aborted,
					diagnostics.RouterRetries:       retries,
					diagnostics.RouterSuccess:       success,
					diagnostics.RouterCompletedTime: (compTime / time.Duration(len(rt.telemetry.requestsMetric))) / time.Millisecond,
				},
			}

			Diagnostics.Track(diagnostics.RouterEvents, diagnosisProperties)
		}

		rt.telemetry.requestsMetric = nil
		rt.telemetry.requestsMetricLock.RUnlock()

		// This lock will ensure we don't send out Track Request while filling up the
		// failureMetric struct
		rt.telemetry.failureMetricLock.Lock()
		for key, value := range rt.telemetry.failuresMetric {
			var err error
			stringValueBytes, err := jsonfast.Marshal(value)
			if err != nil {
				stringValueBytes = []byte{}
			}

			Diagnostics.Track(key, map[string]interface{}{
				diagnostics.RouterDestination: rt.destName,
				diagnostics.Count:             len(value),
				diagnostics.ErrorCountMap:     string(stringValueBytes),
			})
		}
		rt.telemetry.failuresMetric = make(map[string]map[string]int)
		rt.telemetry.failureMetricLock.Unlock()
	}
}

//#JobOrder (see other #JobOrder comment)
// If a job fails (say with given failed_job_id), we need to fail other jobs from that user till
//the failed_job_id succeeds. We achieve this by keeping the failed_job_id in a failedJobIDMap
//structure (mapping userID to failed_job_id). All subsequent jobs (guaranteed to be job_id >= failed_job_id)
//are put in Waiting.State in worker loop till the failed_job_id succeeds.
//However, the step of removing failed_job_id from the failedJobIDMap structure is QUITE TRICKY.
//To understand that, need to understand the complete lifecycle of a job.
//The job goes through the following data-structures in order
//   i>   generatorLoop Buffer (read from DB)
//   ii>  requestQ (no longer used - RIP)
//   iii> Worker Process
//   iv>  responseQ
//   v>   statusInsertLoop Buffer (enough jobs are buffered before updating status)
// Now, when the failed_job_id eventually succeeds in the Worker Process (iii above),
// there may be pending jobs in all the other data-structures. For example, there
//may be jobs in responseQ(iv) and statusInsertLoop(v) buffer - all those jobs will
//be in Waiting state. Similarly, there may be other jobs in requestQ and generatorLoop
//buffer.
//If the failed_job_id succeeds, and we remove the filter gate, then all the jobs in requestQ
//will pass through before the jobs in responseQ/insertStatus buffer. That will violate the
//ordering of job.
//We fix this by removing an entry from the failedJobIDMap structure only when we are guaranteed
//that all the other structures are empty. We do the following to achieve this
// A. In generatorLoop, we do not let any job pass through except failed_job_id. That ensures requestQ is empty
// B. We wait for the failed_job_id status (when succeeded) to be sync'd to disk. This along with A ensures
//    that responseQ and statusInsertLoop Buffer are empty for that userID.
// C. Finally, we want for generatorLoop buffer to be fully processed.

func (rt *HandleT) generatorLoop(ctx context.Context) {
	rt.logger.Infof("Generator started for %s and destinationID %s", rt.destName, rt.destinationId)

	timeout := time.After(10 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			rt.logger.Infof("Generator exiting for router %s", rt.destName)
			return
		case <-timeout:

			start := time.Now()
			processCount := rt.readAndProcess()
			stats.Default.NewTaggedStat("router_generator_loop", stats.TimerType, stats.Tags{"destType": rt.destName}).Since(start)
			stats.Default.NewTaggedStat("router_generator_events", stats.CountType, stats.Tags{"destType": rt.destName}).Count(processCount)

			timeElapsed := time.Since(start)
			nextTimeout := time.Second - timeElapsed
			if nextTimeout < fixedLoopSleep {
				nextTimeout = fixedLoopSleep
			}
			timeout = time.After(nextTimeout)
		}
	}
}

func (rt *HandleT) getQueryParams(pickUpCount int) jobsdb.GetQueryParamsT {
	if rt.destinationId != rt.destName {
		return jobsdb.GetQueryParamsT{
			CustomValFilters: []string{rt.destName},
			ParameterFilters: []jobsdb.ParameterFilterT{{
				Name:     "destination_id",
				Value:    rt.destinationId,
				Optional: false,
			}},
			IgnoreCustomValFiltersInQuery: true,
			PayloadSizeLimit:              rt.adaptiveLimit(rt.payloadLimit),
			JobsLimit:                     pickUpCount,
		}
	}
	return jobsdb.GetQueryParamsT{
		CustomValFilters: []string{rt.destName},
		PayloadSizeLimit: rt.adaptiveLimit(rt.payloadLimit),
		JobsLimit:        pickUpCount,
	}
}

func (rt *HandleT) readAndProcess() int {
	//#JobOrder (See comment marked #JobOrder
	if rt.guaranteeUserEventOrder {
		for idx := range rt.workers {
			rt.workers[idx].barrier.Sync()
		}
	}

	timeOut := rt.routerTimeout
	timeElapsed := time.Since(rt.lastQueryRunTime)
	if timeElapsed < timeOut {
		timeOut = timeElapsed
	}
	rt.lastQueryRunTime = time.Now()

	pickupMap := rt.MultitenantI.GetRouterPickupJobs(rt.destName, rt.noOfWorkers, timeOut, jobQueryBatchSize)
	totalPickupCount := 0
	for _, pickup := range pickupMap {
		if pickup > 0 {
			totalPickupCount += pickup
		}
	}
	iterator := jobiterator.New(
		pickupMap,
		rt.getQueryParams(totalPickupCount),
		rt.getJobsFn(),
		jobiterator.WithDiscardedPercentageTolerance(rt.jobIteratorDiscardedPercentageTolerance),
		jobiterator.WithMaxQueries(rt.jobIteratorMaxQueries),
		jobiterator.WithLegacyOrderGroupKey(!misc.UseFairPickup()),
	)

	rt.logger.Debugf("[%v Router] :: pickupMap: %+v", rt.destName, pickupMap)

	if !iterator.HasNext() {
		rt.logger.Debugf("RT: DB Read Complete. No RT Jobs to process for destination: %s", rt.destName)
		time.Sleep(readSleep)
		return 0
	}

	// List of jobs which can be processed mapped per channel
	type workerJobT struct {
		worker *workerT
		job    *jobsdb.JobT
	}

	var statusList []*jobsdb.JobStatusT
	var toProcess []workerJobT
	throttledUserMap := make(map[string]struct{})

	// Identify jobs which can be processed
	for iterator.HasNext() {
		job := iterator.Next()
		w := rt.findWorker(job, throttledUserMap)
		if w != nil {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				JobState:      jobsdb.Executing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: routerutils.EmptyPayload, // check
				Parameters:    routerutils.EmptyPayload,
				WorkspaceId:   job.WorkspaceId,
			}
			statusList = append(statusList, &status)
			toProcess = append(toProcess, workerJobT{worker: w, job: job})
		} else {
			iterator.Discard(job)
		}
	}
	iteratorStats := iterator.Stats()
	stats.Default.NewTaggedStat("router_iterator_stats_query_count", stats.GaugeType, stats.Tags{"destType": rt.destName}).Gauge(iteratorStats.QueryCount)
	stats.Default.NewTaggedStat("router_iterator_stats_total_jobs", stats.GaugeType, stats.Tags{"destType": rt.destName}).Gauge(iteratorStats.TotalJobs)
	stats.Default.NewTaggedStat("router_iterator_stats_discarded_jobs", stats.GaugeType, stats.Tags{"destType": rt.destName}).Gauge(iteratorStats.DiscardedJobs)

	// Mark the jobs as executing
	err := misc.RetryWithNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
		return rt.jobsDB.UpdateJobStatus(ctx, statusList, []string{rt.destName}, nil)
	}, sendRetryUpdateStats)
	if err != nil {
		pkgLogger.Errorf("Error occurred while marking %s jobs statuses as executing. Panicking. Err: %v", rt.destName, err)
		panic(err)
	}

	rt.logger.Debugf("[DRAIN DEBUG] counts  %v final jobs length being processed %v", rt.destName, len(toProcess))

	if len(toProcess) == 0 {
		rt.logger.Debugf("RT: No workers found for the jobs. Sleeping. Destination: %s", rt.destName)
		time.Sleep(readSleep)
		return 0
	}

	workerAssignedTime := time.Now()
	// Send the jobs to the jobQ
	for _, wrkJob := range toProcess {
		wrkJob.worker.channel <- workerMessageT{job: wrkJob.job, workerAssignedTime: workerAssignedTime}
	}

	return len(toProcess)
}

func (rt *HandleT) getJobsFn() func(context.Context, map[string]int, jobsdb.GetQueryParamsT, jobsdb.MoreToken) (*jobsdb.GetAllJobsResult, error) {
	return func(ctx context.Context, pickupMap map[string]int, params jobsdb.GetQueryParamsT, resumeFrom jobsdb.MoreToken) (*jobsdb.GetAllJobsResult, error) {
		return misc.QueryWithRetriesAndNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) (*jobsdb.GetAllJobsResult, error) {
			return rt.jobsDB.GetAllJobs(
				ctx,
				pickupMap,
				params,
				rt.maxDSQuerySize,
				resumeFrom,
			)
		}, sendQueryRetryStats)
	}
}

func (*HandleT) crashRecover() {
	// NO-OP
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router")
	Diagnostics = diagnostics.Diagnostics
}

// Setup initializes this module
func (rt *HandleT) Setup(
	backendConfig backendconfig.BackendConfig,
	jobsDB jobsdb.MultiTenantJobsDB,
	errorDB jobsdb.JobsDB,
	destinationConfig destinationConfig,
	transientSources transientsource.Service,
	rsourcesService rsources.JobService,
	debugger destinationdebugger.DestinationDebugger,
) {
	rt.backendConfig = backendConfig
	rt.workspaceSet = make(map[string]struct{})
	rt.debugger = debugger

	destName := destinationConfig.name
	rt.logger = pkgLogger.Child(destName)
	rt.logger.Info("Router started: ", destinationConfig.destinationID)

	rt.transientSources = transientSources
	rt.rsourcesService = rsourcesService

	// waiting for reporting client setup
	err := rt.Reporting.WaitForSetup(context.TODO(), utilTypes.CoreReportingClient)
	if err != nil {
		return
	}

	rt.jobsDB = jobsDB
	rt.errorDB = errorDB
	rt.destName = destName
	rt.destinationId = destinationConfig.destinationID
	netClientTimeoutKeys := []string{"Router." + rt.destName + "." + "httpTimeout", "Router." + rt.destName + "." + "httpTimeoutInS", "Router." + "httpTimeout", "Router." + "httpTimeoutInS"}
	config.RegisterDurationConfigVariable(10, &rt.netClientTimeout, false, time.Second, netClientTimeoutKeys...)
	config.RegisterDurationConfigVariable(30, &rt.backendProxyTimeout, false, time.Second, "HttpClient.backendProxy.timeout")
	config.RegisterDurationConfigVariable(90, &rt.jobsDBCommandTimeout, true, time.Second, []string{"JobsDB.Router.CommandRequestTimeout", "JobsDB.CommandRequestTimeout"}...)
	config.RegisterIntConfigVariable(3, &rt.jobdDBMaxRetries, true, 1, []string{"JobsDB." + "Router." + "MaxRetries", "JobsDB." + "MaxRetries"}...)
	rt.crashRecover()
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	if rt.netHandle == nil {
		netHandle := &NetHandleT{}
		netHandle.logger = rt.logger.Child("network")
		netHandle.Setup(destName, rt.netClientTimeout)
		rt.netHandle = netHandle
	}

	rt.customDestinationManager = customDestinationManager.New(destName, customDestinationManager.Opts{
		Timeout: rt.netClientTimeout,
	})
	rt.telemetry = &DiagnosticT{}
	rt.telemetry.failuresMetric = make(map[string]map[string]int)
	rt.telemetry.diagnosisTicker = time.NewTicker(diagnosisTickerTime)

	rt.destinationResponseHandler = New(destinationConfig.responseRules)
	if value, ok := destinationConfig.config["saveDestinationResponse"].(bool); ok {
		rt.saveDestinationResponse = value
	}
	rt.guaranteeUserEventOrder = getRouterConfigBool("guaranteeUserEventOrder", rt.destName, true)
	rt.noOfWorkers = getRouterConfigInt("noOfWorkers", destName, 64)
	maxFailedCountKeys := []string{"Router." + rt.destName + "." + "maxFailedCountForJob", "Router." + "maxFailedCountForJob"}
	retryTimeWindowKeys := []string{"Router." + rt.destName + "." + "retryTimeWindow", "Router." + rt.destName + "." + "retryTimeWindowInMins", "Router." + "retryTimeWindow", "Router." + "retryTimeWindowInMins"}
	savePayloadOnErrorKeys := []string{"Router." + rt.destName + "." + "savePayloadOnError", "Router." + "savePayloadOnError"}
	transformerProxyKeys := []string{"Router." + rt.destName + "." + "transformerProxy", "Router." + "transformerProxy"}

	batchJobCountKeys := []string{"Router." + rt.destName + "." + "noOfJobsToBatchInAWorker", "Router." + "noOfJobsToBatchInAWorker"}
	config.RegisterIntConfigVariable(20, &rt.noOfJobsToBatchInAWorker, true, 1, batchJobCountKeys...)
	config.RegisterIntConfigVariable(3, &rt.maxFailedCountForJob, true, 1, maxFailedCountKeys...)
	routerPayloadLimitKeys := []string{"Router." + rt.destName + "." + "PayloadLimit", "Router." + "PayloadLimit"}
	config.RegisterInt64ConfigVariable(100*bytesize.MB, &rt.payloadLimit, true, 1, routerPayloadLimitKeys...)
	routerTimeoutKeys := []string{"Router." + rt.destName + "." + "routerTimeout", "Router." + "routerTimeout"}
	config.RegisterDurationConfigVariable(3600, &rt.routerTimeout, true, time.Second, routerTimeoutKeys...)
	config.RegisterDurationConfigVariable(180, &rt.retryTimeWindow, true, time.Minute, retryTimeWindowKeys...)
	maxDSQuerySizeKeys := []string{"Router." + rt.destName + "." + "maxDSQuery", "Router." + "maxDSQuery"}
	config.RegisterIntConfigVariable(10, &rt.maxDSQuerySize, true, 1, maxDSQuerySizeKeys...)

	config.RegisterIntConfigVariable(10, &rt.jobIteratorMaxQueries, true, 1, "Router.jobIterator.maxQueries")
	config.RegisterIntConfigVariable(10, &rt.jobIteratorDiscardedPercentageTolerance, true, 1, "Router.jobIterator.discardedPercentageTolerance")

	config.RegisterBoolConfigVariable(false, &rt.enableBatching, false, "Router."+rt.destName+"."+"enableBatching")
	config.RegisterBoolConfigVariable(false, &rt.savePayloadOnError, true, savePayloadOnErrorKeys...)
	config.RegisterBoolConfigVariable(false, &rt.transformerProxy, true, transformerProxyKeys...)
	// START: Alert configuration
	// We want to use these configurations to control what alerts we show via router-abort-count alert definition
	rtAbortTransformationKeys := []string{"Router." + rt.destName + "." + "skipRtAbortAlertForTf", "Router.skipRtAbortAlertForTf"}
	rtAbortDeliveryKeys := []string{"Router." + rt.destName + "." + "skipRtAbortAlertForDelivery", "Router.skipRtAbortAlertForDelivery"}

	config.RegisterBoolConfigVariable(false, &rt.skipRtAbortAlertForTransformation, true, rtAbortTransformationKeys...)
	config.RegisterBoolConfigVariable(false, &rt.skipRtAbortAlertForDelivery, true, rtAbortDeliveryKeys...)
	// END: Alert configuration
	rt.allowAbortedUserJobsCountForProcessing = getRouterConfigInt("allowAbortedUserJobsCountForProcessing", destName, 1)

	statTags := stats.Tags{"destType": rt.destName}
	rt.batchInputCountStat = stats.Default.NewTaggedStat("router_batch_num_input_jobs", stats.CountType, statTags)
	rt.batchOutputCountStat = stats.Default.NewTaggedStat("router_batch_num_output_jobs", stats.CountType, statTags)
	rt.routerTransformInputCountStat = stats.Default.NewTaggedStat("router_transform_num_input_jobs", stats.CountType, statTags)
	rt.routerTransformOutputCountStat = stats.Default.NewTaggedStat("router_transform_num_output_jobs", stats.CountType, statTags)
	rt.batchInputOutputDiffCountStat = stats.Default.NewTaggedStat("router_batch_input_output_diff_jobs", stats.CountType, statTags)
	rt.routerResponseTransformStat = stats.Default.NewTaggedStat("response_transform_latency", stats.TimerType, statTags)
	rt.throttlingErrorStat = stats.Default.NewTaggedStat("router_throttling_error", stats.CountType, statTags)
	rt.throttledStat = stats.Default.NewTaggedStat("router_throttled", stats.CountType, statTags)

	rt.transformer = transformer.NewTransformer(rt.netClientTimeout, rt.backendProxyTimeout)

	rt.oauth = oauth.NewOAuthErrorHandler(backendConfig)

	rt.isBackendConfigInitialized = false
	rt.backendConfigInitialized = make(chan bool)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	rt.backgroundCtx = ctx
	rt.backgroundGroup = g
	rt.backgroundCancel = cancel
	rt.backgroundWait = g.Wait
	rt.initWorkers()

	g.Go(misc.WithBugsnag(func() error {
		rt.collectMetrics(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
		rt.statusInsertLoop()
		return nil
	}))

	if rt.adaptiveLimit == nil {
		rt.adaptiveLimit = func(limit int64) int64 { return limit }
	}

	rruntime.Go(func() {
		rt.backendConfigSubscriber()
	})
	adminInstance.registerRouter(destName, rt)
}

func (rt *HandleT) Start() {
	rt.logger.Infof("Starting router: %s", rt.destName)
	rt.startEnded = make(chan struct{})
	ctx := rt.backgroundCtx

	rt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		defer close(rt.startEnded) // always close the channel
		defer rt.stopWorkers()     // workers are started before the generatorLoop, so always stop them
		select {
		case <-ctx.Done():
			rt.logger.Infof("Router : %s start goroutine exited", rt.destName)
			return nil
		case <-rt.backendConfigInitialized:
			// no-op, just wait
		}
		if rt.customDestinationManager != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-rt.customDestinationManager.BackendConfigInitialized():
				// no-op, just wait
			}
		}
		rt.generatorLoop(ctx)
		return nil
	}))
}

func (rt *HandleT) Shutdown() {
	if rt.startEnded == nil {
		// router is not started
		return
	}
	rt.logger.Infof("Shutting down router: %s destinationId: %s", rt.destName, rt.destinationId)
	rt.backgroundCancel()

	<-rt.startEnded
	_ = rt.backgroundWait()
}

func (rt *HandleT) backendConfigSubscriber() {
	ch := rt.backendConfig.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)
	for configEvent := range ch {
		rt.configSubscriberLock.Lock()
		rt.destinationsMap = map[string]*routerutils.BatchDestinationT{}
		configData := configEvent.Data.(map[string]backendconfig.ConfigT)
		rt.sourceIDWorkspaceMap = map[string]string{}
		for workspaceID, wConfig := range configData {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				rt.sourceIDWorkspaceMap[source.ID] = workspaceID
				for i := range source.Destinations {
					destination := &source.Destinations[i]
					if destination.DestinationDefinition.Name == rt.destName {
						if _, ok := rt.destinationsMap[destination.ID]; !ok {
							rt.destinationsMap[destination.ID] = &routerutils.BatchDestinationT{
								Destination: *destination,
								Sources:     []backendconfig.SourceT{},
							}
						}
						if _, ok := rt.workspaceSet[workspaceID]; !ok {
							rt.workspaceSet[workspaceID] = struct{}{}
							rt.MultitenantI.UpdateWorkspaceLatencyMap(rt.destName, workspaceID, 0)
						}
						rt.destinationsMap[destination.ID].Sources = append(rt.destinationsMap[destination.ID].Sources, *source)

						rt.destinationResponseHandler = New(destination.DestinationDefinition.ResponseRules)
						if value, ok := destination.DestinationDefinition.Config["saveDestinationResponse"].(bool); ok {
							rt.saveDestinationResponse = value
						}

						// Config key "throttlingCost" is expected to have the eventType as the first key and the call type
						// as the second key (e.g. track, identify, etc...) or default to apply the cost to all call types:
						// dDT["config"]["throttlingCost"] = `{"eventType":{"default":1,"track":2,"identify":3}}`
						if value, ok := destination.DestinationDefinition.Config["throttlingCost"].(map[string]interface{}); ok {
							m := types.NewEventTypeThrottlingCost(value)
							rt.throttlingCosts.Store(&m)
						}
					}
				}
			}
		}
		if !rt.isBackendConfigInitialized {
			rt.isBackendConfigInitialized = true
			rt.backendConfigInitialized <- true
		}
		rt.configSubscriberLock.Unlock()
	}
}

func (rt *HandleT) HandleOAuthDestResponse(params *HandleDestOAuthRespParamsT) (int, string) {
	trRespStatusCode := params.trRespStCd
	trRespBody := params.trRespBody
	destinationJob := params.destinationJob

	if trRespStatusCode != http.StatusOK {
		var destErrOutput integrations.TransResponseT
		if destError := json.Unmarshal([]byte(trRespBody), &destErrOutput); destError != nil {
			// Errors like OOM kills of transformer, transformer down etc...
			// If destResBody comes out with a plain string, then this will occur
			return http.StatusInternalServerError, fmt.Sprintf(`{
				Error: %v,
				(trRespStCd, trRespBody): (%v, %v),
			}`, destError, trRespStatusCode, trRespBody)
		}
		workspaceID := destinationJob.JobMetadataArray[0].WorkspaceID
		var errCatStatusCode int
		// Check the category
		// Trigger the refresh endpoint/disable endpoint
		rudderAccountID := oauth.GetAccountId(destinationJob.Destination.Config, oauth.DeliveryAccountIdKey)
		if strings.TrimSpace(rudderAccountID) == "" {
			return trRespStatusCode, trRespBody
		}
		switch destErrOutput.AuthErrorCategory {
		case oauth.DISABLE_DEST:
			return rt.ExecDisableDestination(&destinationJob.Destination, workspaceID, trRespBody, rudderAccountID)
		case oauth.REFRESH_TOKEN:
			var refSecret *oauth.AuthResponse
			refTokenParams := &oauth.RefreshTokenParams{
				Secret:          params.secret,
				WorkspaceId:     workspaceID,
				AccountId:       rudderAccountID,
				DestDefName:     destinationJob.Destination.DestinationDefinition.Name,
				EventNamePrefix: "refresh_token",
				WorkerId:        params.workerID,
			}
			errCatStatusCode, refSecret = rt.oauth.RefreshToken(refTokenParams)
			refSec := *refSecret
			if routerutils.IsNotEmptyString(refSec.Err) && refSec.Err == oauth.INVALID_REFRESH_TOKEN_GRANT {
				// In-case the refresh token has been revoked, this error comes in
				// Even trying to refresh the token also doesn't work here. Hence, this would be more ideal to Abort Events
				// As well as to disable destination as well.
				// Alert the user in this error as well, to check if the refresh token also has been revoked & fix it
				disableStCd, _ := rt.ExecDisableDestination(&destinationJob.Destination, workspaceID, trRespBody, rudderAccountID)
				stats.Default.NewTaggedStat(oauth.INVALID_REFRESH_TOKEN_GRANT, stats.CountType, stats.Tags{
					"destinationId": destinationJob.Destination.ID,
					"workspaceId":   refTokenParams.WorkspaceId,
					"accountId":     refTokenParams.AccountId,
					"destType":      refTokenParams.DestDefName,
					"flowType":      string(oauth.RudderFlow_Delivery),
				}).Increment()
				rt.logger.Errorf(`[OAuth request] Aborting the event as %v`, oauth.INVALID_REFRESH_TOKEN_GRANT)
				return disableStCd, refSec.Err
			}
			// Error while refreshing the token or Has an error while refreshing or sending empty access token
			if errCatStatusCode != http.StatusOK || routerutils.IsNotEmptyString(refSec.Err) {
				return http.StatusTooManyRequests, refSec.Err
			}
			// Retry with Refreshed Token by failing with 5xx
			return http.StatusInternalServerError, trRespBody
		}
	}
	// By default, send the status code & response from transformed response directly
	return trRespStatusCode, trRespBody
}

func (rt *HandleT) ExecDisableDestination(destination *backendconfig.DestinationT, workspaceID, destResBody, rudderAccountId string) (int, string) {
	disableDestStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceID,
		"success":     "true",
		"flowType":    string(oauth.RudderFlow_Delivery),
	}
	errCatStatusCode, errCatResponse := rt.oauth.DisableDestination(destination, workspaceID, rudderAccountId)
	if errCatStatusCode != http.StatusOK {
		// Error while disabling a destination
		// High-Priority notification to rudderstack needs to be sent
		disableDestStatTags["success"] = "false"
		stats.Default.NewTaggedStat("disable_destination_category_count", stats.CountType, disableDestStatTags).Increment()
		return http.StatusBadRequest, errCatResponse
	}
	// High-Priority notification to workspace(& rudderstack) needs to be sent
	stats.Default.NewTaggedStat("disable_destination_category_count", stats.CountType, disableDestStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest, destResBody
}

func (rt *HandleT) updateRudderSourcesStats(ctx context.Context, tx jobsdb.UpdateSafeTx, jobs []*jobsdb.JobT, jobStatuses []*jobsdb.JobStatusT) error {
	rsourcesStats := rsources.NewStatsCollector(rt.rsourcesService)
	rsourcesStats.BeginProcessing(jobs)
	rsourcesStats.JobStatusesUpdated(jobStatuses)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	if err != nil {
		rt.logger.Errorf("publishing rsources stats: %w", err)
	}
	return err
}

func (rt *HandleT) updateProcessedEventsMetrics(statusList []*jobsdb.JobStatusT) {
	eventsPerStateAndCode := map[string]map[string]int{}
	for i := range statusList {
		state := statusList[i].JobState
		code := statusList[i].ErrorCode
		if _, ok := eventsPerStateAndCode[state]; !ok {
			eventsPerStateAndCode[state] = map[string]int{}
		}
		eventsPerStateAndCode[state][code]++
	}
	for state, codes := range eventsPerStateAndCode {
		for code, count := range codes {
			stats.Default.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
				"module":   "router",
				"destType": rt.destName,
				"state":    state,
				"code":     code,
			}).Count(count)
		}
	}
}

func (rt *HandleT) getThrottlingCost(job *jobsdb.JobT) (cost int64) {
	cost = 1
	if tc := rt.throttlingCosts.Load(); tc != nil {
		eventType := gjson.GetBytes(job.Parameters, "event_type").String()
		cost = tc.Cost(eventType)
	}

	return cost * int64(job.EventCount)
}

package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/middleware"
	"github.com/rudderlabs/rudder-server/router"
	recovery "github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/sync/errgroup"

	"github.com/bugsnag/bugsnag-go/v2"
	uuid "github.com/gofrs/uuid"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/rruntime"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

/*
 * The gateway module handles incoming requests from client devices.
 * It batches web requests and writes to DB in bulk to improve I/O.
 * Only after the request payload is persisted, an ACK is sent to
 * the client.
 */

/*
 Basic WebRequest unit.

 Contains some payload, could be of several types(batch, identify, track etc.)

 has a `done` channel that receives a response(error if any)
*/
type webRequestT struct {
	done           chan<- string
	reqType        string
	requestPayload []byte
	writeKey       string
	ipAddr         string
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

var (
	webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess, adminWebPort         int
	maxUserWebRequestBatchSize, maxDBBatchSize, MaxHeaderBytes, maxConcurrentRequests int
	userWebRequestBatchTimeout, dbBatchWriteTimeout                                   time.Duration
	enabledWriteKeysSourceMap                                                         map[string]backendconfig.SourceT
	enabledWriteKeyWebhookMap                                                         map[string]string
	enabledWriteKeyWorkspaceMap                                                       map[string]string
	sourceIDToNameMap                                                                 map[string]string
	configSubscriberLock                                                              sync.RWMutex
	maxReqSize                                                                        int
	enableRateLimit                                                                   bool
	enableSuppressUserFeature                                                         bool
	enableEventSchemasFeature                                                         bool
	diagnosisTickerTime                                                               time.Duration
	ReadTimeout                                                                       time.Duration
	ReadHeaderTimeout                                                                 time.Duration
	WriteTimeout                                                                      time.Duration
	IdleTimeout                                                                       time.Duration
	allowReqsWithoutUserIDAndAnonymousID                                              bool
	gwAllowPartialWriteWithErrors                                                     bool
	pkgLogger                                                                         logger.LoggerI
	Diagnostics                                                                       diagnostics.DiagnosticsI
)

// CustomVal is used as a key in the jobsDB customval column
var CustomVal string

var BatchEvent = []byte(`
	{
		"batch": [
		]
	}
`)

const (
	DELIMITER = string("<<>>")
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("gateway")
	Diagnostics = diagnostics.Diagnostics
}

type userWorkerBatchRequestT struct {
	jobList     []*jobsdb.JobT
	respChannel chan map[uuid.UUID]string
}

type batchUserWorkerBatchRequestT struct {
	batchUserWorkerBatchRequest []*userWorkerBatchRequestT
}

//Basic worker unit that works on incoming webRequests.
//
//Has three channels used to communicate between the two goroutines each worker runs.
//
//One to receive new webRequests, one to send batches of said webRequests and the third to receive errors if any in response to sending the said batches to dbWriterWorker.
type userWebRequestWorkerT struct {
	webRequestQ                 chan *webRequestT
	batchRequestQ               chan *batchWebRequestT
	reponseQ                    chan map[uuid.UUID]string
	workerID                    int
	batchTimeStat               stats.RudderStats
	bufferFullStat, timeOutStat stats.RudderStats
}

//HandleT is the struct returned by the Setup call
type HandleT struct {
	application                  app.Interface
	userWorkerBatchRequestQ      chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ chan *batchUserWorkerBatchRequestT
	jobsDB                       jobsdb.JobsDB
	ackCount                     uint64
	recvCount                    uint64
	backendConfig                backendconfig.BackendConfig
	rateLimiter                  ratelimiter.RateLimiter

	stats                                         stats.Stats
	batchSizeStat                                 stats.RudderStats
	requestSizeStat                               stats.RudderStats
	dbWritesStat                                  stats.RudderStats
	dbWorkersBufferFullStat, dbWorkersTimeOutStat stats.RudderStats
	bodyReadTimeStat                              stats.RudderStats
	addToWebRequestQWaitTime                      stats.RudderStats
	ProcessRequestTime                            stats.RudderStats
	addToBatchRequestQWaitTime                    stats.RudderStats

	diagnosisTicker   *time.Ticker
	requestMetricLock sync.Mutex
	trackSuccessCount int
	trackFailureCount int

	webRequestBatchCount                                       uint64
	userWebRequestWorkers                                      []*userWebRequestWorkerT
	webhookHandler                                             *webhook.HandleT
	suppressUserHandler                                        types.SuppressUserI
	eventSchemaHandler                                         types.EventSchemasI
	versionHandler                                             func(w http.ResponseWriter, r *http.Request)
	logger                                                     logger.LoggerI
	rrh                                                        *RegularRequestHandler
	irh                                                        *ImportRequestHandler
	readonlyGatewayDB, readonlyRouterDB, readonlyBatchRouterDB jobsdb.ReadonlyJobsDB
	netHandle                                                  *http.Client
	httpTimeout                                                time.Duration
	httpWebServer                                              *http.Server
	backgroundCancel                                           context.CancelFunc
	backgroundWait                                             func() error
}

func (gateway *HandleT) updateSourceStats(sourceStats map[string]int, bucket string, sourceTagMap map[string]string) {
	for sourceTag, count := range sourceStats {
		tags := map[string]string{
			"source":      sourceTag,
			"writeKey":    sourceTagMap[sourceTag],
			"reqType":     sourceTagMap["reqType"],
			"workspaceId": sourceTagMap["workspaceId"],
		}
		sourceStatsD := gateway.stats.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

// Part of the gateway module Setup call.
// 	Initiates `maxUserWebRequestWorkerProcess` number of `webRequestWorkers` that listen on their `webRequestQ` for new WebRequests.
func (gateway *HandleT) initUserWebRequestWorkers() {
	gateway.userWebRequestWorkers = make([]*userWebRequestWorkerT, maxUserWebRequestWorkerProcess)
	for i := 0; i < maxUserWebRequestWorkerProcess; i++ {
		gateway.logger.Debug("User Web Request Worker Started", i)
		tags := map[string]string{
			"workerId": strconv.Itoa(i),
		}
		userWebRequestWorker := &userWebRequestWorkerT{
			webRequestQ:    make(chan *webRequestT, maxUserWebRequestBatchSize),
			batchRequestQ:  make(chan *batchWebRequestT),
			reponseQ:       make(chan map[uuid.UUID]string),
			workerID:       i,
			batchTimeStat:  gateway.stats.NewTaggedStat("gateway.batch_time", stats.TimerType, tags),
			bufferFullStat: gateway.stats.NewTaggedStat("gateway.user_request_worker_buffer_full", stats.CountType, tags),
			timeOutStat:    gateway.stats.NewTaggedStat("gateway.user_request_worker_time_out", stats.CountType, tags),
		}
		gateway.userWebRequestWorkers[i] = userWebRequestWorker
	}
}

// runUserWebRequestWorkers starts two goroutines for each worker:
// 	1. `userWebRequestBatcher` batches the webRequests that a worker gets
// 	2. `userWebRequestWorkerProcess` processes the requests in the batches and sends them as part of a `jobsList` to `dbWriterWorker`s.
func (gateway *HandleT) runUserWebRequestWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)

	for _, y := range gateway.userWebRequestWorkers {
		userWebRequestWorker := y
		g.Go(func() error {
			gateway.userWebRequestWorkerProcess(userWebRequestWorker)
			return nil
		})

		g.Go(func() error {
			gateway.userWebRequestBatcher(userWebRequestWorker)
			return nil
		})
	}
	g.Wait()

	close(gateway.userWorkerBatchRequestQ)
}

//Initiates `maxDBWriterProcess` number of dbWriterWorkers
func (gateway *HandleT) initDBWriterWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < maxDBWriterProcess; i++ {
		gateway.logger.Debug("DB Writer Worker Started", i)
		j := i
		g.Go(misc.WithBugsnag(func() error {
			gateway.dbWriterWorkerProcess(j)
			return nil
		}))
	}
	g.Wait()
}

// 	Batches together jobLists received on the `userWorkerBatchRequestQ` channel of the gateway
// 	and queues the batch at the `batchUserWorkerBatchRequestQ` channel of the gateway.
//
// Initiated during the gateway Setup and keeps batching jobLists received from webRequestWorkers
func (gateway *HandleT) userWorkerRequestBatcher() {
	var userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)

	timeout := time.After(dbBatchWriteTimeout)
	for {
		select {
		case userWorkerBatchRequest, hasMore := <-gateway.userWorkerBatchRequestQ:
			if !hasMore {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.batchUserWorkerBatchRequestQ <- &breq
				close(gateway.batchUserWorkerBatchRequestQ)
				return
			}
			//Append to request buffer
			userWorkerBatchRequestBuffer = append(userWorkerBatchRequestBuffer, userWorkerBatchRequest)
			if len(userWorkerBatchRequestBuffer) == maxDBBatchSize {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.dbWorkersBufferFullStat.Count(1)
				gateway.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(dbBatchWriteTimeout)
			if len(userWorkerBatchRequestBuffer) > 0 {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.dbWorkersTimeOutStat.Count(1)
				gateway.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		}
	}
}

//goes over the batches of jobslist, and stores each job in every jobList into gw_db
//sends a map of errors if any(errors mapped to the job.uuid) over the responseQ channel of the webRequestWorker.
//userWebRequestWorkerProcess method of the webRequestWorker is waiting for this errorMessageMap.
//This in turn sends the error over the done channel of each respcetive webRequest.
func (gateway *HandleT) dbWriterWorkerProcess(process int) {
	for breq := range gateway.batchUserWorkerBatchRequestQ {
		jobList := make([]*jobsdb.JobT, 0)
		var errorMessagesMap map[uuid.UUID]string

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			jobList = append(jobList, userWorkerBatchRequest.jobList...)
		}

		if gwAllowPartialWriteWithErrors {
			errorMessagesMap = gateway.jobsDB.StoreWithRetryEach(jobList)
		} else {
			err := gateway.jobsDB.Store(jobList)
			if err != nil {
				gateway.logger.Errorf("Store into gateway db failed with error: %v", err)
				gateway.logger.Errorf("JobList: %+v", jobList)
				panic(err)
			}
		}
		gateway.dbWritesStat.Count(1)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			userWorkerBatchRequest.respChannel <- errorMessagesMap
		}
	}
}

//Out of all the workers, this finds and returns the worker that works on a particular `userID`.
//
//This is done so that requests with a userID keep going to the same worker, which would maintain the consistency in event ordering.
func (gateway *HandleT) findUserWebRequestWorker(userID string) *userWebRequestWorkerT {

	index := int(math.Abs(float64(misc.GetHash(userID) % maxUserWebRequestWorkerProcess)))

	userWebRequestWorker := gateway.userWebRequestWorkers[index]
	if userWebRequestWorker == nil {
		panic(fmt.Errorf("worker is nil"))
	}

	return userWebRequestWorker
}

// 	This function listens on the `webRequestQ` channel of a worker.
// 	Based on `userWebRequestBatchTimeout` and `maxUserWebRequestBatchSize` parameters,
// 	batches them together and queues the batch of webreqs in the `batchRequestQ` channel of the worker
// Every webRequestWorker keeps doing this concurrently.
func (gateway *HandleT) userWebRequestBatcher(userWebRequestWorker *userWebRequestWorkerT) {
	var reqBuffer = make([]*webRequestT, 0)
	timeout := time.After(userWebRequestBatchTimeout)
	var start time.Time
	for {
		select {
		case req, ok := <-userWebRequestWorker.webRequestQ:
			if !ok {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				userWebRequestWorker.bufferFullStat.Count(1)
				start = time.Now()
				userWebRequestWorker.batchRequestQ <- &breq
				gateway.addToBatchRequestQWaitTime.SendTiming(time.Since(start))
				close(userWebRequestWorker.batchRequestQ)
				return
			}

			//Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxUserWebRequestBatchSize {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				userWebRequestWorker.bufferFullStat.Count(1)
				start = time.Now()
				userWebRequestWorker.batchRequestQ <- &breq
				gateway.addToBatchRequestQWaitTime.SendTiming(time.Since(start))
				reqBuffer = make([]*webRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(userWebRequestBatchTimeout)
			if len(reqBuffer) > 0 {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				userWebRequestWorker.timeOutStat.Count(1)
				start = time.Now()
				userWebRequestWorker.batchRequestQ <- &breq
				gateway.addToBatchRequestQWaitTime.SendTiming(time.Since(start))
				reqBuffer = make([]*webRequestT, 0)
			}
		}
	}
}

func (gateway *HandleT) getSourceTagFromWriteKey(writeKey string) string {
	sourceName := gateway.getSourceNameForWriteKey(writeKey)
	sourceTag := misc.GetTagName(writeKey, sourceName)
	return sourceTag
}

//	Listens on the `batchRequestQ` channel of the webRequestWorker for new batches of webRequests
//	Goes over the webRequests in the batch and filters them out(`rateLimit`, `maxReqSize`).
// 	And creates a `jobList` which is then sent to `userWorkerBatchRequestQ` of the gateway and waits for a response
// 	from the `dbwriterWorker`s that batch them and write to the db.
// Finally sends responses(error) if any back to the webRequests over their `done` channels
func (gateway *HandleT) userWebRequestWorkerProcess(userWebRequestWorker *userWebRequestWorkerT) {
	for breq := range userWebRequestWorker.batchRequestQ {
		counter := atomic.AddUint64(&gateway.webRequestBatchCount, 1)
		var jobList []*jobsdb.JobT
		var jobIDReqMap = make(map[uuid.UUID]*webRequestT)
		var jobWriteKeyMap = make(map[uuid.UUID]string)
		var jobEventCountMap = make(map[uuid.UUID]int)
		var sourceStats = make(map[string]int)
		var sourceEventStats = make(map[string]int)
		var sourceSuccessStats = make(map[string]int)
		var sourceSuccessEventStats = make(map[string]int)
		var sourceFailStats = make(map[string]int)
		var sourceFailEventStats = make(map[string]int)
		var workspaceDropRequestStats = make(map[string]int)
		var sourceTagMap = make(map[string]string)
		var preDbStoreCount int
		//Saving the event data read from req.request.Body to the splice.
		//Using this to send event schema to the config backend.
		var eventBatchesToRecord []string
		userWebRequestWorker.batchTimeStat.Start()
		for _, req := range breq.batchRequest {
			writeKey := req.writeKey
			sourceTag := gateway.getSourceTagFromWriteKey(writeKey)
			sourceTagMap[sourceTag] = writeKey
			sourceTagMap["reqType"] = req.reqType
			misc.IncrementMapByKey(sourceStats, sourceTag, 1)
			//Should be function of body
			configSubscriberLock.RLock()
			workspaceId := enabledWriteKeyWorkspaceMap[writeKey]
			configSubscriberLock.RUnlock()

			sourceTagMap["workspaceId"] = workspaceId
			ipAddr := req.ipAddr

			body := req.requestPayload

			if !gjson.ValidBytes(body) {
				req.done <- response.GetStatus(response.InvalidJSON)
				preDbStoreCount++
				misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
				continue
			}

			gateway.requestSizeStat.Observe(float64(len(body)))
			if req.reqType != "batch" {
				body, _ = sjson.SetBytes(body, "type", req.reqType)
				body, _ = sjson.SetRawBytes(BatchEvent, "batch.0", body)
			}
			totalEventsInReq := len(gjson.GetBytes(body, "batch").Array())
			misc.IncrementMapByKey(sourceEventStats, sourceTag, totalEventsInReq)

			// store sourceID before call made to check if source is enabled
			// this prevents not setting sourceID in gw job if disabled before setting it
			sourceID := gateway.getSourceIDForWriteKey(writeKey)
			if !gateway.isWriteKeyEnabled(writeKey) {
				req.done <- response.GetStatus(response.InvalidWriteKey)
				preDbStoreCount++
				misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
				misc.IncrementMapByKey(sourceFailEventStats, sourceTag, totalEventsInReq)
				continue
			}

			if enableRateLimit {
				//In case of "batch" requests, if ratelimiter returns true for LimitReached, just drop the event batch and continue.
				restrictorKey := gateway.backendConfig.GetWorkspaceIDForWriteKey(writeKey)
				if gateway.rateLimiter.LimitReached(restrictorKey) {
					req.done <- response.GetStatus(response.TooManyRequests)
					preDbStoreCount++
					misc.IncrementMapByKey(workspaceDropRequestStats, sourceTag, 1)
					continue
				}
			}

			// set anonymousId if not set in payload
			result := gjson.GetBytes(body, "batch")
			out := []map[string]interface{}{}
			var builtUserID string
			var notIdentifiable, containsAudienceList bool
			result.ForEach(func(_, vjson gjson.Result) bool {
				anonIDFromReq := strings.TrimSpace(vjson.Get("anonymousId").String())
				userIDFromReq := strings.TrimSpace(vjson.Get("userId").String())
				if builtUserID == "" {
					builtUserID = anonIDFromReq + DELIMITER + userIDFromReq
				}

				eventTypeFromReq := strings.TrimSpace(vjson.Get("type").String())

				if anonIDFromReq == "" {
					if userIDFromReq == "" && !allowReqsWithoutUserIDAndAnonymousID {
						notIdentifiable = true
						return false
					}
				}
				if eventTypeFromReq == "audiencelist" {
					containsAudienceList = true
				}
				// hashing combination of userIDFromReq + anonIDFromReq, using colon as a delimiter
				rudderId, err := misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
				if err != nil {
					notIdentifiable = true
					return false
				}

				toSet := vjson.Value().(map[string]interface{})
				toSet["rudderId"] = rudderId
				if messageId := strings.TrimSpace(vjson.Get("messageId").String()); messageId == "" {
					toSet["messageId"] = uuid.Must(uuid.NewV4()).String()
				}
				out = append(out, toSet)
				return true // keep iterating
			})

			if len(body) > maxReqSize && !containsAudienceList {
				req.done <- response.GetStatus(response.RequestBodyTooLarge)
				preDbStoreCount++
				misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
				misc.IncrementMapByKey(sourceFailEventStats, sourceTag, totalEventsInReq)
				continue
			}

			body, _ = sjson.SetBytes(body, "batch", out)

			if notIdentifiable {
				req.done <- response.GetStatus(response.NonIdentifiableRequest)
				preDbStoreCount++
				misc.IncrementMapByKey(sourceFailStats, "notIdentifiable", 1)
				continue
			}

			if enableSuppressUserFeature && gateway.suppressUserHandler != nil {
				userID := gjson.GetBytes(body, "batch.0.userId").String()
				if gateway.suppressUserHandler.IsSuppressedUser(userID, gateway.getSourceIDForWriteKey(writeKey), writeKey) {
					req.done <- ""
					preDbStoreCount++
					continue
				}
			}

			body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
			body, _ = sjson.SetBytes(body, "writeKey", writeKey)
			body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(misc.RFC3339Milli))
			eventBatchesToRecord = append(eventBatchesToRecord, fmt.Sprintf("%s", body))
			sourcesJobRunID := gjson.GetBytes(body, "batch.0.context.sources.job_run_id").Str // pick the job_run_id from the first event of batch. We are assuming job_run_id will be same for all events in a batch and the batch is coming from rudder-sources
			id := uuid.Must(uuid.NewV4())

			params := map[string]interface{}{
				"source_id":         sourceID,
				"batch_id":          counter,
				"source_job_run_id": sourcesJobRunID,
			}
			marshalledParams, err := json.Marshal(params)
			if err != nil {
				gateway.logger.Errorf("[Gateway] Failed to marshal parameters map. Parameters: %+v", params)
				marshalledParams = []byte(`{"error": "rudder-server gateway failed to marshal params"}`)
			}

			newJob := jobsdb.JobT{
				UUID:         id,
				UserID:       builtUserID,
				Parameters:   marshalledParams,
				CustomVal:    CustomVal,
				EventPayload: []byte(body),
				EventCount:   totalEventsInReq,
				WorkspaceId:  workspaceId,
			}
			jobList = append(jobList, &newJob)

			jobIDReqMap[newJob.UUID] = req
			jobWriteKeyMap[newJob.UUID] = sourceTag
			jobEventCountMap[newJob.UUID] = totalEventsInReq
		}

		errorMessagesMap := make(map[uuid.UUID]string)
		if len(jobList) > 0 {
			gateway.userWorkerBatchRequestQ <- &userWorkerBatchRequestT{jobList: jobList,
				respChannel: userWebRequestWorker.reponseQ,
			}

			errorMessagesMap = <-userWebRequestWorker.reponseQ
		}

		if preDbStoreCount+len(jobList) != len(breq.batchRequest) {
			panic(fmt.Errorf("preDbStoreCount:%d+len(jobList):%d != len(breq.batchRequest):%d",
				preDbStoreCount, len(jobList), len(breq.batchRequest)))
		}
		for _, job := range jobList {
			err, found := errorMessagesMap[job.UUID]
			if found {
				misc.IncrementMapByKey(sourceFailStats, jobWriteKeyMap[job.UUID], 1)
				misc.IncrementMapByKey(sourceFailEventStats, jobWriteKeyMap[job.UUID], jobEventCountMap[job.UUID])
			} else {
				misc.IncrementMapByKey(sourceSuccessStats, jobWriteKeyMap[job.UUID], 1)
				misc.IncrementMapByKey(sourceSuccessEventStats, jobWriteKeyMap[job.UUID], jobEventCountMap[job.UUID])
			}
			jobIDReqMap[job.UUID].done <- err
		}
		//Sending events to config backend
		for _, eventBatch := range eventBatchesToRecord {
			writeKey := gjson.Get(eventBatch, "writeKey").Str
			sourcedebugger.RecordEvent(writeKey, eventBatch)
		}

		userWebRequestWorker.batchTimeStat.End()
		gateway.batchSizeStat.Observe(float64(len(breq.batchRequest)))
		// update stats request wise
		gateway.updateSourceStats(sourceStats, "gateway.write_key_requests", sourceTagMap)
		gateway.updateSourceStats(sourceSuccessStats, "gateway.write_key_successful_requests", sourceTagMap)
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_failed_requests", sourceTagMap)
		if enableRateLimit {
			gateway.updateSourceStats(workspaceDropRequestStats, "gateway.work_space_dropped_requests", sourceTagMap)
		}
		// update stats event wise
		gateway.updateSourceStats(sourceEventStats, "gateway.write_key_events", sourceTagMap)
		gateway.updateSourceStats(sourceSuccessEventStats, "gateway.write_key_successful_events", sourceTagMap)
		gateway.updateSourceStats(sourceFailEventStats, "gateway.write_key_failed_events", sourceTagMap)
	}

}

func (gateway *HandleT) isWriteKeyEnabled(writeKey string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	_, ok := enabledWriteKeysSourceMap[writeKey]
	return ok
}

func (gateway *HandleT) getSourceIDForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := enabledWriteKeysSourceMap[writeKey]; ok {
		return enabledWriteKeysSourceMap[writeKey].ID
	}

	return ""
}

func (gateway *HandleT) getSourceNameForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := enabledWriteKeysSourceMap[writeKey]; ok {
		return enabledWriteKeysSourceMap[writeKey].Name
	}

	return "-notFound-"
}

func (gateway *HandleT) printStats(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}

		recvCount := atomic.LoadUint64(&gateway.recvCount)
		ackCount := atomic.LoadUint64(&gateway.ackCount)
		gateway.logger.Debug("Gateway Recv/Ack ", recvCount, ackCount)
	}
}

func (gateway *HandleT) eventSchemaWebHandler(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !enableEventSchemasFeature {
			gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Response: 400, %s", misc.GetIPFromReq(r), r.URL.Path, response.MakeResponse("EventSchemas feature is disabled")))
			http.Error(w, response.MakeResponse("EventSchemas feature is disabled"), 400)
			return
		}
		wrappedFunc(w, r)
	}
}

func (gateway *HandleT) getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte{}, errors.New(response.RequestBodyNil)
	}

	start := time.Now()
	defer gateway.bodyReadTimeStat.Since(start)

	payload, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		gateway.logger.Errorf(
			"Error reading request body, 'Content-Length': %s, partial payload:\n\t%s\n",
			r.Header.Get("Content-Length"),
			string(payload),
		)
		return payload, fmt.Errorf("read all request body: %w", err)
	}
	return payload, nil
}

func (gateway *HandleT) webImportHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webRequestHandler(gateway.irh, w, r, "import")
}

func (gateway *HandleT) webAudienceListHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "audiencelist")
}

func (gateway *HandleT) webBatchHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "batch")
}

func (gateway *HandleT) webIdentifyHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "identify")
}

func (gateway *HandleT) webTrackHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "track")
}

func (gateway *HandleT) webPageHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "page")
}

func (gateway *HandleT) webScreenHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "screen")
}

func (gateway *HandleT) webAliasHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "alias")
}

func (gateway *HandleT) webMergeHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "merge")
}

func (gateway *HandleT) webGroupHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "group")
}

func (gateway *HandleT) pixelPageHandler(w http.ResponseWriter, r *http.Request) {
	gateway.pixelHandler(w, r, "page")
}

func (gateway *HandleT) pixelTrackHandler(w http.ResponseWriter, r *http.Request) {
	gateway.pixelHandler(w, r, "track")
}

func (gateway *HandleT) beaconBatchHandler(w http.ResponseWriter, r *http.Request) {
	gateway.beaconHandler(w, r, "batch")
}

type pendingEventsRequestPayload struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
	JobRunID      string `json:"job_run_id"`
}

func (gateway *HandleT) pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	//Force return that there are pending
	if config.GetBool("Gateway.DisablePendingEvents", false) {
		w.Write([]byte(`{ "pending_events": 1 }`))
		return
	}

	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string

	if !recovery.IsNormalMode() {
		errorMessage = "server not in normal mode"
		defer http.Error(w, errorMessage, 500)
		gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Response: 500, %s", misc.GetIPFromReq(r), r.URL.Path, errorMessage))
		return
	}

	defer func() {
		if errorMessage != "" {
			gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Response: 400, %s", misc.GetIPFromReq(r), r.URL.Path, errorMessage))
			http.Error(w, errorMessage, 400)
		}
	}()

	payload, _, err := gateway.getPayloadAndWriteKey(w, r, "pending-events")
	if err != nil {
		errorMessage = err.Error()
		return
	}

	if !gjson.ValidBytes(payload) {
		errorMessage = response.GetStatus(response.InvalidJSON)
		return
	}

	var reqPayload pendingEventsRequestPayload
	err = json.Unmarshal(payload, &reqPayload)
	if err != nil {
		errorMessage = err.Error()
		return
	}

	if reqPayload.SourceID == "" {
		errorMessage = "Empty source id"
		return
	}

	gwParameterFilters := []jobsdb.ParameterFilterT{
		{
			Name:     "source_id",
			Value:    reqPayload.SourceID,
			Optional: false,
		},
	}

	var rtParameterFilters []jobsdb.ParameterFilterT
	if reqPayload.DestinationID == "" {
		rtParameterFilters = gwParameterFilters
	} else {
		rtParameterFilters = []jobsdb.ParameterFilterT{
			{
				Name:     "source_id",
				Value:    reqPayload.SourceID,
				Optional: false,
			},
			{
				Name:     "destination_id",
				Value:    reqPayload.DestinationID,
				Optional: false,
			},
		}
	}
	if reqPayload.JobRunID != "" {
		jobRunIDParam := jobsdb.ParameterFilterT{
			Name:     "source_job_run_id",
			Value:    reqPayload.JobRunID,
			Optional: false,
		}
		gwParameterFilters = append(gwParameterFilters, jobRunIDParam)
		rtParameterFilters = append(rtParameterFilters, jobRunIDParam)
	}

	var excludeGateway bool
	excludeGatewayStr := r.URL.Query().Get("exclude_gateway")
	if excludeGatewayStr != "" {
		val, err := strconv.ParseBool(excludeGatewayStr)
		if err == nil {
			excludeGateway = val
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), config.GetDuration("Gateway.pendingEventsQueryTimeout", 10, time.Second))
	defer cancel()

	var pending bool
	if !excludeGateway {
		pending, err = gateway.readonlyGatewayDB.HavePendingJobs(ctx, []string{CustomVal}, -1, gwParameterFilters)
		if err != nil || pending {
			w.Write([]byte(`{ "pending_events": 1 }`))
			return
		}
	}

	pending, err = gateway.readonlyRouterDB.HavePendingJobs(ctx, nil, -1, rtParameterFilters)
	if err != nil || pending {
		w.Write([]byte(`{ "pending_events": 1 }`))
		return
	}

	pending, err = gateway.readonlyBatchRouterDB.HavePendingJobs(ctx, nil, -1, rtParameterFilters)
	if err != nil || pending {
		w.Write([]byte(`{ "pending_events": 1 }`))
		return
	}

	w.Write([]byte(fmt.Sprintf("{ \"pending_events\": %d }", getIntResponseFromBool(gateway.getWarehousePending(payload)))))
}

func getIntResponseFromBool(resp bool) int {
	if resp {
		return 1
	}
	return 0
}

func (gateway *HandleT) getWarehousePending(payload []byte) bool {
	uri := fmt.Sprintf(`%s/v1/warehouse/pending-events?triggerUpload=true`, misc.GetWarehouseURL())
	resp, err := gateway.netHandle.Post(uri, "application/json; charset=utf-8",
		bytes.NewBuffer(payload))
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	var whPendingResponse warehouseutils.PendingEventsResponseT
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	err = json.Unmarshal(respData, &whPendingResponse)
	if err != nil {
		return false
	}

	return whPendingResponse.PendingEvents
}

type failedEventsRequestPayload struct {
	TaskRunID string `json:"task_run_id"`
}

func (gateway *HandleT) fetchFailedEventsHandler(w http.ResponseWriter, r *http.Request) {
	gateway.failedEventsHandler(w, r, "fetch")
}

func (gateway *HandleT) clearFailedEventsHandler(w http.ResponseWriter, r *http.Request) {
	gateway.failedEventsHandler(w, r, "clear")
}

func (gateway *HandleT) failedEventsHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)

	if !recovery.IsNormalMode() {
		errorMessage := "server not in normal mode"
		defer http.Error(w, errorMessage, 500)
		gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Response: 500, %s", misc.GetIPFromReq(r), r.URL.Path, errorMessage))
		return
	}

	var err error
	var payload []byte
	defer func() {
		if err != nil {
			gateway.logger.Debug(err.Error())
			http.Error(w, err.Error(), 400)
		}
	}()

	payload, _, err = gateway.getPayloadAndWriteKey(w, r, reqType)
	if err != nil {
		return
	}

	if !gjson.ValidBytes(payload) {
		err = errors.New(response.GetStatus(response.InvalidJSON))
		return
	}

	var reqPayload failedEventsRequestPayload
	err = json.Unmarshal(payload, &reqPayload)
	if err != nil {
		return
	}
	if reqPayload.TaskRunID == "" {
		err = errors.New("empty task run id")
		return
	}
	var resp []byte
	switch reqType {
	case "fetch":
		failedEvents := router.GetFailedEventsManager().FetchFailedRecordIDs(reqPayload.TaskRunID)
		failedMsgIDsByDestinationID := make(map[string][]interface{})
		for _, failedEvent := range failedEvents {
			if _, ok := failedMsgIDsByDestinationID[failedEvent.DestinationID]; !ok {
				failedMsgIDsByDestinationID[failedEvent.DestinationID] = []interface{}{}
			}
			failedMsgIDsByDestinationID[failedEvent.DestinationID] = append(failedMsgIDsByDestinationID[failedEvent.DestinationID], failedEvent.RecordID)
		}

		resp, err = json.Marshal(failedMsgIDsByDestinationID)
		if err != nil {
			return
		}

	case "clear":
		router.GetFailedEventsManager().DropFailedRecordIDs(reqPayload.TaskRunID)
		resp = []byte("OK")
	}

	w.Write(resp)
}

//ProcessRequest throws a webRequest into the queue and waits for the response before returning
func (rrh *RegularRequestHandler) ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	done := make(chan string, 1)
	start := time.Now()
	gateway.addToWebRequestQ(w, r, done, reqType, payload, writeKey)
	gateway.addToWebRequestQWaitTime.SendTiming(time.Since(start))
	defer gateway.ProcessRequestTime.Since(start)
	errorMessage := <-done
	return errorMessage
}

//RequestHandler interface for abstracting out server-side import request processing and rest of the calls
type RequestHandler interface {
	ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string
}

//ImportRequestHandler is an empty struct to capture import specific request handling functionality
type ImportRequestHandler struct {
}

//RegularRequestHandler is an empty struct to capture non-import specific request handling functionality
type RegularRequestHandler struct {
}

//ProcessWebRequest is an Interface wrapper for webhook
func (gateway *HandleT) ProcessWebRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	return gateway.rrh.ProcessRequest(gateway, w, r, reqType, payload, writeKey)
}

func (gateway *HandleT) getPayloadAndWriteKey(w http.ResponseWriter, r *http.Request, reqType string) ([]byte, string, error) {
	var sourceFailStats = make(map[string]int)
	var err error
	writeKey, _, ok := r.BasicAuth()
	if !ok || writeKey == "" {
		err = errors.New(response.NoWriteKeyInBasicAuth)
		misc.IncrementMapByKey(sourceFailStats, "noWriteKey", 1)
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_failed_requests", map[string]string{"noWriteKey": "noWriteKey", "reqType": reqType})
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_requests", map[string]string{"noWriteKey": "noWriteKey", "reqType": reqType})

		return []byte{}, "", err
	}
	payload, err := gateway.getPayloadFromRequest(r)
	if err != nil {
		sourceTag := gateway.getSourceTagFromWriteKey(writeKey)
		misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_failed_requests", map[string]string{sourceTag: writeKey, "reqType": reqType})
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_requests", map[string]string{sourceTag: writeKey, "reqType": reqType})
		return []byte{}, writeKey, fmt.Errorf("read payload from request: %w", err)
	}
	return payload, writeKey, err
}

func (gateway *HandleT) pixelWebHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.pixelWebRequestHandler(gateway.rrh, w, r, reqType)
}

func (gateway *HandleT) webHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.webRequestHandler(gateway.rrh, w, r, reqType)
}

func (gateway *HandleT) webRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	webReqHandlerTime := gateway.stats.NewTaggedStat("gateway.web_req_handler_time", stats.TimerType, stats.Tags{"reqType": reqType})
	webReqHandlerStartTime := time.Now()
	defer webReqHandlerTime.Since(webReqHandlerStartTime)

	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			if strings.Contains(errorMessage, response.GetStatus(response.TooManyRequests)) {
				gateway.logger.Infof("IP: %s -- %s -- Response: %d, %s", misc.GetIPFromReq(r), r.URL.Path, http.StatusTooManyRequests, errorMessage)
				http.Error(w, errorMessage, http.StatusTooManyRequests)
				return
			}
			gateway.logger.Infof("IP: %s -- %s -- Response: 400, %s", misc.GetIPFromReq(r), r.URL.Path, errorMessage)
			http.Error(w, errorMessage, 400)
		}
	}()
	payload, writeKey, err := gateway.getPayloadAndWriteKey(w, r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	errorMessage = rh.ProcessRequest(gateway, &w, r, reqType, payload, writeKey)
	atomic.AddUint64(&gateway.ackCount, 1)
	gateway.trackRequestMetrics(errorMessage)
	if errorMessage != "" {
		return
	}
	gateway.logger.Debugf("IP: %s -- %s -- Response: 200, %s", misc.GetIPFromReq(r), r.URL.Path, response.GetStatus(response.Ok))

	httpWriteTime := gateway.stats.NewTaggedStat("gateway.http_write_time", stats.TimerType, stats.Tags{"reqType": reqType})
	httpWriteStartTime := time.Now()
	w.Write([]byte(response.GetStatus(response.Ok)))
	httpWriteTime.Since(httpWriteStartTime)
}

func (gateway *HandleT) pixelWebRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	sendPixelResponse(w)
	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Error while handling request: %s", misc.GetIPFromReq(r), r.URL.Path, errorMessage))
		}
	}()
	payload, writeKey, err := gateway.getPayloadAndWriteKey(w, r, reqType)
	if err != nil {
		errorMessage = err.Error()
		return
	}
	errorMessage = rh.ProcessRequest(gateway, &w, r, reqType, payload, writeKey)

	atomic.AddUint64(&gateway.ackCount, 1)
	gateway.trackRequestMetrics(errorMessage)
}

//ProcessRequest on ImportRequestHandler splits payload by user and throws them into the webrequestQ and waits for all their responses before returning
func (irh *ImportRequestHandler) ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	errorMessage := ""
	usersPayload, payloadError := gateway.getUsersPayload(payload)
	if payloadError != nil {
		return payloadError.Error()
	}
	count := len(usersPayload)
	done := make(chan string, count)
	for key := range usersPayload {
		gateway.addToWebRequestQ(w, r, done, "batch", usersPayload[key], writeKey)
	}

	interimMsgs := []string{}
	for index := 0; index < count; index++ {
		interimErrorMessage := <-done
		interimMsgs = append(interimMsgs, interimErrorMessage)
	}
	errorMessage = strings.Join(interimMsgs[:], "")

	return errorMessage
}

func (gateway *HandleT) getUsersPayload(requestPayload []byte) (map[string][]byte, error) {
	userMap := make(map[string][][]byte)
	var index int

	if !gjson.ValidBytes(requestPayload) {
		return make(map[string][]byte), errors.New(response.InvalidJSON)
	}

	result := gjson.GetBytes(requestPayload, "batch")

	result.ForEach(func(_, _ gjson.Result) bool {
		anonIDFromReq := strings.TrimSpace(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v.anonymousId`, index)).String())
		userIDFromReq := strings.TrimSpace(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v.userId`, index)).String())
		rudderID, err := misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			return false
		}
		userMap[rudderID.String()] = append(userMap[rudderID.String()], []byte(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v`, index)).String()))
		index++
		return true
	})
	recontructedUserMap := make(map[string][]byte)
	for key := range userMap {
		var tempValue string
		var err error
		for index = 0; index < len(userMap[key]); index++ {
			tempValue, err = sjson.SetRaw(tempValue, fmt.Sprintf("batch.%v", index), string(userMap[key][index]))
			if err != nil {
				return recontructedUserMap, err
			}
		}
		recontructedUserMap[key] = []byte(tempValue)
	}
	return recontructedUserMap, nil
}

func (gateway *HandleT) trackRequestMetrics(errorMessage string) {
	if diagnostics.EnableGatewayMetric {
		gateway.requestMetricLock.Lock()
		defer gateway.requestMetricLock.Unlock()
		if errorMessage != "" {
			gateway.trackFailureCount = gateway.trackFailureCount + 1
		} else {
			gateway.trackSuccessCount = gateway.trackSuccessCount + 1
		}
	}
}

func (gateway *HandleT) collectMetrics(ctx context.Context) {
	if diagnostics.EnableGatewayMetric {
		for {
			select {
			case <-ctx.Done():
				return
			case <-gateway.diagnosisTicker.C:
				gateway.requestMetricLock.Lock()
				if gateway.trackSuccessCount > 0 || gateway.trackFailureCount > 0 {
					Diagnostics.Track(diagnostics.GatewayEvents, map[string]interface{}{
						diagnostics.GatewaySuccess: gateway.trackSuccessCount,
						diagnostics.GatewayFailure: gateway.trackFailureCount,
					})
					gateway.trackSuccessCount = 0
					gateway.trackFailureCount = 0
				}
				gateway.requestMetricLock.Unlock()
			}
		}
	}
}

func (gateway *HandleT) setWebPayload(r *http.Request, qp url.Values, reqType string) error {
	// add default fields to body
	body := []byte(`{"channel": "web","integrations": {"All": true}}`)
	currentTime := time.Now()
	body, _ = sjson.SetBytes(body, "originalTimestamp", currentTime)
	body, _ = sjson.SetBytes(body, "sentAt", currentTime)

	// make sure anonymousId is in correct format
	if anonymousID, ok := qp["anonymousId"]; ok {
		qp["anonymousId"][0] = regexp.MustCompile(`^"(.*)"$`).ReplaceAllString(anonymousID[0], `$1`)
	}

	// add queryParams to body
	for key := range qp {
		body, _ = sjson.SetBytes(body, key, qp[key][0])
	}

	// add request specific fields to body
	body, _ = sjson.SetBytes(body, "type", reqType)
	switch reqType {
	case "page":
		if pageName, ok := qp["name"]; ok {
			if pageName[0] == "" {
				pageName[0] = "Unknown Page"
			}
			body, _ = sjson.SetBytes(body, "name", pageName[0])
		}
	case "track":
		if evName, ok := qp["event"]; ok {
			if evName[0] == "" {
				return errors.New("track: Mandatory field 'event' missing")
			}
			body, _ = sjson.SetBytes(body, "event", evName[0])
		}
	}
	// add body to request
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

func sendPixelResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "image/gif")
	w.Write([]byte(response.GetPixelResponse()))
}

func (gateway *HandleT) pixelHandler(w http.ResponseWriter, r *http.Request, reqType string) {

	queryParams := r.URL.Query()
	if queryParams["writeKey"] != nil {
		writeKey := queryParams["writeKey"]
		// make a new request
		req, err := http.NewRequest(http.MethodPost, "", nil)
		if err != nil {
			sendPixelResponse(w)
			return
		}
		// set basic auth header
		req.SetBasicAuth(writeKey[0], "")
		delete(queryParams, "writeKey")

		// set X-Forwarded-For header
		req.Header.Add("X-Forwarded-For", r.Header.Get("X-Forwarded-For"))

		// convert the pixel request(r) to a web request(req)
		err = gateway.setWebPayload(req, queryParams, reqType)
		if err == nil {
			gateway.pixelWebHandler(w, req, reqType)
		} else {
			sendPixelResponse(w)
		}
	} else {
		gateway.logger.Info(fmt.Sprintf("IP: %s -- %s -- Error while handling request: Write Key not found", misc.GetIPFromReq(r), r.URL.Path))
		sendPixelResponse(w)
	}
}

func (gateway *HandleT) beaconHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	queryParams := r.URL.Query()
	if writeKey, present := queryParams["writeKey"]; present && writeKey[0] != "" {

		// set basic auth header
		r.SetBasicAuth(writeKey[0], "")
		delete(queryParams, "writeKey")

		// send req to webHandler
		gateway.webHandler(w, r, reqType)
	} else {
		gateway.logger.Infof("IP: %s -- %s -- Error while handling beacon request: Write Key not found", misc.GetIPFromReq(r), r.URL.Path)
		http.Error(w, response.NoWriteKeyInQueryParams, http.StatusUnauthorized)
	}
}

func (gateway *HandleT) healthHandler(w http.ResponseWriter, r *http.Request) {
	app.HealthHandler(w, r, gateway.jobsDB)
}

func reflectOrigin(origin string) bool {
	return true
}

/*
StartWebHandler starts all gateway web handlers, listening on gateway port.
Supports CORS from all origins.
This function will block.
*/
func (gateway *HandleT) StartWebHandler(ctx context.Context) error {
	if err := gateway.backendConfig.WaitForConfig(ctx); err != nil {
		return err
	}

	gateway.logger.Infof("Starting in %d", webPort)
	srvMux := mux.NewRouter()
	srvMux.Use(
		middleware.StatMiddleware(ctx),
		middleware.LimitConcurrentRequests(maxConcurrentRequests),
		middleware.ContentType(),
	)
	srvMux.HandleFunc("/v1/batch", gateway.webBatchHandler).Methods("POST")
	srvMux.HandleFunc("/v1/identify", gateway.webIdentifyHandler).Methods("POST")
	srvMux.HandleFunc("/v1/track", gateway.webTrackHandler).Methods("POST")
	srvMux.HandleFunc("/v1/page", gateway.webPageHandler).Methods("POST")
	srvMux.HandleFunc("/v1/screen", gateway.webScreenHandler).Methods("POST")
	srvMux.HandleFunc("/v1/alias", gateway.webAliasHandler).Methods("POST")
	srvMux.HandleFunc("/v1/merge", gateway.webMergeHandler).Methods("POST")
	srvMux.HandleFunc("/v1/group", gateway.webGroupHandler).Methods("POST")
	srvMux.HandleFunc("/health", gateway.healthHandler).Methods("GET")
	srvMux.HandleFunc("/v1/import", gateway.webImportHandler).Methods("POST")
	srvMux.HandleFunc("/v1/audiencelist", gateway.webAudienceListHandler).Methods("POST")
	srvMux.HandleFunc("/", gateway.healthHandler).Methods("GET")
	srvMux.HandleFunc("/pixel/v1/track", gateway.pixelTrackHandler).Methods("GET")
	srvMux.HandleFunc("/pixel/v1/page", gateway.pixelPageHandler).Methods("GET")
	srvMux.HandleFunc("/version", gateway.versionHandler).Methods("GET")
	srvMux.HandleFunc("/v1/webhook", gateway.webhookHandler.RequestHandler).Methods("POST", "GET")
	srvMux.HandleFunc("/beacon/v1/batch", gateway.beaconBatchHandler).Methods("POST")

	if enableEventSchemasFeature {
		srvMux.HandleFunc("/schemas/event-models", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventModels)).Methods("GET")
		srvMux.HandleFunc("/schemas/event-versions", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventVersions)).Methods("GET")
		srvMux.HandleFunc("/schemas/event-model/{EventID}/key-counts", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetKeyCounts)).Methods("GET")
		srvMux.HandleFunc("/schemas/event-model/{EventID}/metadata", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventModelMetadata)).Methods("GET")
		srvMux.HandleFunc("/schemas/event-version/{VersionID}/metadata", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetSchemaVersionMetadata)).Methods("GET")
		srvMux.HandleFunc("/schemas/event-version/{VersionID}/missing-keys", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetSchemaVersionMissingKeys)).Methods("GET")
		srvMux.HandleFunc("/schemas/event-models/json-schemas", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetJsonSchemas)).Methods("GET")
	}

	//todo: remove in next release
	srvMux.HandleFunc("/v1/pending-events", gateway.pendingEventsHandler).Methods("POST")
	srvMux.HandleFunc("/v1/failed-events", gateway.fetchFailedEventsHandler).Methods("POST")
	srvMux.HandleFunc("/v1/clear-failed-events", gateway.clearFailedEventsHandler).Methods("POST")

	c := cors.New(cors.Options{
		AllowOriginFunc:  reflectOrigin,
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
		MaxAge:           900, // 15 mins
	})
	if diagnostics.EnableServerStartedMetric {
		Diagnostics.Track(diagnostics.ServerStarted, map[string]interface{}{
			diagnostics.ServerStarted: time.Now(),
		})
	}
	gateway.httpWebServer = &http.Server{
		Addr:              ":" + strconv.Itoa(webPort),
		Handler:           c.Handler(bugsnag.Handler(srvMux)),
		ReadTimeout:       ReadTimeout,
		ReadHeaderTimeout: ReadHeaderTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		<-ctx.Done()
		return gateway.httpWebServer.Shutdown(context.Background())
	})
	g.Go(func() error {
		return gateway.httpWebServer.ListenAndServe()
	})

	return g.Wait()
}

//AdminHandler for Admin Operations
func (gateway *HandleT) StartAdminHandler(ctx context.Context) error {

	if err := gateway.backendConfig.WaitForConfig(ctx); err != nil {
		return err
	}

	gateway.logger.Infof("Starting AdminHandler in %d", adminWebPort)
	srvMux := mux.NewRouter()
	srvMux.Use(
		middleware.StatMiddleware(ctx),
		middleware.LimitConcurrentRequests(maxConcurrentRequests),
		middleware.ContentType(),
	)
	srvMux.HandleFunc("/v1/pending-events", gateway.pendingEventsHandler).Methods("POST")

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(adminWebPort),
		Handler: bugsnag.Handler(srvMux),
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(context.Background())
	})
	g.Go(func() error {
		return srv.ListenAndServe()
	})

	return g.Wait()
}

// Gets the config from config backend and extracts enabled writekeys
func (gateway *HandleT) backendConfigSubscriber() {
	ch := make(chan pubsub.DataEvent)
	gateway.backendConfig.Subscribe(ch, backendconfig.TopicProcessConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		enabledWriteKeysSourceMap = map[string]backendconfig.SourceT{}
		enabledWriteKeyWebhookMap = map[string]string{}
		enabledWriteKeyWorkspaceMap = map[string]string{}
		sources := config.Data.(backendconfig.ConfigT)
		sourceIDToNameMap = map[string]string{}
		for _, source := range sources.Sources {
			sourceIDToNameMap[source.ID] = source.Name
			if source.Enabled {
				enabledWriteKeysSourceMap[source.WriteKey] = source
				enabledWriteKeyWorkspaceMap[source.WriteKey] = source.WorkspaceID
				if source.SourceDefinition.Category == "webhook" {
					enabledWriteKeyWebhookMap[source.WriteKey] = source.SourceDefinition.Name
					gateway.webhookHandler.Register(source.SourceDefinition.Name)
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

/*
Public methods on GatewayWebhookI
*/

/*
Finds the worker for a particular userID and queues the webrequest with the worker(pushes the req into the webRequestQ channel of the worker).

They are further batched together in userWebRequestBatcher
*/
func (gateway *HandleT) addToWebRequestQ(writer *http.ResponseWriter, req *http.Request, done chan string, reqType string, requestPayload []byte, writeKey string) {

	userIDHeader := req.Header.Get("AnonymousId")
	//If necessary fetch userID from request body.
	if userIDHeader == "" {
		//If the request comes through proxy, proxy would already send this. So this shouldn't be happening in that case
		userIDHeader = uuid.Must(uuid.NewV4()).String()
	}
	userWebRequestWorker := gateway.findUserWebRequestWorker(userIDHeader)
	ipAddr := misc.GetIPFromReq(req)
	webReq := webRequestT{done: done, reqType: reqType, requestPayload: requestPayload, writeKey: writeKey, ipAddr: ipAddr}
	userWebRequestWorker.webRequestQ <- &webReq
}

// IncrementRecvCount increments the received count for gateway requests
func (gateway *HandleT) IncrementRecvCount(count uint64) {
	atomic.AddUint64(&gateway.recvCount, count)
}

// IncrementAckCount increments the acknowledged count for gateway requests
func (gateway *HandleT) IncrementAckCount(count uint64) {
	atomic.AddUint64(&gateway.ackCount, count)
}

// UpdateSourceStats creates a new stat for every writekey and updates it with the corresponding count
func (gateway *HandleT) UpdateSourceStats(sourceStats map[string]int, bucket string, sourceTagMap map[string]string) {
	gateway.updateSourceStats(sourceStats, bucket, sourceTagMap)
}

// TrackRequestMetrics provides access to add request success/failure telemetry
func (gateway *HandleT) TrackRequestMetrics(errorMessage string) {
	gateway.trackRequestMetrics(errorMessage)
}

// GetWebhookSourceDefName returns the webhook source definition name by write key
func (gateway *HandleT) GetWebhookSourceDefName(writeKey string) (name string, ok bool) {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	name, ok = enabledWriteKeyWebhookMap[writeKey]
	return
}

func (gateway *HandleT) SetReadonlyDBs(readonlyGatewayDB, readonlyRouterDB, readonlyBatchRouterDB jobsdb.ReadonlyJobsDB) {
	gateway.readonlyGatewayDB = readonlyGatewayDB
	gateway.readonlyRouterDB = readonlyRouterDB
	gateway.readonlyBatchRouterDB = readonlyBatchRouterDB
}

/*
Setup initializes this module:
- Monitors backend config for changes.
- Starts web request batching goroutine, that batches incoming messages.
- Starts web request batch db writer goroutine, that writes incoming batches to JobsDB.
- Starts debugging goroutine that prints gateway stats.

This function will block until backend config is initialy received.
*/
func (gateway *HandleT) Setup(application app.Interface, backendConfig backendconfig.BackendConfig, jobsDB jobsdb.JobsDB, rateLimiter ratelimiter.RateLimiter, versionHandler func(w http.ResponseWriter, r *http.Request)) {
	gateway.logger = pkgLogger
	gateway.application = application
	gateway.stats = stats.DefaultStats

	gateway.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	config.RegisterDurationConfigVariable(30, &gateway.httpTimeout, false, time.Second, "Gateway.httpTimeout")
	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: gateway.httpTimeout}
	gateway.netHandle = client

	//For the lack of better stat type, using TimerType.
	gateway.batchSizeStat = gateway.stats.NewStat("gateway.batch_size", stats.HistogramType)
	gateway.requestSizeStat = gateway.stats.NewStat("gateway.request_size", stats.HistogramType)
	gateway.dbWritesStat = gateway.stats.NewStat("gateway.db_writes", stats.CountType)
	gateway.dbWorkersBufferFullStat = gateway.stats.NewStat("gateway.db_workers_buffer_full", stats.CountType)
	gateway.dbWorkersTimeOutStat = gateway.stats.NewStat("gateway.db_workers_time_out", stats.CountType)
	gateway.bodyReadTimeStat = gateway.stats.NewStat("gateway.http_body_read_time", stats.TimerType)
	gateway.addToWebRequestQWaitTime = gateway.stats.NewStat("gateway.web_request_queue_wait_time", stats.TimerType)
	gateway.addToBatchRequestQWaitTime = gateway.stats.NewStat("gateway.batch_request_queue_wait_time", stats.TimerType)
	gateway.ProcessRequestTime = gateway.stats.NewStat("gateway.process_request_time", stats.TimerType)

	gateway.backendConfig = backendConfig
	gateway.rateLimiter = rateLimiter
	gateway.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, maxDBBatchSize)
	gateway.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, maxDBWriterProcess)
	gateway.jobsDB = jobsDB

	gateway.versionHandler = versionHandler

	gateway.irh = &ImportRequestHandler{}
	gateway.rrh = &RegularRequestHandler{}

	gateway.webhookHandler = webhook.Setup(gateway)
	gatewayAdmin := GatewayAdmin{handle: gateway}
	gatewayRPCHandler := GatewayRPCHandler{jobsDB: gateway.jobsDB, readOnlyJobsDB: gateway.readonlyGatewayDB}

	admin.RegisterStatusHandler("Gateway", &gatewayAdmin)
	admin.RegisterAdminHandler("Gateway", &gatewayRPCHandler)

	if enableSuppressUserFeature && gateway.application.Features().SuppressUser != nil {
		rruntime.Go(func() {
			gateway.suppressUserHandler = application.Features().SuppressUser.Setup(gateway.backendConfig)
		})
	}

	if enableEventSchemasFeature {
		gateway.eventSchemaHandler = event_schema.GetInstance()
	}

	rruntime.Go(func() {
		gateway.backendConfigSubscriber()
	})

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	if err := gateway.backendConfig.WaitForConfig(ctx); err != nil {
		cancel()
		return
	}

	gateway.initUserWebRequestWorkers()

	gateway.backgroundCancel = cancel
	gateway.backgroundWait = g.Wait

	g.Go(misc.WithBugsnag(func() error {
		gateway.runUserWebRequestWorkers(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.userWorkerRequestBatcher()
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.initDBWriterWorkers(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.printStats(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.collectMetrics(ctx)
		return nil
	}))
}

func (gateway *HandleT) Shutdown() {
	gateway.backgroundCancel()
	gateway.webhookHandler.Shutdown()

	// UserWebRequestWorkers
	for _, worker := range gateway.userWebRequestWorkers {
		close(worker.webRequestQ)
	}

	gateway.backgroundWait()
}

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
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/middleware"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/rruntime"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
	"github.com/rudderlabs/rudder-server/services/stats"
	rs_httputil "github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
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
	userIDHeader   string
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

var (
	webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess, adminWebPort         int
	maxUserWebRequestBatchSize, maxDBBatchSize, maxHeaderBytes, maxConcurrentRequests int
	userWebRequestBatchTimeout, dbBatchWriteTimeout                                   time.Duration
	writeKeysSourceMap                                                                map[string]backendconfig.SourceT
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
	pkgLogger                                                                         logger.Logger
	Diagnostics                                                                       diagnostics.DiagnosticsI
	semverRegexp                                                                      *regexp.Regexp = regexp.MustCompile(`^v?([0-9]+)(\.[0-9]+)?(\.[0-9]+)?(-([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?(\+([0-9A-Za-z\-]+(\.[0-9A-Za-z\-]+)*))?$`)
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
	DELIMITER                 = string("<<>>")
	eventStreamSourceCategory = "eventStream"
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

type sourceDebugger struct {
	data     []byte
	writeKey string
}

// Basic worker unit that works on incoming webRequests.
//
// Has three channels used to communicate between the two goroutines each worker runs.
//
// One to receive new webRequests, one to send batches of said webRequests and the third to receive errors if any in response to sending the said batches to dbWriterWorker.
type userWebRequestWorkerT struct {
	webRequestQ                 chan *webRequestT
	batchRequestQ               chan *batchWebRequestT
	reponseQ                    chan map[uuid.UUID]string
	workerID                    int
	batchTimeStat               stats.Measurement
	bufferFullStat, timeOutStat stats.Measurement
}

// HandleT is the struct returned by the Setup call
type HandleT struct {
	application                  app.App
	userWorkerBatchRequestQ      chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ chan *batchUserWorkerBatchRequestT
	jobsDB                       jobsdb.JobsDB
	ackCount                     uint64
	recvCount                    uint64
	backendConfig                backendconfig.BackendConfig
	rateLimiter                  ratelimiter.RateLimiter

	stats                                         stats.Stats
	batchSizeStat                                 stats.Measurement
	requestSizeStat                               stats.Measurement
	dbWritesStat                                  stats.Measurement
	dbWorkersBufferFullStat, dbWorkersTimeOutStat stats.Measurement
	bodyReadTimeStat                              stats.Measurement
	addToWebRequestQWaitTime                      stats.Measurement
	processRequestTime                            stats.Measurement
	emptyAnonIdHeaderStat                         stats.Measurement
	addToBatchRequestQWaitTime                    stats.Measurement

	diagnosisTicker   *time.Ticker
	requestMetricLock sync.Mutex
	trackSuccessCount int
	trackFailureCount int

	userWebRequestWorkers []*userWebRequestWorkerT
	webhookHandler        *webhook.HandleT
	suppressUserHandler   types.UserSuppression
	eventSchemaHandler    types.EventSchemasI
	versionHandler        func(w http.ResponseWriter, r *http.Request)
	logger                logger.Logger
	rrh                   *RegularRequestHandler
	irh                   *ImportRequestHandler
	readonlyGatewayDB     jobsdb.ReadonlyJobsDB
	netHandle             *http.Client
	httpTimeout           time.Duration
	httpWebServer         *http.Server
	backgroundCancel      context.CancelFunc
	backgroundWait        func() error
	rsourcesService       rsources.JobService
	sourcehandle          sourcedebugger.SourceDebugger
	whProxy               http.Handler
}

// Part of the gateway module Setup call.
//
//	Initiates `maxUserWebRequestWorkerProcess` number of `webRequestWorkers` that listen on their `webRequestQ` for new WebRequests.
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
//  1. `userWebRequestBatcher` batches the webRequests that a worker gets
//  2. `userWebRequestWorkerProcess` processes the requests in the batches and sends them as part of a `jobsList` to `dbWriterWorker`s.
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
	_ = g.Wait()

	close(gateway.userWorkerBatchRequestQ)
}

// Initiates `maxDBWriterProcess` number of dbWriterWorkers
func (gateway *HandleT) initDBWriterWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < maxDBWriterProcess; i++ {
		gateway.logger.Debug("DB Writer Worker Started", i)
		g.Go(misc.WithBugsnag(func() error {
			gateway.dbWriterWorkerProcess()
			return nil
		}))
	}
	_ = g.Wait()
}

//	Batches together jobLists received on the `userWorkerBatchRequestQ` channel of the gateway
//	and queues the batch at the `batchUserWorkerBatchRequestQ` channel of the gateway.
//
// Initiated during the gateway Setup and keeps batching jobLists received from webRequestWorkers
func (gateway *HandleT) userWorkerRequestBatcher() {
	userWorkerBatchRequestBuffer := make([]*userWorkerBatchRequestT, 0)

	timeout := time.After(dbBatchWriteTimeout)
	for {
		select {
		case userWorkerBatchRequest, hasMore := <-gateway.userWorkerBatchRequestQ:
			if !hasMore {
				if len(userWorkerBatchRequestBuffer) > 0 {
					gateway.batchUserWorkerBatchRequestQ <- &batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				}
				close(gateway.batchUserWorkerBatchRequestQ)
				return
			}
			// Append to request buffer
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

// goes over the batches of jobs-list, and stores each job in every jobList into gw_db
// sends a map of errors if any(errors mapped to the job.uuid) over the responseQ channel of the webRequestWorker.
// userWebRequestWorkerProcess method of the webRequestWorker is waiting for this errorMessageMap.
// This in turn sends the error over the done channel of each respective webRequest.
func (gateway *HandleT) dbWriterWorkerProcess() {
	for breq := range gateway.batchUserWorkerBatchRequestQ {
		var (
			jobList          = make([]*jobsdb.JobT, 0)
			errorMessagesMap = map[uuid.UUID]string{}
		)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			jobList = append(jobList, userWorkerBatchRequest.jobList...)
		}

		ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
		err := gateway.jobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			if gwAllowPartialWriteWithErrors {
				var err error
				errorMessagesMap, err = gateway.jobsDB.StoreWithRetryEachInTx(ctx, tx, jobList)
				if err != nil {
					return err
				}
			} else {
				err := gateway.jobsDB.StoreInTx(ctx, tx, jobList)
				if err != nil {
					gateway.logger.Errorf("Store into gateway db failed with error: %v", err)
					gateway.logger.Errorf("JobList: %+v", jobList)
					return err
				}
			}

			// rsources stats
			rsourcesStats := rsources.NewStatsCollector(gateway.rsourcesService)
			rsourcesStats.JobsStoredWithErrors(jobList, errorMessagesMap)
			return rsourcesStats.Publish(ctx, tx.SqlTx())
		})
		if err != nil {
			errorMessage := err.Error()
			if ctx.Err() != nil {
				errorMessage = ctx.Err().Error()
			}
			for _, job := range jobList {
				errorMessagesMap[job.UUID] = errorMessage
			}
		}
		cancel()
		gateway.dbWritesStat.Count(1)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			userWorkerBatchRequest.respChannel <- errorMessagesMap
		}
	}
}

// Out of all the workers, this finds and returns the worker that works on a particular `userID`.
//
// This is done so that requests with a userID keep going to the same worker, which would maintain the consistency in event ordering.
func (gateway *HandleT) findUserWebRequestWorker(userID string) *userWebRequestWorkerT {
	index := int(math.Abs(float64(misc.GetHash(userID) % maxUserWebRequestWorkerProcess)))

	userWebRequestWorker := gateway.userWebRequestWorkers[index]
	if userWebRequestWorker == nil {
		panic(fmt.Errorf("worker is nil"))
	}

	return userWebRequestWorker
}

//	This function listens on the `webRequestQ` channel of a worker.
//	Based on `userWebRequestBatchTimeout` and `maxUserWebRequestBatchSize` parameters,
//	batches them together and queues the batch of webreqs in the `batchRequestQ` channel of the worker
//
// Every webRequestWorker keeps doing this concurrently.
func (gateway *HandleT) userWebRequestBatcher(userWebRequestWorker *userWebRequestWorkerT) {
	reqBuffer := make([]*webRequestT, 0)
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

			// Append to request buffer
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

func (gateway *HandleT) NewSourceStat(writeKey, reqType string) *gwstats.SourceStat {
	return &gwstats.SourceStat{
		Source:      gateway.getSourceTagFromWriteKey(writeKey),
		SourceID:    gateway.getSourceIDForWriteKey(writeKey),
		WriteKey:    writeKey,
		ReqType:     reqType,
		WorkspaceID: gateway.getWorkspaceForWriteKey(writeKey),
		SourceType:  gateway.getSourceCategoryForWriteKey(writeKey),
	}
}

//	Listens on the `batchRequestQ` channel of the webRequestWorker for new batches of webRequests
//	Goes over the webRequests in the batch and filters them out(`rateLimit`, `maxReqSize`).
//	And creates a `jobList` which is then sent to `userWorkerBatchRequestQ` of the gateway and waits for a response
//	from the `dbwriterWorker`s that batch them and write to the db.
//
// Finally sends responses(error) if any back to the webRequests over their `done` channels
func (gateway *HandleT) userWebRequestWorkerProcess(userWebRequestWorker *userWebRequestWorkerT) {
	for breq := range userWebRequestWorker.batchRequestQ {
		var jobList []*jobsdb.JobT
		jobIDReqMap := make(map[uuid.UUID]*webRequestT)
		jobSourceTagMap := make(map[uuid.UUID]string)
		sourceStats := make(map[string]*gwstats.SourceStat)
		// Saving the event data read from req.request.Body to the splice.
		// Using this to send event schema to the config backend.
		var eventBatchesToRecord []sourceDebugger
		batchStart := time.Now()
		for _, req := range breq.batchRequest {
			writeKey := req.writeKey
			sourceTag := gateway.getSourceTagFromWriteKey(writeKey)
			if _, ok := sourceStats[sourceTag]; !ok {
				sourceStats[sourceTag] = gateway.NewSourceStat(writeKey, req.reqType)
			}
			jobData, err := gateway.getJobDataFromRequest(req)
			sourceStats[sourceTag].Version = jobData.version
			if err != nil {
				switch {
				case err == errRequestDropped:
					req.done <- response.TooManyRequests
					sourceStats[sourceTag].RequestDropped()
				case err == errRequestSuppressed:
					req.done <- "" // no error
					sourceStats[sourceTag].RequestSuppressed()
				default:
					req.done <- err.Error()
					sourceStats[sourceTag].RequestEventsFailed(jobData.numEvents, err.Error())
				}
				continue
			}
			jobList = append(jobList, jobData.job)
			jobIDReqMap[jobData.job.UUID] = req
			jobSourceTagMap[jobData.job.UUID] = sourceTag
			eventBatchesToRecord = append(eventBatchesToRecord, sourceDebugger{data: jobData.job.EventPayload, writeKey: writeKey})
		}

		errorMessagesMap := make(map[uuid.UUID]string)
		if len(jobList) > 0 {
			gateway.userWorkerBatchRequestQ <- &userWorkerBatchRequestT{
				jobList:     jobList,
				respChannel: userWebRequestWorker.reponseQ,
			}
			errorMessagesMap = <-userWebRequestWorker.reponseQ
		}

		for _, job := range jobList {
			err, found := errorMessagesMap[job.UUID]
			sourceTag := jobSourceTagMap[job.UUID]
			if found {
				sourceStats[sourceTag].RequestEventsFailed(job.EventCount, "storeFailed")
			} else {
				sourceStats[sourceTag].RequestEventsSucceeded(job.EventCount)
			}
			jobIDReqMap[job.UUID].done <- err
		}
		// Sending events to config backend
		for _, eventBatch := range eventBatchesToRecord {
			gateway.sourcehandle.RecordEvent(eventBatch.writeKey, eventBatch.data)
		}

		userWebRequestWorker.batchTimeStat.Since(batchStart)
		gateway.batchSizeStat.Observe(float64(len(breq.batchRequest)))

		for _, v := range sourceStats {
			v.Report(gateway.stats)
		}
	}
}

var (
	errRequestDropped    = errors.New("request dropped")
	errRequestSuppressed = errors.New("request suppressed")
)

type jobFromReq struct {
	job       *jobsdb.JobT
	numEvents int
	version   string
}

func (gateway *HandleT) getJobDataFromRequest(req *webRequestT) (jobData *jobFromReq, err error) {
	var (
		writeKey = req.writeKey
		sourceID = gateway.getSourceIDForWriteKey(writeKey)
		// Should be function of body
		workspaceId  = gateway.getWorkspaceForWriteKey(writeKey)
		userIDHeader = req.userIDHeader
		ipAddr       = req.ipAddr
		body         = req.requestPayload
	)

	jobData = &jobFromReq{}
	if !gjson.ValidBytes(body) {
		err = errors.New(response.InvalidJSON)
		return
	}

	gateway.requestSizeStat.Observe(float64(len(body)))
	if req.reqType != "batch" {
		body, err = sjson.SetBytes(body, "type", req.reqType)
		if err != nil {
			err = errors.New(response.NotRudderEvent)
			return
		}
		body, _ = sjson.SetRawBytes(BatchEvent, "batch.0", body)
	}

	eventsBatch := gjson.GetBytes(body, "batch").Array()
	jobData.numEvents = len(eventsBatch)

	if !gateway.isValidWriteKey(writeKey) {
		err = errors.New(response.InvalidWriteKey)
		return
	}

	if !gateway.isWriteKeyEnabled(writeKey) {
		err = errors.New(response.SourceDisabled)
		return
	}

	if enableRateLimit {
		// In case of "batch" requests, if rate-limiter returns true for LimitReached, just drop the event batch and continue.
		if gateway.rateLimiter.LimitReached(workspaceId) {
			err = errRequestDropped
			return
		}
	}

	var (
		// map to hold modified/filtered events of the batch
		out []map[string]interface{}

		// values retrieved from first event in batch
		firstUserID                                 string
		firstSourcesJobRunID, firstSourcesTaskRunID string

		// facts about the batch populated as we iterate over events
		containsAudienceList, suppressed bool
	)

	isUserSuppressed := gateway.memoizedIsUserSuppressed()
	for idx, v := range eventsBatch {
		toSet, ok := v.Value().(map[string]interface{})
		if !ok {
			err = errors.New(response.NotRudderEvent)
			return
		}

		anonIDFromReq, _ := toSet["anonymousId"].(string)
		userIDFromReq, _ := toSet["userId"].(string)
		if isNonIdentifiable(anonIDFromReq, userIDFromReq) {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}

		if idx == 0 {
			firstUserID = buildUserID(userIDHeader, anonIDFromReq, userIDFromReq)
			firstSourcesJobRunID, _ = misc.MapLookup(
				toSet,
				"context",
				"sources",
				"job_run_id",
			).(string)
			firstSourcesTaskRunID, _ = misc.MapLookup(
				toSet,
				"context",
				"sources",
				"task_run_id",
			).(string)

			// calculate version
			firstSDKName, _ := misc.MapLookup(
				toSet,
				"context",
				"library",
				"name",
			).(string)
			firstSDKVersion, _ := misc.MapLookup(
				toSet,
				"context",
				"library",
				"version",
			).(string)
			if firstSDKVersion != "" && !semverRegexp.Match([]byte(firstSDKVersion)) {
				firstSDKVersion = "invalid"
			}
			if firstSDKName != "" || firstSDKVersion != "" {
				jobData.version = firstSDKName + "/" + firstSDKVersion
			}
		}

		if isUserSuppressed(workspaceId, userIDFromReq, sourceID) {
			suppressed = true
			continue
		}

		// hashing combination of userIDFromReq + anonIDFromReq, using colon as a delimiter
		var rudderId uuid.UUID
		rudderId, err = misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}
		toSet["rudderId"] = rudderId
		setRandomMessageIDWhenEmpty(toSet)
		if eventTypeFromReq, _ := misc.MapLookup(
			toSet,
			"type",
		).(string); eventTypeFromReq == "audiencelist" {
			containsAudienceList = true
		}

		out = append(out, toSet)
	}

	if len(out) == 0 && suppressed {
		err = errRequestSuppressed
		return
	}

	if len(body) > maxReqSize && !containsAudienceList {
		err = errors.New(response.RequestBodyTooLarge)
		return
	}

	body, _ = sjson.SetBytes(body, "batch", out)
	body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
	body, _ = sjson.SetBytes(body, "writeKey", writeKey)
	body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(misc.RFC3339Milli))

	id := uuid.New()

	params := map[string]interface{}{
		"source_id":          sourceID,
		"source_job_run_id":  firstSourcesJobRunID,
		"source_task_run_id": firstSourcesTaskRunID,
	}
	marshalledParams, err := json.Marshal(params)
	if err != nil {
		gateway.logger.Errorf(
			"[Gateway] Failed to marshal parameters map. Parameters: %+v",
			params,
		)
		marshalledParams = []byte(
			`{"error": "rudder-server gateway failed to marshal params"}`,
		)
	}
	err = nil
	job := &jobsdb.JobT{
		UUID:         id,
		UserID:       firstUserID,
		Parameters:   marshalledParams,
		CustomVal:    CustomVal,
		EventPayload: body,
		EventCount:   jobData.numEvents,
		WorkspaceId:  workspaceId,
	}
	jobData.job = job
	return
}

func isNonIdentifiable(anonIDFromReq, userIDFromReq string) bool {
	if anonIDFromReq == "" && userIDFromReq == "" {
		return !allowReqsWithoutUserIDAndAnonymousID
	}
	return false
}

func buildUserID(userIDHeader, anonIDFromReq, userIDFromReq string) string {
	if anonIDFromReq != "" {
		return userIDHeader + DELIMITER + anonIDFromReq + DELIMITER + userIDFromReq
	}
	return userIDHeader + DELIMITER + userIDFromReq + DELIMITER + userIDFromReq
}

// memoizedIsUserSuppressed is a memoized version of isUserSuppressed
func (gateway *HandleT) memoizedIsUserSuppressed() func(workspaceID, userID, sourceID string) bool {
	cache := map[string]bool{}
	return func(workspaceID, userID, sourceID string) bool {
		key := workspaceID + ":" + userID + ":" + sourceID
		if val, ok := cache[key]; ok {
			return val
		}
		val := gateway.isUserSuppressed(workspaceID, userID, sourceID)
		cache[key] = val
		return val
	}
}

func (gateway *HandleT) isUserSuppressed(workspaceID, userID, sourceID string) bool {
	if !enableSuppressUserFeature || gateway.suppressUserHandler == nil {
		return false
	}
	return gateway.suppressUserHandler.IsSuppressedUser(
		workspaceID,
		userID,
		sourceID,
	)
}

// checks for the presence of messageId in the event
//
// sets to a new uuid if not present
func setRandomMessageIDWhenEmpty(event map[string]interface{}) {
	messageID, _ := event["messageId"].(string)
	if strings.TrimSpace(messageID) == "" {
		event["messageId"] = uuid.New().String()
	}
}

func (*HandleT) getSourceCategoryForWriteKey(writeKey string) (category string) {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := writeKeysSourceMap[writeKey]; ok {
		category = writeKeysSourceMap[writeKey].SourceDefinition.Category
		if category == "" {
			category = eventStreamSourceCategory
		}
	}
	return
}

func (*HandleT) getWorkspaceForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := enabledWriteKeyWorkspaceMap[writeKey]; ok {
		return enabledWriteKeyWorkspaceMap[writeKey]
	}
	return ""
}

func (*HandleT) isValidWriteKey(writeKey string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	_, ok := writeKeysSourceMap[writeKey]
	return ok
}

func (*HandleT) isWriteKeyEnabled(writeKey string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	return writeKeysSourceMap[writeKey].Enabled
}

func (*HandleT) getSourceIDForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := writeKeysSourceMap[writeKey]; ok {
		return writeKeysSourceMap[writeKey].ID
	}

	return ""
}

func (*HandleT) getSourceNameForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := writeKeysSourceMap[writeKey]; ok {
		return writeKeysSourceMap[writeKey].Name
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
	_ = r.Body.Close()
	if err != nil {
		gateway.logger.Errorf(
			"Error reading request body, 'Content-Length': %s, partial payload:\n\t%s\n",
			r.Header.Get("Content-Length"),
			string(payload),
		)
		return payload, errors.New(response.RequestBodyReadFailed)
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

func warehouseHandler(w http.ResponseWriter, r *http.Request) {
	origin, err := url.Parse(misc.GetWarehouseURL())
	if err != nil {
		http.Error(w, err.Error(), 404)
	}
	// gateway.logger.LogRequest(r)
	director := func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = origin.Host
	}
	proxy := &httputil.ReverseProxy{Director: director}
	proxy.ServeHTTP(w, r)
}

// ProcessRequest throws a webRequest into the queue and waits for the response before returning
func (*RegularRequestHandler) ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	done := make(chan string, 1)
	start := time.Now()
	gateway.addToWebRequestQ(w, r, done, reqType, payload, writeKey)
	gateway.addToWebRequestQWaitTime.SendTiming(time.Since(start))
	defer gateway.processRequestTime.Since(start)
	errorMessage := <-done
	return errorMessage
}

// RequestHandler interface for abstracting out server-side import request processing and rest of the calls
type RequestHandler interface {
	ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string
}

// ImportRequestHandler is an empty struct to capture import specific request handling functionality
type ImportRequestHandler struct{}

// RegularRequestHandler is an empty struct to capture non-import specific request handling functionality
type RegularRequestHandler struct{}

// ProcessWebRequest is an Interface wrapper for webhook
func (gateway *HandleT) ProcessWebRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	return gateway.rrh.ProcessRequest(gateway, w, r, reqType, payload, writeKey)
}

func (gateway *HandleT) getPayloadAndWriteKey(_ http.ResponseWriter, r *http.Request, reqType string) ([]byte, string, error) {
	var err error
	writeKey, _, ok := r.BasicAuth()
	sourceID := gateway.getSourceIDForWriteKey(writeKey)
	if !ok || writeKey == "" {
		err = errors.New(response.NoWriteKeyInBasicAuth)

		stat := gwstats.SourceStat{
			Source:   "noWriteKey",
			SourceID: sourceID,
			WriteKey: "noWriteKey",
			ReqType:  reqType,
		}
		stat.RequestFailed("noWriteKeyInBasicAuth")
		stat.Report(gateway.stats)

		return []byte{}, "", err
	}
	payload, err := gateway.getPayloadFromRequest(r)
	if err != nil {
		stat := gwstats.SourceStat{
			Source:      gateway.getSourceTagFromWriteKey(writeKey),
			WriteKey:    writeKey,
			ReqType:     reqType,
			SourceID:    sourceID,
			WorkspaceID: gateway.getWorkspaceForWriteKey(writeKey),
			SourceType:  gateway.getSourceCategoryForWriteKey(writeKey),
		}
		stat.RequestFailed("requestBodyReadFailed")
		stat.Report(gateway.stats)

		return []byte{}, writeKey, err
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
			gateway.logger.Infof("IP: %s -- %s -- Response: %d, %s", misc.GetIPFromReq(r), r.URL.Path, response.GetErrorStatusCode(errorMessage), errorMessage)
			http.Error(w, response.GetStatus(errorMessage), response.GetErrorStatusCode(errorMessage))
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
	_, _ = w.Write([]byte(response.GetStatus(response.Ok)))
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

// ProcessRequest on ImportRequestHandler splits payload by user and throws them into the webrequestQ and waits for all their responses before returning
func (*ImportRequestHandler) ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, _ string, payload []byte, writeKey string) string {
	usersPayload, payloadError := gateway.getUsersPayload(payload)
	if payloadError != nil {
		return payloadError.Error()
	}
	count := len(usersPayload)
	done := make(chan string, count)
	for key := range usersPayload {
		gateway.addToWebRequestQ(w, r, done, "batch", usersPayload[key], writeKey)
	}

	var interimMsgs []string
	for index := 0; index < count; index++ {
		interimErrorMessage := <-done
		interimMsgs = append(interimMsgs, interimErrorMessage)
	}
	return strings.Join(interimMsgs, "")
}

// for performance see: https://github.com/rudderlabs/rudder-server/pull/2040
func (*HandleT) getUsersPayload(requestPayload []byte) (map[string][]byte, error) {
	if !gjson.ValidBytes(requestPayload) {
		return make(map[string][]byte), errors.New(response.InvalidJSON)
	}

	result := gjson.GetBytes(requestPayload, "batch")

	var (
		userCnt = make(map[string]int)
		userMap = make(map[string][]byte)
	)
	result.ForEach(func(_, value gjson.Result) bool {
		anonIDFromReq := value.Get("anonymousId").String()
		userIDFromReq := value.Get("userId").String()
		rudderID, err := misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			return false
		}

		uuidStr := rudderID.String()
		tempValue, ok := userMap[uuidStr]
		if !ok {
			userCnt[uuidStr] = 0
			userMap[uuidStr] = append([]byte(`{"batch":[`), append([]byte(value.Raw), ']', '}')...)
		} else {
			path := "batch." + strconv.Itoa(userCnt[uuidStr]+1)
			raw, err := sjson.SetRaw(string(tempValue), path, value.Raw)
			if err != nil {
				return false
			}
			userCnt[uuidStr]++
			userMap[uuidStr] = []byte(raw)
		}

		return true
	})
	return userMap, nil
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

func (*HandleT) setWebPayload(r *http.Request, qp url.Values, reqType string) error {
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
	_, err := w.Write([]byte(response.GetPixelResponse()))
	if err != nil {
		pkgLogger.Warnf("Error while sending pixel response: %v", err)
		return
	}
}

func (gateway *HandleT) pixelHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	queryParams := r.URL.Query()
	if queryParams["writeKey"] != nil {
		writeKey := queryParams["writeKey"]
		// make a new request
		req, err := http.NewRequest(http.MethodPost, "", http.NoBody)
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

// Robots prevents robots from crawling the gateway endpoints
func (*HandleT) robots(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("User-agent: * \nDisallow: / \n"))
}

func reflectOrigin(_ string) bool {
	return true
}

/*
StartWebHandler starts all gateway web handlers, listening on gateway port.
Supports CORS from all origins.
This function will block.
*/
func (gateway *HandleT) StartWebHandler(ctx context.Context) error {
	gateway.logger.Infof("WebHandler waiting for BackendConfig before starting on %d", webPort)
	gateway.backendConfig.WaitForConfig(ctx)
	gateway.logger.Infof("WebHandler Starting on %d", webPort)
	component := "gateway"
	srvMux := mux.NewRouter()
	srvMux.Use(
		middleware.StatMiddleware(ctx, srvMux, stats.Default, component),
		middleware.LimitConcurrentRequests(maxConcurrentRequests),
		middleware.UncompressMiddleware,
	)
	srvMux.HandleFunc("/v1/batch", gateway.webBatchHandler).Methods("POST")
	srvMux.HandleFunc("/v1/identify", gateway.webIdentifyHandler).Methods("POST")
	srvMux.HandleFunc("/v1/track", gateway.webTrackHandler).Methods("POST")
	srvMux.HandleFunc("/v1/page", gateway.webPageHandler).Methods("POST")
	srvMux.HandleFunc("/v1/screen", gateway.webScreenHandler).Methods("POST")
	srvMux.HandleFunc("/v1/alias", gateway.webAliasHandler).Methods("POST")
	srvMux.HandleFunc("/v1/merge", gateway.webMergeHandler).Methods("POST")
	srvMux.HandleFunc("/v1/group", gateway.webGroupHandler).Methods("POST")
	srvMux.HandleFunc("/health", WithContentType("application/json; charset=utf-8", app.LivenessHandler(gateway.jobsDB))).Methods("GET")
	srvMux.HandleFunc("/", WithContentType("application/json; charset=utf-8", app.LivenessHandler(gateway.jobsDB))).Methods("GET")
	srvMux.HandleFunc("/v1/import", gateway.webImportHandler).Methods("POST")
	srvMux.HandleFunc("/v1/audiencelist", gateway.webAudienceListHandler).Methods("POST")
	srvMux.HandleFunc("/pixel/v1/track", gateway.pixelTrackHandler).Methods("GET")
	srvMux.HandleFunc("/pixel/v1/page", gateway.pixelPageHandler).Methods("GET")
	srvMux.HandleFunc("/v1/webhook", gateway.webhookHandler.RequestHandler).Methods("POST", "GET")
	srvMux.HandleFunc("/beacon/v1/batch", gateway.beaconBatchHandler).Methods("POST")
	srvMux.PathPrefix("/v1/warehouse").Handler(http.HandlerFunc(warehouseHandler)).Methods("GET", "POST")
	srvMux.HandleFunc("/version", WithContentType("application/json; charset=utf-8", gateway.versionHandler)).Methods("GET")
	srvMux.HandleFunc("/robots.txt", gateway.robots).Methods("GET")

	if enableEventSchemasFeature {
		srvMux.HandleFunc("/schemas/event-models", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventModels))).Methods("GET")
		srvMux.HandleFunc("/schemas/event-versions", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventVersions))).Methods("GET")
		srvMux.HandleFunc("/schemas/event-model/{EventID}/key-counts", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetKeyCounts))).Methods("GET")
		srvMux.HandleFunc("/schemas/event-model/{EventID}/metadata", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventModelMetadata))).Methods("GET")
		srvMux.HandleFunc("/schemas/event-version/{VersionID}/metadata", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetSchemaVersionMetadata))).Methods("GET")
		srvMux.HandleFunc("/schemas/event-version/{VersionID}/missing-keys", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetSchemaVersionMissingKeys))).Methods("GET")
		srvMux.HandleFunc("/schemas/event-models/json-schemas", WithContentType("application/json; charset=utf-8", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetJsonSchemas))).Methods("GET")
	}

	srvMux.HandleFunc("/v1/warehouse/pending-events", gateway.whProxy.ServeHTTP).Methods("POST")

	// rudder-sources new APIs
	rsourcesHandler := rsources_http.NewHandler(
		gateway.rsourcesService,
		gateway.logger.Child("rsources"))
	srvMux.PathPrefix("/v1/job-status").Handler(WithContentType("application/json; charset=utf-8", rsourcesHandler.ServeHTTP))

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
		MaxHeaderBytes:    maxHeaderBytes,
	}

	return rs_httputil.ListenAndServe(ctx, gateway.httpWebServer)
}

// StartAdminHandler for Admin Operations
func (gateway *HandleT) StartAdminHandler(ctx context.Context) error {
	gateway.logger.Infof("AdminHandler waiting for BackendConfig before starting on %d", adminWebPort)
	gateway.backendConfig.WaitForConfig(ctx)
	gateway.logger.Infof("AdminHandler starting on %d", adminWebPort)
	component := "gateway"
	srvMux := mux.NewRouter()
	srvMux.Use(
		middleware.StatMiddleware(ctx, srvMux, stats.Default, component),
		middleware.LimitConcurrentRequests(maxConcurrentRequests),
	)
	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(adminWebPort),
		Handler: bugsnag.Handler(srvMux),
	}

	return rs_httputil.ListenAndServe(ctx, srv)
}

// Gets the config from config backend and extracts enabled writekeys
func (gateway *HandleT) backendConfigSubscriber() {
	ch := gateway.backendConfig.Subscribe(context.TODO(), backendconfig.TopicProcessConfig)
	for data := range ch {
		var (
			newWriteKeysSourceMap          = map[string]backendconfig.SourceT{}
			newEnabledWriteKeyWebhookMap   = map[string]string{}
			newEnabledWriteKeyWorkspaceMap = map[string]string{}
			newSourceIDToNameMap           = map[string]string{}
		)
		config := data.Data.(map[string]backendconfig.ConfigT)
		for workspaceID, wsConfig := range config {
			for _, source := range wsConfig.Sources {
				newSourceIDToNameMap[source.ID] = source.Name
				newWriteKeysSourceMap[source.WriteKey] = source

				if source.Enabled {
					newEnabledWriteKeyWorkspaceMap[source.WriteKey] = workspaceID
					if source.SourceDefinition.Category == "webhook" {
						newEnabledWriteKeyWebhookMap[source.WriteKey] = source.SourceDefinition.Name
						gateway.webhookHandler.Register(source.SourceDefinition.Name)
					}
				}
			}
		}
		configSubscriberLock.Lock()
		writeKeysSourceMap = newWriteKeysSourceMap
		enabledWriteKeyWebhookMap = newEnabledWriteKeyWebhookMap
		enabledWriteKeyWorkspaceMap = newEnabledWriteKeyWorkspaceMap
		sourceIDToNameMap = newSourceIDToNameMap
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
func (gateway *HandleT) addToWebRequestQ(_ *http.ResponseWriter, req *http.Request, done chan string, reqType string, requestPayload []byte, writeKey string) {
	userIDHeader := req.Header.Get("AnonymousId")
	workerKey := userIDHeader
	if userIDHeader == "" {
		// If the request comes through proxy, proxy would already send this. So this shouldn't be happening in that case
		workerKey = uuid.New().String()
		gateway.emptyAnonIdHeaderStat.Increment()
	}
	userWebRequestWorker := gateway.findUserWebRequestWorker(workerKey)
	ipAddr := misc.GetIPFromReq(req)
	webReq := webRequestT{done: done, reqType: reqType, requestPayload: requestPayload, writeKey: writeKey, ipAddr: ipAddr, userIDHeader: userIDHeader}
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

// TrackRequestMetrics provides access to add request success/failure telemetry
func (gateway *HandleT) TrackRequestMetrics(errorMessage string) {
	gateway.trackRequestMetrics(errorMessage)
}

// GetWebhookSourceDefName returns the webhook source definition name by write key
func (*HandleT) GetWebhookSourceDefName(writeKey string) (name string, ok bool) {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	name, ok = enabledWriteKeyWebhookMap[writeKey]
	return
}

func (gateway *HandleT) SetReadonlyDB(readonlyGatewayDB jobsdb.ReadonlyJobsDB) {
	gateway.readonlyGatewayDB = readonlyGatewayDB
}

/*
Setup initializes this module:
- Monitors backend config for changes.
- Starts web request batching goroutine, that batches incoming messages.
- Starts web request batch db writer goroutine, that writes incoming batches to JobsDB.
- Starts debugging goroutine that prints gateway stats.

This function will block until backend config is initially received.
*/
func (gateway *HandleT) Setup(
	ctx context.Context,
	application app.App, backendConfig backendconfig.BackendConfig, jobsDB jobsdb.JobsDB,
	rateLimiter ratelimiter.RateLimiter, versionHandler func(w http.ResponseWriter, r *http.Request),
	rsourcesService rsources.JobService, sourcehandle sourcedebugger.SourceDebugger,
) error {
	gateway.logger = pkgLogger
	gateway.application = application
	gateway.stats = stats.Default

	gateway.rsourcesService = rsourcesService
	gateway.sourcehandle = sourcehandle

	gateway.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	config.RegisterDurationConfigVariable(30, &gateway.httpTimeout, false, time.Second, "Gateway.httpTimeout")
	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: gateway.httpTimeout}
	gateway.netHandle = client

	// For the lack of better stat type, using TimerType.
	gateway.batchSizeStat = gateway.stats.NewStat("gateway.batch_size", stats.HistogramType)
	gateway.requestSizeStat = gateway.stats.NewStat("gateway.request_size", stats.HistogramType)
	gateway.dbWritesStat = gateway.stats.NewStat("gateway.db_writes", stats.CountType)
	gateway.dbWorkersBufferFullStat = gateway.stats.NewStat("gateway.db_workers_buffer_full", stats.CountType)
	gateway.dbWorkersTimeOutStat = gateway.stats.NewStat("gateway.db_workers_time_out", stats.CountType)
	gateway.bodyReadTimeStat = gateway.stats.NewStat("gateway.http_body_read_time", stats.TimerType)
	gateway.addToWebRequestQWaitTime = gateway.stats.NewStat("gateway.web_request_queue_wait_time", stats.TimerType)
	gateway.addToBatchRequestQWaitTime = gateway.stats.NewStat("gateway.batch_request_queue_wait_time", stats.TimerType)
	gateway.processRequestTime = gateway.stats.NewStat("gateway.process_request_time", stats.TimerType)
	gateway.backendConfig = backendConfig
	gateway.rateLimiter = rateLimiter
	gateway.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, maxDBBatchSize)
	gateway.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, maxDBWriterProcess)
	gateway.emptyAnonIdHeaderStat = gateway.stats.NewStat("gateway.empty_anonymous_id_header", stats.CountType)
	gateway.jobsDB = jobsDB

	gateway.versionHandler = versionHandler

	gateway.irh = &ImportRequestHandler{}
	gateway.rrh = &RegularRequestHandler{}

	gateway.webhookHandler = webhook.Setup(gateway, gateway.stats)

	whURL, err := url.ParseRequestURI(misc.GetWarehouseURL())
	if err != nil {
		return fmt.Errorf("Invalid warehouse URL %s: %w", whURL, err)
	}
	whProxy := httputil.NewSingleHostReverseProxy(whURL)
	gateway.whProxy = whProxy

	gatewayAdmin := GatewayAdmin{handle: gateway}
	gatewayRPCHandler := GatewayRPCHandler{jobsDB: gateway.jobsDB, readOnlyJobsDB: gateway.readonlyGatewayDB}

	admin.RegisterStatusHandler("Gateway", &gatewayAdmin)
	admin.RegisterAdminHandler("Gateway", &gatewayRPCHandler)

	if enableSuppressUserFeature && gateway.application.Features().SuppressUser != nil {
		var err error
		gateway.suppressUserHandler, err = application.Features().SuppressUser.Setup(ctx, gateway.backendConfig)
		if err != nil {
			return fmt.Errorf("could not setup suppress user feature: %w", err)
		}
	}

	if enableEventSchemasFeature {
		gateway.eventSchemaHandler = event_schema.GetInstance()
	}

	rruntime.Go(func() {
		gateway.backendConfigSubscriber()
	})

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	gateway.backgroundCancel = cancel
	gateway.backgroundWait = g.Wait
	gateway.initUserWebRequestWorkers()

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
	return nil
}

func (gateway *HandleT) Shutdown() error {
	gateway.backgroundCancel()
	if err := gateway.webhookHandler.Shutdown(); err != nil {
		return err
	}

	// UserWebRequestWorkers
	for _, worker := range gateway.userWebRequestWorkers {
		close(worker.webRequestQ)
	}

	return gateway.backgroundWait()
}

func WithContentType(contentType string, delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", contentType)
		delegate(w, r)
	}
}

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/sanitize"
	"github.com/rudderlabs/rudder-go-kit/stringify"
	kituuid "github.com/rudderlabs/rudder-go-kit/uuid"
	"github.com/rudderlabs/rudder-schemas/go/stream"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/internal/bot"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type Handle struct {
	// dependencies

	config          *config.Config
	logger          logger.Logger
	stats           stats.Stats
	tracer          stats.Tracer
	application     app.App
	backendConfig   backendconfig.BackendConfig
	jobsDB          jobsdb.JobsDB
	errDB           jobsdb.JobsDB
	rateLimiter     throttler.Throttler
	versionHandler  func(w http.ResponseWriter, r *http.Request)
	rsourcesService rsources.JobService
	sourcehandle    sourcedebugger.SourceDebugger

	// statistic measurements initialised during Setup

	batchSizeStat                                 stats.Measurement
	requestSizeStat                               stats.Measurement
	dbWritesStat                                  stats.Measurement
	dbWorkersBufferFullStat, dbWorkersTimeOutStat stats.Measurement
	bodyReadTimeStat                              stats.Measurement
	addToWebRequestQWaitTime                      stats.Measurement
	addToBatchRequestQWaitTime                    stats.Measurement
	processRequestTime                            stats.Measurement
	emptyAnonIdHeaderStat                         stats.Measurement

	// state initialised during Setup

	diagnosisTicker              *time.Ticker
	userWorkerBatchRequestQ      chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ chan *batchUserWorkerBatchRequestT
	irh                          RequestHandler
	rrh                          RequestHandler
	webhook                      webhook.Webhook
	whProxy                      http.Handler
	suppressUserHandler          types.UserSuppression
	backgroundCancel             context.CancelFunc
	backgroundWait               func() error
	userWebRequestWorkers        []*userWebRequestWorkerT
	backendConfigInitialisedChan chan struct{}
	now                          func() time.Time

	// other state

	backendConfigInitialised bool
	inFlightRequests         *sync.WaitGroup

	trackCounterMu    sync.Mutex // protects trackSuccessCount and trackFailureCount
	trackSuccessCount int
	trackFailureCount int

	// backendconfig state
	configSubscriberLock sync.RWMutex
	writeKeysSourceMap   map[string]backendconfig.SourceT
	sourceIDSourceMap    map[string]backendconfig.SourceT

	conf struct { // configuration parameters
		webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess                       int
		maxUserWebRequestBatchSize, maxDBBatchSize, maxHeaderBytes, maxConcurrentRequests int
		userWebRequestBatchTimeout, dbBatchWriteTimeout                                   config.ValueLoader[time.Duration]

		maxReqSize                           config.ValueLoader[int]
		enableRateLimit                      config.ValueLoader[bool]
		enableSuppressUserFeature            bool
		diagnosisTickerTime                  time.Duration
		ReadTimeout                          time.Duration
		ReadHeaderTimeout                    time.Duration
		WriteTimeout                         time.Duration
		IdleTimeout                          time.Duration
		allowReqsWithoutUserIDAndAnonymousID config.ValueLoader[bool]
		gwAllowPartialWriteWithErrors        config.ValueLoader[bool]
	}

	// additional internal http handlers
	internalHttpHandlers map[string]http.Handler

	streamMsgValidator func(message *stream.Message) error
}

// findUserWebRequestWorker finds and returns the worker that works on a particular `userID`.
// This is done so that requests with a userID keep going to the same worker, which would maintain the consistency in event ordering.
func (gw *Handle) findUserWebRequestWorker(userID string) *userWebRequestWorkerT {
	index := int(math.Abs(float64(misc.GetHash(userID) % gw.conf.maxUserWebRequestWorkerProcess)))

	userWebRequestWorker := gw.userWebRequestWorkers[index]
	if userWebRequestWorker == nil {
		panic(fmt.Errorf("worker is nil"))
	}

	return userWebRequestWorker
}

//	userWebRequestBatcher listens on the `webRequestQ` channel of a worker.
//	Based on `userWebRequestBatchTimeout` and `maxUserWebRequestBatchSize` parameters,
//	batches them together and queues the batch of webreqs in the `batchRequestQ` channel of the worker
//
// Every webRequestWorker keeps doing this concurrently.
func (gw *Handle) userWebRequestBatcher(userWebRequestWorker *userWebRequestWorkerT) {
	for {
		reqBuffer, numWebRequests, bufferTime, isWebRequestQOpen := lo.BufferWithTimeout(
			userWebRequestWorker.webRequestQ,
			gw.conf.maxUserWebRequestBatchSize,
			gw.conf.userWebRequestBatchTimeout.Load(),
		)
		if numWebRequests > 0 {
			if numWebRequests == gw.conf.maxUserWebRequestBatchSize {
				userWebRequestWorker.bufferFullStat.Count(1)
			}
			if bufferTime >= gw.conf.userWebRequestBatchTimeout.Load() {
				userWebRequestWorker.timeOutStat.Count(1)
			}
			breq := batchWebRequestT{batchRequest: reqBuffer}
			start := time.Now()
			userWebRequestWorker.batchRequestQ <- &breq
			gw.addToBatchRequestQWaitTime.SendTiming(time.Since(start))
		}
		if !isWebRequestQOpen {
			close(userWebRequestWorker.batchRequestQ)
			return
		}
	}
}

//	userWebRequestWorkerProcess listens on the `batchRequestQ` channel of the webRequestWorker for new batches of webRequests
//	Goes over the webRequests in the batch and filters them out(`rateLimit`, `maxReqSize`).
//	And creates a `jobList` which is then sent to `userWorkerBatchRequestQ` of the gateway and waits for a response
//	from the `dbwriterWorker`s that batch them and write to the db.
//
// Finally sends responses(error) if any back to the webRequests over their `done` channels
func (gw *Handle) userWebRequestWorkerProcess(userWebRequestWorker *userWebRequestWorkerT) {
	for breq := range userWebRequestWorker.batchRequestQ {
		var jobBatches [][]*jobsdb.JobT
		jobIDReqMap := make(map[uuid.UUID]*webRequestT)
		jobSourceTagMap := make(map[uuid.UUID]string)
		sourceStats := make(map[string]*gwstats.SourceStat)
		// Saving the event data read from req.request.Body to the splice.
		// Using this to send event schema to the config backend.
		var eventBatchesToRecord []sourceDebugger
		batchStart := time.Now()
		for _, req := range breq.batchRequest {
			arctx := req.authContext
			sourceTag := arctx.SourceTag()
			if _, ok := sourceStats[sourceTag]; !ok {
				sourceStats[sourceTag] = gw.NewSourceStat(arctx, req.reqType)
			}
			jobData, err := gw.getJobDataFromRequest(req)
			sourceStats[sourceTag].Version = jobData.version
			sourceStats[sourceTag].RequestEventsBot(jobData.botEvents)
			if err != nil {
				switch {
				case errors.Is(err, errRequestDropped):
					req.done <- response.TooManyRequests
					sourceStats[sourceTag].RequestDropped()
				case errors.Is(err, errRequestSuppressed):
					req.done <- "" // no error
					sourceStats[sourceTag].RequestSuppressed()
				default:
					req.done <- err.Error()
					sourceStats[sourceTag].RequestEventsFailed(jobData.numEvents, err.Error())
				}
				continue
			}
			if len(jobData.jobs) > 0 {
				jobBatches = append(jobBatches, jobData.jobs)
				jobIDReqMap[jobData.jobs[0].UUID] = req
				jobSourceTagMap[jobData.jobs[0].UUID] = sourceTag
				for _, job := range jobData.jobs {
					eventBatchesToRecord = append(
						eventBatchesToRecord,
						sourceDebugger{
							data:     job.EventPayload,
							writeKey: arctx.WriteKey,
						},
					)
				}
			} else {
				req.done <- response.EmptyBatchPayload
				sourceStats[sourceTag].RequestFailed(response.EmptyBatchPayload)
			}
		}

		errorMessagesMap := make(map[uuid.UUID]string)
		if len(jobBatches) > 0 {
			gw.userWorkerBatchRequestQ <- &userWorkerBatchRequestT{
				jobBatches:  jobBatches,
				respChannel: userWebRequestWorker.reponseQ,
			}
			errorMessagesMap = <-userWebRequestWorker.reponseQ
		}

		for _, batch := range jobBatches {
			err, found := errorMessagesMap[batch[0].UUID]
			sourceTag := jobSourceTagMap[batch[0].UUID]
			if found {
				sourceStats[sourceTag].RequestEventsFailed(len(batch), "storeFailed")
				jobIDReqMap[batch[0].UUID].errors = append(jobIDReqMap[batch[0].UUID].errors, err)
			} else {
				sourceStats[sourceTag].RequestEventsSucceeded(len(batch))
			}
			jobIDReqMap[batch[0].UUID].done <- err
		}
		// Sending events to config backend
		for _, eventBatch := range eventBatchesToRecord {
			gw.sourcehandle.RecordEvent(eventBatch.writeKey, eventBatch.data)
		}

		userWebRequestWorker.batchTimeStat.Since(batchStart)
		gw.batchSizeStat.Observe(float64(len(breq.batchRequest)))

		for _, v := range sourceStats {
			v.Report(gw.stats)
		}
	}
}

// getJobDataFromRequest parses the request body and returns the jobData or an error if
// - the request payload is invalid JSON
// - the request payload does not correspond to a rudder event
// - the write key is invalid
// - the write key is not allowed to send events
// - the request is rate limited
// - the payload doesn't contain a valid identifier (userId, anonymousId)
// - the payload is too large
// - user in the payload is not allowed to send events (suppressed)
func (gw *Handle) getJobDataFromRequest(req *webRequestT) (jobData *jobFromReq, err error) {
	var (
		arctx         = req.authContext
		sourceID      = arctx.SourceID
		destinationID = arctx.DestinationID
		// Should be function of body
		workspaceId  = arctx.WorkspaceID
		userIDHeader = req.userIDHeader
		ipAddr       = req.ipAddr
		body         = req.requestPayload

		// values retrieved from first event in batch
		sourcesJobRunID, sourcesTaskRunID = req.authContext.SourceJobRunID, req.authContext.SourceTaskRunID
		sourceCategory                    = req.authContext.SourceCategory

		// tracing
		traceParent = req.traceParent
	)

	fillMessageID := func(event map[string]interface{}) {
		messageID, _ := event["messageId"].(string)
		messageID = strings.TrimSpace(sanitize.Unicode(messageID))
		if messageID == "" {
			event["messageId"] = uuid.New().String()
		} else {
			event["messageId"] = messageID
		}
	}

	jobData = &jobFromReq{}
	if !gjson.ValidBytes(body) {
		err = errors.New(response.InvalidJSON)
		return
	}

	gw.requestSizeStat.Observe(float64(len(body)))
	if req.reqType != "batch" && req.reqType != "replay" && req.reqType != "retl" {
		body, err = sjson.SetBytes(body, "type", req.reqType)
		if err != nil {
			err = errors.New(response.NotRudderEvent)
			return
		}
		body, _ = sjson.SetRawBytes(batchEvent, "batch.0", body)
	}

	eventsBatch := gjson.GetBytes(body, "batch").Array()
	jobData.numEvents = len(eventsBatch)

	type jobObject struct {
		userID string
		events []map[string]interface{}
	}

	var (
		// map to hold modified/filtered events of the batch
		out []jobObject

		marshalledParams []byte

		// facts about the batch populated as we iterate over events
		containsAudienceList, suppressed bool
	)

	isUserSuppressed := gw.memoizedIsUserSuppressed()
	for idx, v := range eventsBatch {
		toSet, ok := v.Value().(map[string]interface{})
		if !ok {
			err = errors.New(response.NotRudderEvent)
			return
		}

		anonIDFromReq := strings.TrimSpace(sanitize.Unicode(stringify.Any(toSet["anonymousId"])))
		userIDFromReq := strings.TrimSpace(sanitize.Unicode(stringify.Any(toSet["userId"])))
		eventTypeFromReq, _ := misc.MapLookup(
			toSet,
			"type",
		).(string)

		if gw.isNonIdentifiable(anonIDFromReq, userIDFromReq, eventTypeFromReq) {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}

		eventContext, ok := misc.MapLookup(toSet, "context").(map[string]interface{})
		if ok {
			if idx == 0 {
				if v, _ := misc.MapLookup(eventContext, "sources", "job_run_id").(string); v != "" {
					sourcesJobRunID = v
				}
				if v, _ := misc.MapLookup(eventContext, "sources", "task_run_id").(string); v != "" {
					sourcesTaskRunID = v
				}

				// calculate version
				firstSDKName, _ := misc.MapLookup(
					eventContext,
					"library",
					"name",
				).(string)
				firstSDKVersion, _ := misc.MapLookup(
					eventContext,
					"library",
					"version",
				).(string)

				if firstSDKVersion != "" && !semverRegexp.Match([]byte(firstSDKVersion)) { // skipcq: CRT-A0007
					firstSDKVersion = "invalid"
				}
				if firstSDKName != "" || firstSDKVersion != "" {
					jobData.version = firstSDKName + "/" + firstSDKVersion
				}
			}

			userAgent, _ := misc.MapLookup(
				eventContext,
				"userAgent",
			).(string)
			if bot.IsBotUserAgent(userAgent) {
				jobData.botEvents++
			}
		}

		if isUserSuppressed(workspaceId, userIDFromReq, sourceID) {
			suppressed = true
			continue
		}

		// hashing combination of userIDFromReq + anonIDFromReq, using colon as a delimiter
		rudderId, err := getRudderId(userIDFromReq, anonIDFromReq)
		if err != nil {
			return jobData, err
		}
		toSet["rudderId"] = rudderId
		if _, ok := toSet["receivedAt"]; !ok {
			toSet["receivedAt"] = gw.now().Format(misc.RFC3339Milli)
		}
		if _, ok := toSet["request_ip"]; ok {
			var tcOk bool
			ipAddr, tcOk = toSet["request_ip"].(string)
			if !tcOk {
				gw.logger.Warnf("request_ip is not a string: %v", toSet["request_ip"])
			}

		} else {
			toSet["request_ip"] = ipAddr
		}
		fillMessageID(toSet)
		if eventTypeFromReq == "audiencelist" {
			containsAudienceList = true
		}

		userID := buildUserID(userIDHeader, anonIDFromReq, userIDFromReq)
		out = append(out, jobObject{
			userID: userID,
			events: []map[string]interface{}{toSet},
		})
	}

	if gw.conf.enableRateLimit.Load() && sourcesJobRunID == "" && sourcesTaskRunID == "" {
		// In case of "batch" requests, if rate-limiter returns true for LimitReached, just drop the event batch and continue.
		ok, errCheck := gw.rateLimiter.CheckLimitReached(context.TODO(), workspaceId, int64(len(eventsBatch)))
		if errCheck != nil {
			gw.stats.NewTaggedStat("gateway.rate_limiter_error", stats.CountType, stats.Tags{"workspaceId": workspaceId}).Increment()
			gw.logger.Errorf("Rate limiter error: %v Allowing the request", errCheck)
		}
		if ok {
			return jobData, errRequestDropped
		}
	}

	if len(out) == 0 && suppressed {
		err = errRequestSuppressed
		return
	}

	if len(body) > gw.conf.maxReqSize.Load() && !containsAudienceList {
		err = errors.New(response.RequestBodyTooLarge)
		return
	}

	params := map[string]any{
		"source_id":          sourceID,
		"source_job_run_id":  sourcesJobRunID,
		"source_task_run_id": sourcesTaskRunID,
		"traceparent":        traceParent,
		"source_category":    sourceCategory,
	}
	if len(destinationID) != 0 {
		params["destination_id"] = destinationID
	}
	marshalledParams, err = json.Marshal(params)
	if err != nil {
		gw.logger.Errorf(
			"[Gateway] Failed to marshal parameters map. Parameters: %+v",
			params,
		)
		marshalledParams = []byte(
			`{"error": "rudder-server gateway failed to marshal params"}`,
		)
	}
	jobs := make([]*jobsdb.JobT, 0)
	for _, userEvent := range out {
		var (
			payload    json.RawMessage
			eventCount int
		)
		{
			type SingularEventBatch struct {
				Batch      []map[string]interface{} `json:"batch"`
				RequestIP  string                   `json:"requestIP"`
				WriteKey   string                   `json:"writeKey"`
				ReceivedAt string                   `json:"receivedAt"`
			}
			receivedAt, ok := userEvent.events[0]["receivedAt"].(string)
			if !ok || !arctx.ReplaySource {
				receivedAt = gw.now().Format(misc.RFC3339Milli)
			}
			singularEventBatch := SingularEventBatch{
				Batch:      userEvent.events,
				RequestIP:  ipAddr,
				WriteKey:   arctx.WriteKey,
				ReceivedAt: receivedAt,
			}
			payload, err = json.Marshal(singularEventBatch)
			if err != nil {
				err = errors.New(response.InvalidJSON)
				return
			}
			eventCount = len(userEvent.events)
		}

		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       userEvent.userID,
			Parameters:   marshalledParams,
			CustomVal:    customVal,
			EventPayload: payload,
			EventCount:   eventCount,
			WorkspaceId:  workspaceId,
		})
	}
	err = nil
	jobData.jobs = jobs
	return
}

func (gw *Handle) isNonIdentifiable(anonIDFromReq, userIDFromReq, eventType string) bool {
	if eventType == extractEvent || eventType == rETLEvent {
		// extract or rETL event is allowed without user id and anonymous id
		return false
	}
	if anonIDFromReq == "" && userIDFromReq == "" {
		return !gw.conf.allowReqsWithoutUserIDAndAnonymousID.Load()
	}
	return false
}

func buildUserID(userIDHeader, anonIDFromReq, userIDFromReq string) string {
	if anonIDFromReq == "" {
		anonIDFromReq = userIDFromReq
	}
	return userIDHeader + delimiter + anonIDFromReq + delimiter + userIDFromReq
}

// memoizedIsUserSuppressed is a memoized version of isUserSuppressed
func (gw *Handle) memoizedIsUserSuppressed() func(workspaceID, userID, sourceID string) bool {
	cache := map[string]bool{}
	return func(workspaceID, userID, sourceID string) bool {
		key := workspaceID + ":" + userID + ":" + sourceID
		if val, ok := cache[key]; ok {
			return val
		}
		val := gw.isUserSuppressed(workspaceID, userID, sourceID)
		cache[key] = val
		return val
	}
}

// isUserSuppressed checks if the user is suppressed or not
func (gw *Handle) isUserSuppressed(workspaceID, userID, sourceID string) bool {
	if !gw.conf.enableSuppressUserFeature || gw.suppressUserHandler == nil {
		return false
	}
	if metadata := gw.suppressUserHandler.GetSuppressedUser(workspaceID, userID, sourceID); metadata != nil {
		if !metadata.CreatedAt.IsZero() {
			gw.stats.NewTaggedStat("gateway.user_suppression_age", stats.TimerType, stats.Tags{
				"workspaceId": workspaceID,
				"sourceID":    sourceID,
			}).Since(metadata.CreatedAt)
		}
		return true
	}
	return false
}

// getPayload reads the request body and returns the payload's bytes or an error if the payload cannot be read
func (gw *Handle) getPayload(arctx *gwtypes.AuthRequestContext, r *http.Request, reqType string) ([]byte, error) {
	payload, err := gw.getPayloadFromRequest(r)
	if err != nil {
		stat := gwstats.SourceStat{
			Source:      arctx.SourceTag(),
			WriteKey:    arctx.WriteKey,
			ReqType:     reqType,
			SourceID:    arctx.SourceID,
			WorkspaceID: arctx.WorkspaceID,
			SourceType:  arctx.SourceCategory,
		}
		stat.RequestFailed("requestBodyReadFailed")
		stat.Report(gw.stats)

		return nil, err
	}
	return payload, err
}

func (gw *Handle) getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte{}, errors.New(response.RequestBodyNil)
	}

	start := time.Now()
	defer gw.bodyReadTimeStat.Since(start)

	payload, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		gw.logger.Errorf(
			"Error reading request body, 'Content-Length': %s, partial payload:\n\t%s\n:%v",
			r.Header.Get("Content-Length"),
			string(payload),
			err,
		)
		return payload, errors.New(response.RequestBodyReadFailed)
	}
	return payload, nil
}

/*
addToWebRequestQ finds the worker for a particular userID and queues the webrequest with the worker(pushes the req into the webRequestQ channel of the worker).
They are further batched together in userWebRequestBatcher
*/
func (gw *Handle) addToWebRequestQ(_ *http.ResponseWriter, req *http.Request, done chan string, reqType string, requestPayload []byte, arctx *gwtypes.AuthRequestContext) {
	userIDHeader := req.Header.Get("AnonymousId")
	workerKey := userIDHeader
	if userIDHeader == "" {
		// If the request comes through proxy, proxy would already send this. So this shouldn't be happening in that case
		workerKey = uuid.New().String()
		gw.emptyAnonIdHeaderStat.Increment()
	}
	userWebRequestWorker := gw.findUserWebRequestWorker(workerKey)
	ipAddr := kithttputil.GetRequestIP(req)

	traceParent := stats.GetTraceParentFromContext(req.Context())
	if traceParent == "" {
		gw.logger.Debugw("traceParent not found in request")
	}

	webReq := webRequestT{
		done:           done,
		reqType:        reqType,
		requestPayload: requestPayload,
		authContext:    arctx,
		traceParent:    traceParent,
		ipAddr:         ipAddr,
		userIDHeader:   userIDHeader,
	}
	userWebRequestWorker.webRequestQ <- &webReq
}

func (gw *Handle) internalBatchHandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			ctx           = r.Context()
			reqType       = ctx.Value(gwtypes.CtxParamCallType).(string)
			jobsWithStats []jobWithStat
			body          []byte
			err           error
			status        int
			errorMessage  string
			responseBody  string
			stat          = gwstats.SourceStat{ReqType: reqType}
		)

		// TODO: add tracing
		gw.logger.LogRequest(r)
		body, err = gw.getPayloadFromRequest(r)
		if err != nil {
			stat.RequestFailed("requestBodyReadFailed")
			stat.Report(gw.stats)
			goto requestError
		}
		jobsWithStats, err = gw.extractJobsFromInternalBatchPayload(reqType, body)
		if err != nil {
			goto requestError
		}

		if len(jobsWithStats) > 0 {
			jobs := lo.Map(jobsWithStats, func(jws jobWithStat, _ int) *jobsdb.JobT {
				return jws.job
			})
			if err = gw.storeJobs(ctx, jobs); err != nil {
				for _, jws := range jobsWithStats {
					jws.stat.EventsFailed(1, "storeFailed")
					jws.stat.Report(gw.stats)
				}
				stat.RequestFailed("storeFailed")
				stat.Report(gw.stats)
				goto requestError
			}
			for _, jws := range jobsWithStats {
				jws.stat.EventsSuccess(1)
				jws.stat.Report(gw.stats)
				// Sending events to config backend
				if jws.stat.WriteKey == "" {
					gw.logger.Errorn("writeKey not found in event payload")
					continue
				}
				gw.sourcehandle.RecordEvent(jws.stat.WriteKey, jws.job.EventPayload)
			}
			stat.RequestSucceeded()
			stat.Report(gw.stats)
		} else {
			stat.RequestEventsSucceeded(0)
			stat.Report(gw.stats)
		}

		status = http.StatusOK
		responseBody = response.GetStatus(response.Ok)
		gw.logger.Debugn("response",
			logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
			logger.NewStringField("path", r.URL.Path),
			logger.NewIntField("status", int64(status)),
			logger.NewStringField("body", responseBody),
		)
		_, _ = w.Write([]byte(responseBody))
		return

	requestError:
		errorMessage = err.Error()
		status = response.GetErrorStatusCode(errorMessage)
		responseBody = response.GetStatus(errorMessage)
		gw.logger.Errorn("response",
			logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
			logger.NewStringField("path", r.URL.Path),
			logger.NewIntField("status", int64(status)),
			obskit.Error(err),
		)
		gw.logger.Debugn("response",
			logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
			logger.NewStringField("path", r.URL.Path),
			logger.NewIntField("status", int64(status)),
			logger.NewStringField("body", responseBody),
			logger.NewStringField("request", string(body)),
		)
		http.Error(w, responseBody, status)
	}
}

type jobWithStat struct {
	job  *jobsdb.JobT
	stat gwstats.SourceStat
}

func (gw *Handle) extractJobsFromInternalBatchPayload(reqType string, body []byte) (
	[]jobWithStat, error,
) {
	type params struct {
		MessageID       string `json:"message_id"`
		SourceID        string `json:"source_id"`
		SourceJobRunID  string `json:"source_job_run_id"`
		SourceTaskRunID string `json:"source_task_run_id"`
		UserID          string `json:"user_id"`
		TraceParent     string `json:"traceparent"`
		DestinationID   string `json:"destination_id,omitempty"`
		SourceCategory  string `json:"source_category"`
	}

	type singularEventBatch struct {
		Batch      []json.RawMessage `json:"batch"`
		ReceivedAt string            `json:"receivedAt"`
		RequestIP  string            `json:"requestIP"`
		WriteKey   string            `json:"writeKey"` // only needed for live-events
	}

	var (
		messages         []stream.Message
		isUserSuppressed = gw.memoizedIsUserSuppressed()
		res              []jobWithStat
		stat             = gwstats.SourceStat{ReqType: reqType}
	)

	err := jsonfast.Unmarshal(body, &messages)
	if err != nil {
		stat.RequestFailed(response.InvalidJSON)
		stat.Report(gw.stats)
		gw.logger.Errorn("invalid json in request",
			obskit.Error(err))
		return nil, errors.New(response.InvalidJSON)
	}
	gw.requestSizeStat.Observe(float64(len(body)))

	if len(messages) == 0 {
		stat.RequestFailed(response.NotRudderEvent)
		stat.Report(gw.stats)
		gw.logger.Errorn("no messages in request")
		return nil, errors.New(response.NotRudderEvent)
	}

	res = make([]jobWithStat, 0, len(messages))

	for _, msg := range messages {
		stat := gwstats.SourceStat{ReqType: reqType}
		err := gw.streamMsgValidator(&msg)
		if err != nil {
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.logger.Errorn("invalid message in request",
				loggerFields...)
			stat.RequestEventsFailed(1, response.InvalidStreamMessage)
			stat.Report(gw.stats)
			return nil, errors.New(response.InvalidStreamMessage)
		}
		// TODO: get rid of this check
		if msg.Properties.RequestType != "" {
			switch msg.Properties.RequestType {
			case "batch", "replay", "retl", "import":
			default:
				msg.Payload, err = sjson.SetBytes(msg.Payload, "type", msg.Properties.RequestType)
				if err != nil {
					stat.RequestEventsFailed(1, response.NotRudderEvent)
					stat.Report(gw.stats)
					loggerFields := msg.Properties.LoggerFields()
					loggerFields = append(loggerFields, obskit.Error(err))
					gw.logger.Errorn("failed to set type in message", loggerFields...)
					return nil, errors.New(response.NotRudderEvent)
				}
			}
		}

		anonIDFromReq := sanitizeAndTrim(gjson.GetBytes(msg.Payload, "anonymousId").String())
		userIDFromReq := sanitizeAndTrim(gjson.GetBytes(msg.Payload, "userId").String())
		messageID, changed := getMessageID(msg.Payload)
		if changed {
			msg.Payload, err = sjson.SetBytes(msg.Payload, "messageId", messageID)
			if err != nil {
				stat.RequestFailed(response.NotRudderEvent)
				stat.Report(gw.stats)
				gw.logger.Errorn("failed to set messageID in message",
					obskit.Error(err))
				return nil, errors.New(response.NotRudderEvent)
			}
		}
		rudderId, err := getRudderId(userIDFromReq, anonIDFromReq)
		if err != nil {
			stat.RequestFailed(response.NotRudderEvent)
			stat.Report(gw.stats)
			gw.logger.Errorn("failed to get rudderId",
				obskit.Error(err))
			return nil, errors.New(response.NotRudderEvent)
		}
		msg.Payload, err = sjson.SetBytes(msg.Payload, "rudderId", rudderId.String())
		if err != nil {
			stat.RequestFailed(response.NotRudderEvent)
			stat.Report(gw.stats)
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.logger.Errorn("failed to set rudderId in message",
				loggerFields...)
			return nil, errors.New(response.NotRudderEvent)
		}
		writeKey, ok := gw.getWriteKeyFromSourceID(msg.Properties.SourceID)
		if !ok {
			// only live-events will not work if writeKey is not found
			gw.logger.Errorn("unable to get writeKey for job",
				logger.NewStringField("messageId", messageID),
				obskit.SourceID(msg.Properties.SourceID))
		}
		stat.SourceID = msg.Properties.SourceID
		stat.WorkspaceID = msg.Properties.WorkspaceID
		stat.WriteKey = writeKey
		if isUserSuppressed(msg.Properties.WorkspaceID, msg.Properties.UserID, msg.Properties.SourceID) {
			sourceConfig := gw.getSourceConfigFromSourceID(msg.Properties.SourceID)
			gw.logger.Infon("suppressed event",
				obskit.SourceID(msg.Properties.SourceID),
				obskit.WorkspaceID(msg.Properties.WorkspaceID),
				logger.NewStringField("userIDFromReq", msg.Properties.UserID),
			)
			gw.stats.NewTaggedStat(
				"gateway.write_key_suppressed_events",
				stats.CountType,
				gw.newSourceStatTagsWithReason(&sourceConfig, reqType, errEventSuppressed.Error()),
			).Increment()
			continue
		}

		gw.stats.NewTaggedStat("gateway.event_pickup_lag_seconds", stats.TimerType, stats.Tags{
			"workspaceId": msg.Properties.WorkspaceID,
		}).Since(msg.Properties.ReceivedAt)

		jobsDBParams := params{
			MessageID:       messageID,
			SourceID:        msg.Properties.SourceID,
			SourceJobRunID:  msg.Properties.SourceJobRunID,
			SourceTaskRunID: msg.Properties.SourceTaskRunID,
			UserID:          msg.Properties.UserID,
			TraceParent:     msg.Properties.TraceID,
			DestinationID:   msg.Properties.DestinationID,
			SourceCategory:  msg.Properties.SourceType,
		}

		marshalledParams, err := json.Marshal(jobsDBParams)
		if err != nil {
			gw.logger.Errorn("[Gateway] Failed to marshal parameters map",
				logger.NewField("params", jobsDBParams),
				obskit.Error(err),
			)
			marshalledParams = []byte(
				`{"error": "rudder-server gateway failed to marshal params"}`,
			)
		}

		msg.Payload, err = fillReceivedAt(msg.Payload, msg.Properties.ReceivedAt)
		if err != nil {
			err = fmt.Errorf("filling receivedAt: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.stats)
			gw.logger.Errorn("failed to fill receivedAt in message",
				obskit.Error(err))
			return nil, fmt.Errorf("filling receivedAt: %w", err)
		}
		msg.Payload, err = fillRequestIP(msg.Payload, msg.Properties.RequestIP)
		if err != nil {
			err = fmt.Errorf("filling request_ip: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.stats)
			gw.logger.Errorn("failed to fill request_ip in message",
				obskit.Error(err))
			return nil, fmt.Errorf("filling request_ip: %w", err)
		}

		eventBatch := singularEventBatch{
			Batch:      []json.RawMessage{msg.Payload},
			ReceivedAt: msg.Properties.ReceivedAt.Format(misc.RFC3339Milli),
			RequestIP:  msg.Properties.RequestIP,
			WriteKey:   writeKey,
		}

		payload, err := json.Marshal(eventBatch)
		if err != nil {
			err = fmt.Errorf("marshalling event batch: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.stats)
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.logger.Errorn("failed to marshal event batch",
				loggerFields...)
			return nil, fmt.Errorf("marshalling event batch: %w", err)
		}
		jobUUID := uuid.New()
		res = append(res, jobWithStat{
			stat: stat,
			job: &jobsdb.JobT{
				UUID:         jobUUID,
				UserID:       msg.Properties.RoutingKey,
				Parameters:   marshalledParams,
				CustomVal:    customVal,
				EventPayload: payload,
				EventCount:   len(eventBatch.Batch),
				WorkspaceId:  msg.Properties.WorkspaceID,
			},
		})
	}
	if len(res) == 0 { // events suppressed - but return success
		return nil, nil
	}

	return res, nil
}

// getMessageID returns the messageID from the event payload.
// If the messageID is not present, it generates a new one.
// It also returns a boolean indicating if the messageID was changed.
func getMessageID(event []byte) (string, bool) {
	messageID := gjson.GetBytes(event, "messageId").String()
	sanitizedMessageID := sanitizeAndTrim(messageID)
	if sanitizedMessageID == "" {
		return uuid.New().String(), true
	}
	return sanitizedMessageID, messageID != sanitizedMessageID
}

func getRudderId(userIDFromReq, anonIDFromReq string) (uuid.UUID, error) {
	rudderId, err := kituuid.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
	if err != nil {
		err = errors.New(response.NonIdentifiableRequest)
		return rudderId, err
	}
	return rudderId, nil
}

func sanitizeAndTrim(str string) string {
	return strings.TrimSpace(sanitize.Unicode(str))
}

func fillReceivedAt(event []byte, receivedAt time.Time) ([]byte, error) {
	if !gjson.GetBytes(event, "receivedAt").Exists() {
		return sjson.SetBytes(event, "receivedAt", receivedAt.Format(misc.RFC3339Milli))
	}
	return event, nil
}

func fillRequestIP(event []byte, ip string) ([]byte, error) {
	if !gjson.GetBytes(event, "request_ip").Exists() {
		return sjson.SetBytes(event, "request_ip", ip)
	}
	return event, nil
}

func (gw *Handle) getSourceConfigFromSourceID(sourceID string) backendconfig.SourceT {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.sourceIDSourceMap[sourceID]; ok {
		return s
	}
	return backendconfig.SourceT{}
}

func (gw *Handle) getWriteKeyFromSourceID(sourceID string) (string, bool) {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.sourceIDSourceMap[sourceID]; ok {
		return s.WriteKey, true
	}
	return "", false
}

func (gw *Handle) storeJobs(ctx context.Context, jobs []*jobsdb.JobT) error {
	ctx, cancel := context.WithTimeout(ctx, gw.conf.WriteTimeout)
	defer cancel()
	defer gw.dbWritesStat.Count(1)
	return gw.jobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
		if err := gw.jobsDB.StoreInTx(ctx, tx, jobs); err != nil {
			gw.logger.Errorn(
				"Store into gateway db failed with error",
				obskit.Error(err),
				logger.NewField("jobs", jobs),
			)
			return err
		}

		// rsources stats
		rsourcesStats := rsources.NewStatsCollector(
			gw.rsourcesService,
			rsources.IgnoreDestinationID(),
		)
		rsourcesStats.JobsStoredWithErrors(jobs, nil)
		return rsourcesStats.Publish(ctx, tx.SqlTx())
	})
}

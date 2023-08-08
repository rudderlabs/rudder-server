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

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/internal/bot"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Handle struct {
	// dependencies

	config          *config.Config
	logger          logger.Logger
	stats           stats.Stats
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
	netHandle                    *http.Client
	userWorkerBatchRequestQ      chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ chan *batchUserWorkerBatchRequestT
	irh                          RequestHandler
	rrh                          RequestHandler
	webhookHandler               webhook.Webhook
	whProxy                      http.Handler
	suppressUserHandler          types.UserSuppression
	eventSchemaHandler           types.EventSchemasI
	backgroundCancel             context.CancelFunc
	backgroundWait               func() error
	userWebRequestWorkers        []*userWebRequestWorkerT
	backendConfigInitialisedChan chan struct{}

	// other state

	backendConfigInitialised bool
	ackCount                 uint64
	recvCount                uint64

	trackCounterMu    sync.Mutex // protects trackSuccessCount and trackFailureCount
	trackSuccessCount int
	trackFailureCount int

	conf struct { // configuration parameters
		httpTimeout                                                                       time.Duration
		webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess, adminWebPort         int
		maxUserWebRequestBatchSize, maxDBBatchSize, maxHeaderBytes, maxConcurrentRequests int
		userWebRequestBatchTimeout, dbBatchWriteTimeout                                   time.Duration
		writeKeysSourceMap                                                                map[string]backendconfig.SourceT
		enabledWriteKeyWebhookMap                                                         map[string]string
		enabledWriteKeyWorkspaceMap                                                       map[string]string
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
		allowBatchSplitting                                                               bool
	}
}

// findUserWebRequestWorker finds and returns the worker that works on a particular `userID`.
// This is done so that requests with a userID keep going to the same worker, which would maintain the consistency in event ordering.
func (gateway *Handle) findUserWebRequestWorker(userID string) *userWebRequestWorkerT {
	index := int(math.Abs(float64(misc.GetHash(userID) % gateway.conf.maxUserWebRequestWorkerProcess)))

	userWebRequestWorker := gateway.userWebRequestWorkers[index]
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
func (gateway *Handle) userWebRequestBatcher(userWebRequestWorker *userWebRequestWorkerT) {
	for {
		reqBuffer, numWebRequests, bufferTime, isWebRequestQOpen := lo.BufferWithTimeout(
			userWebRequestWorker.webRequestQ,
			gateway.conf.maxUserWebRequestBatchSize,
			gateway.conf.userWebRequestBatchTimeout,
		)
		if numWebRequests > 0 {
			if numWebRequests == gateway.conf.maxUserWebRequestBatchSize {
				userWebRequestWorker.bufferFullStat.Count(1)
			}
			if bufferTime >= gateway.conf.userWebRequestBatchTimeout {
				userWebRequestWorker.timeOutStat.Count(1)
			}
			breq := batchWebRequestT{batchRequest: reqBuffer}
			start := time.Now()
			userWebRequestWorker.batchRequestQ <- &breq
			gateway.addToBatchRequestQWaitTime.SendTiming(time.Since(start))
		}
		if !isWebRequestQOpen {
			close(userWebRequestWorker.batchRequestQ)
			return
		}
	}
}

func (gateway *Handle) NewSourceStat(writeKey, reqType string) *gwstats.SourceStat {
	return &gwstats.SourceStat{
		Source:      gateway.getSourceTagFromWriteKey(writeKey),
		SourceID:    gateway.getSourceIDForWriteKey(writeKey),
		WriteKey:    writeKey,
		ReqType:     reqType,
		WorkspaceID: gateway.getWorkspaceForWriteKey(writeKey),
		SourceType:  gateway.getSourceCategoryForWriteKey(writeKey),
	}
}

//	userWebRequestWorkerProcess listens on the `batchRequestQ` channel of the webRequestWorker for new batches of webRequests
//	Goes over the webRequests in the batch and filters them out(`rateLimit`, `maxReqSize`).
//	And creates a `jobList` which is then sent to `userWorkerBatchRequestQ` of the gateway and waits for a response
//	from the `dbwriterWorker`s that batch them and write to the db.
//
// Finally sends responses(error) if any back to the webRequests over their `done` channels
func (gateway *Handle) userWebRequestWorkerProcess(userWebRequestWorker *userWebRequestWorkerT) {
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
			writeKey := req.writeKey
			sourceTag := gateway.getSourceTagFromWriteKey(writeKey)
			if _, ok := sourceStats[sourceTag]; !ok {
				sourceStats[sourceTag] = gateway.NewSourceStat(writeKey, req.reqType)
			}
			jobData, err := gateway.getJobDataFromRequest(req)
			sourceStats[sourceTag].Version = jobData.version
			sourceStats[sourceTag].RequestEventsBot(jobData.botEvents)
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
			if len(jobData.jobs) > 0 {
				jobBatches = append(jobBatches, jobData.jobs)
				jobIDReqMap[jobData.jobs[0].UUID] = req
				jobSourceTagMap[jobData.jobs[0].UUID] = sourceTag
				for _, job := range jobData.jobs {
					eventBatchesToRecord = append(
						eventBatchesToRecord,
						sourceDebugger{
							data:     job.EventPayload,
							writeKey: writeKey,
						},
					)
				}
			} else {
				req.done <- response.InvalidJSON
				sourceStats[sourceTag].RequestFailed(response.InvalidJSON)
			}
		}

		errorMessagesMap := make(map[uuid.UUID]string)
		if len(jobBatches) > 0 {
			gateway.userWorkerBatchRequestQ <- &userWorkerBatchRequestT{
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
			gateway.sourcehandle.RecordEvent(eventBatch.writeKey, eventBatch.data)
		}

		userWebRequestWorker.batchTimeStat.Since(batchStart)
		gateway.batchSizeStat.Observe(float64(len(breq.batchRequest)))

		for _, v := range sourceStats {
			v.Report(gateway.stats)
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
func (gateway *Handle) getJobDataFromRequest(req *webRequestT) (jobData *jobFromReq, err error) {
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
		body, _ = sjson.SetRawBytes(batchEvent, "batch.0", body)
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

	if gateway.conf.enableRateLimit {
		// In case of "batch" requests, if rate-limiter returns true for LimitReached, just drop the event batch and continue.
		ok, errCheck := gateway.rateLimiter.CheckLimitReached(context.TODO(), workspaceId)
		if errCheck != nil {
			gateway.stats.NewTaggedStat("gateway.rate_limiter_error", stats.CountType, stats.Tags{"workspaceId": workspaceId}).Increment()
			gateway.logger.Errorf("Rate limiter error: %v Allowing the request", errCheck)
		}
		if ok {
			return jobData, errRequestDropped
		}
	}

	type jobObject struct {
		userID string
		events []map[string]interface{}
	}

	var (
		// map to hold modified/filtered events of the batch
		out []jobObject

		marshalledParams []byte
		// values retrieved from first event in batch
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

		anonIDFromReq := strings.TrimSpace(misc.GetStringifiedData(toSet["anonymousId"]))
		userIDFromReq := strings.TrimSpace(misc.GetStringifiedData(toSet["userId"]))
		eventTypeFromReq, _ := misc.MapLookup(
			toSet,
			"type",
		).(string)

		if gateway.isNonIdentifiable(anonIDFromReq, userIDFromReq, eventTypeFromReq) {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}

		eventContext, ok := misc.MapLookup(toSet, "context").(map[string]interface{})
		if ok {
			if idx == 0 {
				firstSourcesJobRunID, _ = misc.MapLookup(
					eventContext,
					"sources",
					"job_run_id",
				).(string)
				firstSourcesTaskRunID, _ = misc.MapLookup(
					eventContext,
					"sources",
					"task_run_id",
				).(string)

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
		var rudderId uuid.UUID
		rudderId, err = misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			err = errors.New(response.NonIdentifiableRequest)
			return
		}
		toSet["rudderId"] = rudderId
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

	if len(out) == 0 && suppressed {
		err = errRequestSuppressed
		return
	}

	if len(body) > gateway.conf.maxReqSize && !containsAudienceList {
		err = errors.New(response.RequestBodyTooLarge)
		return
	}

	params := map[string]interface{}{
		"source_id":          sourceID,
		"source_job_run_id":  firstSourcesJobRunID,
		"source_task_run_id": firstSourcesTaskRunID,
	}
	marshalledParams, err = json.Marshal(params)
	if err != nil {
		gateway.logger.Errorf(
			"[Gateway] Failed to marshal parameters map. Parameters: %+v",
			params,
		)
		marshalledParams = []byte(
			`{"error": "rudder-server gateway failed to marshal params"}`,
		)
	}
	if !gateway.conf.allowBatchSplitting {
		// instead of multiple jobs with one event, create one job with all events
		out = []jobObject{
			{
				userID: out[0].userID,
				events: lo.Map(out, func(userEvent jobObject, _ int) map[string]interface{} {
					return userEvent.events[0]
				}),
			},
		}
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
			singularEventBatch := SingularEventBatch{
				Batch:      userEvent.events,
				RequestIP:  ipAddr,
				WriteKey:   writeKey,
				ReceivedAt: time.Now().Format(misc.RFC3339Milli),
			}
			payload, err = json.Marshal(singularEventBatch)
			if err != nil {
				panic(err)
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

func (gateway *Handle) isNonIdentifiable(anonIDFromReq, userIDFromReq, eventType string) bool {
	if eventType == extractEvent {
		// extract event is allowed without user id and anonymous id
		return false
	}
	if anonIDFromReq == "" && userIDFromReq == "" {
		return !gateway.conf.allowReqsWithoutUserIDAndAnonymousID
	}
	return false
}

func buildUserID(userIDHeader, anonIDFromReq, userIDFromReq string) string {
	if anonIDFromReq != "" {
		return userIDHeader + delimiter + anonIDFromReq + delimiter + userIDFromReq
	}
	return userIDHeader + delimiter + userIDFromReq + delimiter + userIDFromReq
}

// memoizedIsUserSuppressed is a memoized version of isUserSuppressed
func (gateway *Handle) memoizedIsUserSuppressed() func(workspaceID, userID, sourceID string) bool {
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

// isUserSuppressed checks if the user is suppressed or not
func (gateway *Handle) isUserSuppressed(workspaceID, userID, sourceID string) bool {
	if !gateway.conf.enableSuppressUserFeature || gateway.suppressUserHandler == nil {
		return false
	}
	if metadata := gateway.suppressUserHandler.GetSuppressedUser(workspaceID, userID, sourceID); metadata != nil {
		if !metadata.CreatedAt.IsZero() {
			gateway.stats.NewTaggedStat("gateway.user_suppression_age", stats.TimerType, stats.Tags{
				"workspaceId": workspaceID,
				"sourceID":    sourceID,
			}).Since(metadata.CreatedAt)
		}
		return true
	}
	return false
}

// fillMessageID checks for the presence of messageId in the event
// and sets to a new uuid if not present
func fillMessageID(event map[string]interface{}) {
	messageID, _ := event["messageId"].(string)
	if strings.TrimSpace(messageID) == "" {
		event["messageId"] = uuid.New().String()
	}
}

// printStats prints a debug log containing received and acked events every 10 seconds
func (gateway *Handle) printStats(ctx context.Context) {
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

// ProcessWebRequest is an interface wrapper for webhook
func (gateway *Handle) ProcessWebRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	return gateway.rrh.ProcessRequest(w, r, reqType, payload, writeKey)
}

// getPayloadAndWriteKey reads the request body and returns the payload's bytes and write key or an error if the payload cannot be read
// or the write key is not present in the request
// TODO: split this function into two
func (gateway *Handle) getPayloadAndWriteKey(_ http.ResponseWriter, r *http.Request, reqType string) ([]byte, string, error) {
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

func (gateway *Handle) getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return []byte{}, errors.New(response.RequestBodyNil)
	}

	start := time.Now()
	defer gateway.bodyReadTimeStat.Since(start)

	payload, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		gateway.logger.Errorf(
			"Error reading request body, 'Content-Length': %s, partial payload:\n\t%s\n:%v",
			r.Header.Get("Content-Length"),
			string(payload),
			err,
		)
		return payload, errors.New(response.RequestBodyReadFailed)
	}
	return payload, nil
}

// getPayloadFromRequest reads the request body and returns event payloads grouped by user id
// for performance see: https://github.com/rudderlabs/rudder-server/pull/2040
func (*Handle) getUsersPayload(requestPayload []byte) (map[string][]byte, error) {
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

// TrackRequestMetrics updates the track counters (success and failure counts)
func (gateway *Handle) TrackRequestMetrics(errorMessage string) {
	if diagnostics.EnableGatewayMetric {
		gateway.trackCounterMu.Lock()
		defer gateway.trackCounterMu.Unlock()
		if errorMessage != "" {
			gateway.trackFailureCount = gateway.trackFailureCount + 1
		} else {
			gateway.trackSuccessCount = gateway.trackSuccessCount + 1
		}
	}
}

// collectMetrics collects the gateway metrics and sends them using diagnostics
func (gateway *Handle) collectMetrics(ctx context.Context) {
	if diagnostics.EnableGatewayMetric {
		for {
			select {
			case <-ctx.Done():
				return
			case <-gateway.diagnosisTicker.C:
				gateway.trackCounterMu.Lock()
				if gateway.trackSuccessCount > 0 || gateway.trackFailureCount > 0 {
					diagnostics.Diagnostics.Track(diagnostics.GatewayEvents, map[string]interface{}{
						diagnostics.GatewaySuccess: gateway.trackSuccessCount,
						diagnostics.GatewayFailure: gateway.trackFailureCount,
					})
					gateway.trackSuccessCount = 0
					gateway.trackFailureCount = 0
				}
				gateway.trackCounterMu.Unlock()
			}
		}
	}
}

// setWebPayload reads a pixel GET request and maps it to a proper payload in the request's body
func (*Handle) setWebPayload(r *http.Request, qp url.Values, reqType string) error {
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

/*
addToWebRequestQ finds the worker for a particular userID and queues the webrequest with the worker(pushes the req into the webRequestQ channel of the worker).
They are further batched together in userWebRequestBatcher
*/
func (gateway *Handle) addToWebRequestQ(_ *http.ResponseWriter, req *http.Request, done chan string, reqType string, requestPayload []byte, writeKey string) {
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
func (gateway *Handle) IncrementRecvCount(count uint64) {
	atomic.AddUint64(&gateway.recvCount, count)
}

// IncrementAckCount increments the acknowledged count for gateway requests
func (gateway *Handle) IncrementAckCount(count uint64) {
	atomic.AddUint64(&gateway.ackCount, count)
}

// GetWebhookSourceDefName returns the webhook source definition name by write key
func (gateway *Handle) GetWebhookSourceDefName(writeKey string) (name string, ok bool) {
	gateway.conf.configSubscriberLock.RLock()
	defer gateway.conf.configSubscriberLock.RUnlock()
	name, ok = gateway.conf.enabledWriteKeyWebhookMap[writeKey]
	return
}

// SaveErrors saves errors to the error db
func (gateway *Handle) SaveWebhookFailures(reqs []*model.FailedWebhookPayload) error {
	jobs := make([]*jobsdb.JobT, 0, len(reqs))
	for _, req := range reqs {
		params := map[string]interface{}{
			"source_id":   gateway.getSourceIDForWriteKey(req.WriteKey),
			"stage":       "webhook",
			"source_type": req.SourceType,
			"reason":      req.Reason,
		}
		marshalledParams, err := json.Marshal(params)
		if err != nil {
			gateway.logger.Errorf("[Gateway] Failed to marshal parameters map. Parameters: %+v", params)
			marshalledParams = []byte(`{"error": "rudder-server gateway failed to marshal params"}`)
		}

		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       uuid.New().String(), // Using a random userid for these failures. There is no notion of user id for these events.
			Parameters:   marshalledParams,
			CustomVal:    "WEBHOOK",
			EventPayload: req.Payload,
			EventCount:   1,
			WorkspaceId:  gateway.getWorkspaceForWriteKey(req.WriteKey),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), gateway.conf.WriteTimeout)
	defer cancel()
	return gateway.errDB.Store(ctx, jobs)
}

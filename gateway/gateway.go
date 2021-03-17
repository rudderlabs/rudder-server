package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
	"github.com/rudderlabs/rudder-server/services/diagnostics"

	"github.com/bugsnag/bugsnag-go"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/rruntime"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

/*
 * The gateway module handles incoming requests from client devices.
 * It batches web requests and writes to DB in bulk to improce I/O.
 * Only after the request payload is persisted, an ACK is sent to
 * the client.
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
	webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess int
	maxUserWebRequestBatchSize, maxDBBatchSize                  int
	userWebRequestBatchTimeout, dbBatchWriteTimeout             time.Duration
	enabledWriteKeysSourceMap                                   map[string]backendconfig.SourceT
	enabledWriteKeyWebhookMap                                   map[string]string
	sourceIDToNameMap                                           map[string]string
	configSubscriberLock                                        sync.RWMutex
	maxReqSize                                                  int
	enableRateLimit                                             bool
	enableSuppressUserFeature                                   bool
	enableEventSchemasFeature                                   bool
	diagnosisTickerTime                                         time.Duration
	allowReqsWithoutUserIDAndAnonymousID                        bool
	pkgLogger                                                   logger.LoggerI
	Diagnostics                                                 diagnostics.DiagnosticsI = diagnostics.Diagnostics
)

// CustomVal is used as a key in the jobsDB customval column
var CustomVal string

var BatchEvent = []byte(`
	{
		"batch": [
		]
	}
`)

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("gateway")
}

type userWorkerBatchRequestT struct {
	jobList     []*jobsdb.JobT
	respChannel chan map[uuid.UUID]string
}

type batchUserWorkerBatchRequestT struct {
	batchUserWorkerBatchRequest []*userWorkerBatchRequestT
}

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
	application                                                app.Interface
	userWorkerBatchRequestQ                                    chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ                               chan *batchUserWorkerBatchRequestT
	jobsDB                                                     jobsdb.JobsDB
	ackCount                                                   uint64
	recvCount                                                  uint64
	backendConfig                                              backendconfig.BackendConfig
	rateLimiter                                                ratelimiter.RateLimiter
	stats                                                      stats.Stats
	batchSizeStat                                              stats.RudderStats
	requestSizeStat                                            stats.RudderStats
	dbWritesStat                                               stats.RudderStats
	dbWorkersBufferFullStat, dbWorkersTimeOutStat              stats.RudderStats
	trackSuccessCount                                          int
	trackFailureCount                                          int
	requestMetricLock                                          sync.RWMutex
	diagnosisTicker                                            *time.Ticker
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
}

func (gateway *HandleT) updateSourceStats(sourceStats map[string]int, bucket string, sourceTagMap map[string]string) {
	for sourceTag, count := range sourceStats {
		tags := map[string]string{
			"source":   sourceTag,
			"writeKey": sourceTagMap[sourceTag],
		}
		sourceStatsD := gateway.stats.NewTaggedStat(bucket, stats.CountType, tags)
		sourceStatsD.Count(count)
	}
}

func (gateway *HandleT) initUserWebRequestWorkers() {
	gateway.userWebRequestWorkers = make([]*userWebRequestWorkerT, maxUserWebRequestWorkerProcess)
	for i := 0; i < maxUserWebRequestWorkerProcess; i++ {
		gateway.logger.Debug("User Web Request Worker Started", i)
		userWebRequestWorker := &userWebRequestWorkerT{
			webRequestQ:    make(chan *webRequestT, maxUserWebRequestBatchSize),
			batchRequestQ:  make(chan *batchWebRequestT),
			reponseQ:       make(chan map[uuid.UUID]string),
			workerID:       i,
			batchTimeStat:  gateway.stats.NewStat("gateway.batch_time", stats.TimerType),
			bufferFullStat: gateway.stats.NewStat(fmt.Sprintf("gateway.user_request_worker_%d_buffer_full", i), stats.CountType),
			timeOutStat:    gateway.stats.NewStat(fmt.Sprintf("gateway.user_request_worker_%d_time_out", i), stats.CountType),
		}
		gateway.userWebRequestWorkers[i] = userWebRequestWorker
		rruntime.Go(func() {
			gateway.userWebRequestWorkerProcess(userWebRequestWorker)
		})

		rruntime.Go(func() {
			gateway.userWebRequestBatcher(userWebRequestWorker)
		})
	}

}

func (gateway *HandleT) initDBWriterWorkers() {
	for i := 0; i < maxDBWriterProcess; i++ {
		gateway.logger.Debug("DB Writer Worker Started", i)
		j := i
		rruntime.Go(func() {
			gateway.dbWriterWorkerProcess(j)
		})
	}
}

func (gateway *HandleT) userWorkerRequestBatcher() {
	var userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
	timeout := time.After(dbBatchWriteTimeout)
	for {
		select {
		case userWorkerBatchRequest := <-gateway.userWorkerBatchRequestQ:

			//Append to request buffer
			userWorkerBatchRequestBuffer = append(userWorkerBatchRequestBuffer, userWorkerBatchRequest)
			if len(userWorkerBatchRequestBuffer) == maxDBBatchSize {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.dbWorkersBufferFullStat.Count(1)
				gateway.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = nil
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(dbBatchWriteTimeout)
			if len(userWorkerBatchRequestBuffer) > 0 {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.dbWorkersTimeOutStat.Count(1)
				gateway.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = nil
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		}
	}
}

func (gateway *HandleT) dbWriterWorkerProcess(process int) {
	gwAllowPartialWriteWithErrors := config.GetBool("Gateway.allowPartialWriteWithErrors", true)
	for breq := range gateway.batchUserWorkerBatchRequestQ {
		jobList := make([]*jobsdb.JobT, 0)
		var errorMessagesMap map[uuid.UUID]string

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			jobList = append(jobList, userWorkerBatchRequest.jobList...)
		}

		if gwAllowPartialWriteWithErrors {
			errorMessagesMap = gateway.jobsDB.StoreWithRetryEach(jobList)
		} else {
			gateway.jobsDB.Store(jobList)
		}
		gateway.dbWritesStat.Count(1)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			userWorkerBatchRequest.respChannel <- errorMessagesMap
		}
	}
}

func (gateway *HandleT) findUserWebRequestWorker(userID string) *userWebRequestWorkerT {

	index := int(math.Abs(float64(misc.GetHash(userID) % maxUserWebRequestWorkerProcess)))

	userWebRequestWorker := gateway.userWebRequestWorkers[index]
	if userWebRequestWorker == nil {
		panic(fmt.Errorf("worker is nil"))
	}

	return userWebRequestWorker
}

//Function to process the batch requests. It saves data in DB and
//sends and ACK on the done channel which unblocks the HTTP handler
func (gateway *HandleT) userWebRequestBatcher(userWebRequestWorker *userWebRequestWorkerT) {
	var reqBuffer = make([]*webRequestT, 0)
	timeout := time.After(userWebRequestBatchTimeout)
	for {
		select {
		case req := <-userWebRequestWorker.webRequestQ:

			//Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxUserWebRequestBatchSize {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				userWebRequestWorker.bufferFullStat.Count(1)
				userWebRequestWorker.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(userWebRequestBatchTimeout)
			if len(reqBuffer) > 0 {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				userWebRequestWorker.timeOutStat.Count(1)
				userWebRequestWorker.batchRequestQ <- &breq
				reqBuffer = nil
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
			misc.IncrementMapByKey(sourceStats, sourceTag, 1)

			ipAddr := req.ipAddr

			body := req.requestPayload

			if enableRateLimit {
				//In case of "batch" requests, if ratelimiter returns true for LimitReached, just drop the event batch and continue.
				restrictorKey := gateway.backendConfig.GetWorkspaceIDForWriteKey(writeKey)
				if gateway.rateLimiter.LimitReached(restrictorKey) {
					req.done <- response.GetStatus(response.TooManyRequests)
					preDbStoreCount++
					misc.IncrementMapByKey(workspaceDropRequestStats, restrictorKey, 1)
					continue
				}
			}

			if !gjson.ValidBytes(body) {
				req.done <- response.GetStatus(response.InvalidJSON)
				preDbStoreCount++
				misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
				continue
			}
			gateway.requestSizeStat.Count(len(body))
			if req.reqType != "batch" {
				body, _ = sjson.SetBytes(body, "type", req.reqType)
				body, _ = sjson.SetRawBytes(BatchEvent, "batch.0", body)
			}
			totalEventsInReq := len(gjson.GetBytes(body, "batch").Array())
			misc.IncrementMapByKey(sourceEventStats, sourceTag, totalEventsInReq)
			if len(body) > maxReqSize {
				req.done <- response.GetStatus(response.RequestBodyTooLarge)
				preDbStoreCount++
				misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
				misc.IncrementMapByKey(sourceFailEventStats, sourceTag, totalEventsInReq)
				continue
			}

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

			// set anonymousId if not set in payload
			var index int
			result := gjson.GetBytes(body, "batch")

			var notIdentifiable bool
			result.ForEach(func(_, _ gjson.Result) bool {
				anonIDFromReq := strings.TrimSpace(gjson.GetBytes(body, fmt.Sprintf(`batch.%v.anonymousId`, index)).String())
				userIDFromReq := strings.TrimSpace(gjson.GetBytes(body, fmt.Sprintf(`batch.%v.userId`, index)).String())
				if anonIDFromReq == "" {
					if userIDFromReq == "" && !allowReqsWithoutUserIDAndAnonymousID {
						notIdentifiable = true
						return false
					}
				}
				// hashing combination of userIDFromReq + anonIDFromReq, using colon as a delimiter
				rudderId, err := misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
				if err != nil {
					notIdentifiable = true
					return false
				}
				body, _ = sjson.SetBytes(body, fmt.Sprintf(`batch.%v.rudderId`, index), rudderId)
				if strings.TrimSpace(gjson.GetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index)).String()) == "" {
					body, _ = sjson.SetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index), uuid.NewV4().String())
				}
				index++
				return true // keep iterating
			})

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

			gateway.logger.Debug("IP address is ", ipAddr)
			body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
			body, _ = sjson.SetBytes(body, "writeKey", writeKey)
			body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(misc.RFC3339Milli))
			eventBatchesToRecord = append(eventBatchesToRecord, fmt.Sprintf("%s", body))

			id := uuid.NewV4()
			//Should be function of body
			newJob := jobsdb.JobT{
				UUID:         id,
				UserID:       gjson.GetBytes(body, "batch.0.rudderId").Str,
				Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v", "batch_id": %d}`, sourceID, counter)),
				CustomVal:    CustomVal,
				EventPayload: []byte(body),
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
		gateway.batchSizeStat.Count(len(breq.batchRequest))
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
	return misc.Contains(enabledWriteKeysSourceMap, writeKey)
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

func (gateway *HandleT) printStats() {
	for {
		time.Sleep(10 * time.Second)
		recvCount := atomic.LoadUint64(&gateway.recvCount)
		ackCount := atomic.LoadUint64(&gateway.ackCount)
		gateway.logger.Debug("Gateway Recv/Ack ", recvCount, ackCount)
	}
}

func (gateway *HandleT) stat(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		latencyStat := gateway.stats.NewSampledTaggedStat("gateway.response_time", stats.TimerType, stats.Tags{})
		latencyStat.Start()
		wrappedFunc(w, r)
		latencyStat.End()
	}
}

func (gateway *HandleT) eventSchemaWebHandler(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !enableEventSchemasFeature {
			gateway.logger.Debug("EventSchemas feature is disabled. You can enabled it through enableEventSchemasFeature flag in config.toml")
			http.Error(w, response.MakeResponse("EventSchemas feature is disabled"), 400)
			return
		}
		wrappedFunc(w, r)
	}
}

func (gateway *HandleT) getPayloadFromRequest(r *http.Request) ([]byte, error) {
	if r.Body != nil {
		payload, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		return payload, err
	}
	return []byte{}, errors.New(response.RequestBodyNil)
}

func (gateway *HandleT) webImportHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webRequestHandler(gateway.irh, w, r, "import")
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
}

func (gateway *HandleT) pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gateway.logger.Debug(errorMessage)
			http.Error(w, response.GetStatus(errorMessage), 400)
		}
	}()

	payload, _, err := gateway.getPayloadAndWriteKey(w, r)
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

	var excludeGateway bool
	excludeGatewayStr := r.URL.Query().Get("exclude_gateway")
	if excludeGatewayStr != "" {
		val, err := strconv.ParseBool(excludeGatewayStr)
		if err == nil {
			excludeGateway = val
		}
	}

	var gwPendingCount, rtPendingCount int64
	if !excludeGateway {
		gwPendingCount = gateway.readonlyGatewayDB.GetPendingJobsCount([]string{CustomVal}, -1, gwParameterFilters)
	}
	rtPendingCount = gateway.readonlyRouterDB.GetPendingJobsCount(nil, -1, rtParameterFilters)
	//brtPendingCount := gateway.readonlyBatchRouterDB.GetPendingJobsCount(nil, -1, nil)

	totalPendingCount := gwPendingCount + rtPendingCount

	w.Write([]byte(fmt.Sprintf("{ \"pending_events\": %d }", totalPendingCount)))
}

//ProcessRequest throws a webRequest into the queue and waits for the response before returning
func (rrh *RegularRequestHandler) ProcessRequest(gateway *HandleT, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	done := make(chan string, 1)
	gateway.addToWebRequestQ(w, r, done, reqType, payload, writeKey)
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

func (gateway *HandleT) getPayloadAndWriteKey(w http.ResponseWriter, r *http.Request) ([]byte, string, error) {
	var sourceFailStats = make(map[string]int)
	var err error
	writeKey, _, ok := r.BasicAuth()
	if !ok || writeKey == "" {
		err = errors.New(response.NoWriteKeyInBasicAuth)
		misc.IncrementMapByKey(sourceFailStats, "noWriteKey", 1)
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_failed_requests", map[string]string{"noWriteKey": "noWriteKey"})
		return []byte{}, "", err
	}
	payload, err := gateway.getPayloadFromRequest(r)
	if err != nil {
		sourceTag := gateway.getSourceTagFromWriteKey(writeKey)
		misc.IncrementMapByKey(sourceFailStats, sourceTag, 1)
		gateway.updateSourceStats(sourceFailStats, "gateway.write_key_failed_requests", map[string]string{sourceTag: writeKey})
		return []byte{}, writeKey, err
	}
	return payload, writeKey, err
}

func (gateway *HandleT) webHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.webRequestHandler(gateway.rrh, w, r, reqType)
}

func (gateway *HandleT) webRequestHandler(rh RequestHandler, w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	var errorMessage string
	defer func() {
		if errorMessage != "" {
			gateway.logger.Debug(errorMessage)
			http.Error(w, response.GetStatus(errorMessage), 400)
		}
	}()
	payload, writeKey, err := gateway.getPayloadAndWriteKey(w, r)
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
	gateway.logger.Debug(response.GetStatus(response.Ok))
	w.Write([]byte(response.GetStatus(response.Ok)))
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

func (gateway *HandleT) collectMetrics() {
	if diagnostics.EnableGatewayMetric {
		for {
			select {
			case <-gateway.diagnosisTicker.C:
				gateway.requestMetricLock.RLock()
				if gateway.trackSuccessCount > 0 || gateway.trackFailureCount > 0 {
					Diagnostics.Track(diagnostics.GatewayEvents, map[string]interface{}{
						diagnostics.GatewaySuccess: gateway.trackSuccessCount,
						diagnostics.GatewayFailure: gateway.trackFailureCount,
					})
					gateway.trackSuccessCount = 0
					gateway.trackFailureCount = 0
				}
				gateway.requestMetricLock.RUnlock()
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
	r.Body = ioutil.NopCloser(bytes.NewReader(body))
	return nil
}

func (gateway *HandleT) pixelHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	if r.Method == http.MethodGet {
		queryParams := r.URL.Query()
		if writeKey, present := queryParams["writeKey"]; present && writeKey[0] != "" {
			// make a new request
			req, _ := http.NewRequest(http.MethodPost, "", nil)

			// set basic auth header
			req.SetBasicAuth(writeKey[0], "")
			delete(queryParams, "writeKey")

			// set X-Forwarded-For header
			req.Header.Add("X-Forwarded-For", r.Header.Get("X-Forwarded-For"))

			// convert the pixel request(r) to a web request(req)
			err := gateway.setWebPayload(req, queryParams, reqType)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			// send req to webHandler
			gateway.webHandler(w, req, reqType)
		} else {
			http.Error(w, response.NoWriteKeyInQueryParams, http.StatusUnauthorized)
		}
	} else {
		http.Error(w, response.InvalidRequestMethod, http.StatusBadRequest)
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
func (gateway *HandleT) StartWebHandler() {

	gateway.logger.Infof("Starting in %d", webPort)
	srvMux := mux.NewRouter()
	srvMux.Use(headerMiddleware)
	srvMux.HandleFunc("/v1/batch", gateway.stat(gateway.webBatchHandler))
	srvMux.HandleFunc("/v1/identify", gateway.stat(gateway.webIdentifyHandler))
	srvMux.HandleFunc("/v1/track", gateway.stat(gateway.webTrackHandler))
	srvMux.HandleFunc("/v1/page", gateway.stat(gateway.webPageHandler))
	srvMux.HandleFunc("/v1/screen", gateway.stat(gateway.webScreenHandler))
	srvMux.HandleFunc("/v1/alias", gateway.stat(gateway.webAliasHandler))
	srvMux.HandleFunc("/v1/merge", gateway.stat(gateway.webMergeHandler))
	srvMux.HandleFunc("/v1/group", gateway.stat(gateway.webGroupHandler))
	srvMux.HandleFunc("/health", gateway.healthHandler)
	srvMux.HandleFunc("/v1/import", gateway.stat(gateway.webImportHandler))
	srvMux.HandleFunc("/", gateway.healthHandler)
	srvMux.HandleFunc("/pixel/v1/track", gateway.stat(gateway.pixelTrackHandler))
	srvMux.HandleFunc("/pixel/v1/page", gateway.stat(gateway.pixelPageHandler))
	srvMux.HandleFunc("/version", gateway.versionHandler)
	srvMux.HandleFunc("/v1/webhook", gateway.stat(gateway.webhookHandler.RequestHandler))
	srvMux.HandleFunc("/beacon/v1/batch", gateway.stat(gateway.beaconBatchHandler))

	if enableEventSchemasFeature {
		srvMux.HandleFunc("/schemas/event-models", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventModels))
		srvMux.HandleFunc("/schemas/event-versions", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventVersions))
		srvMux.HandleFunc("/schemas/event-model/{EventID}/key-counts", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetKeyCounts))
		srvMux.HandleFunc("/schemas/event-model/{EventID}/metadata", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetEventModelMetadata))
		srvMux.HandleFunc("/schemas/event-version/{VersionID}/metadata", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetSchemaVersionMetadata))
		srvMux.HandleFunc("/schemas/event-version/{VersionID}/missing-keys", gateway.eventSchemaWebHandler(gateway.eventSchemaHandler.GetSchemaVersionMissingKeys))
	}

	srvMux.HandleFunc("/v1/pending-events", gateway.stat(gateway.pendingEventsHandler))

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
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(webPort),
		Handler:           c.Handler(bugsnag.Handler(srvMux)),
		ReadTimeout:       config.GetDuration("ReadTimeOutInSec", 0*time.Second),
		ReadHeaderTimeout: config.GetDuration("ReadHeaderTimeoutInSec", 0*time.Second),
		WriteTimeout:      config.GetDuration("WriteTimeOutInSec", 10*time.Second),
		IdleTimeout:       config.GetDuration("IdleTimeoutInSec", 720*time.Second),
		MaxHeaderBytes:    config.GetInt("MaxHeaderBytes", 524288),
	}
	gateway.logger.Fatal(srv.ListenAndServe())
}

//Currently sets the content-type only for eventSchemas, health responses.
//Note : responses via http.Error aren't affected. They default to text/plain
func headerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/schemas") || strings.HasPrefix(r.URL.Path, "/health") {
			w.Header().Add("Content-Type", "application/json; charset=utf-8")
		}
		next.ServeHTTP(w, r)
	})
}

// Gets the config from config backend and extracts enabled writekeys
func (gateway *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	gateway.backendConfig.Subscribe(ch, backendconfig.TopicProcessConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		enabledWriteKeysSourceMap = map[string]backendconfig.SourceT{}
		enabledWriteKeyWebhookMap = map[string]string{}
		sources := config.Data.(backendconfig.ConfigT)
		sourceIDToNameMap = map[string]string{}
		for _, source := range sources.Sources {
			sourceIDToNameMap[source.ID] = source.Name
			if source.Enabled {
				enabledWriteKeysSourceMap[source.WriteKey] = source
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

// addToWebRequestQ provides access to add a request to the gateway's webRequestQ
func (gateway *HandleT) addToWebRequestQ(writer *http.ResponseWriter, req *http.Request, done chan string, reqType string, requestPayload []byte, writeKey string) {
	userIDHeader := req.Header.Get("AnonymousId")
	//If necessary fetch userID from request body.
	if userIDHeader == "" {
		//If the request comes through proxy, proxy would already send this. So this shouldn't be happening in that case
		userIDHeader = uuid.NewV4().String()
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

	gateway.batchSizeStat = gateway.stats.NewStat("gateway.batch_size", stats.CountType)
	gateway.requestSizeStat = gateway.stats.NewStat("gateway.request_size", stats.CountType)
	gateway.dbWritesStat = gateway.stats.NewStat("gateway.db_writes", stats.CountType)
	gateway.dbWorkersBufferFullStat = gateway.stats.NewStat("gateway.db_workers_buffer_full", stats.CountType)
	gateway.dbWorkersTimeOutStat = gateway.stats.NewStat("gateway.db_workers_time_out", stats.CountType)

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

	if gateway.application.Features().SuppressUser != nil {
		gateway.suppressUserHandler = application.Features().SuppressUser.Setup(gateway.backendConfig)
	}

	if enableEventSchemasFeature {
		gateway.eventSchemaHandler = event_schema.GetInstance()
	}

	rruntime.Go(func() {
		gateway.backendConfigSubscriber()
	})

	gateway.initUserWebRequestWorkers()
	rruntime.Go(func() {
		gateway.userWorkerRequestBatcher()
	})
	gateway.initDBWriterWorkers()

	gateway.backendConfig.WaitForConfig()
	rruntime.Go(func() {
		gateway.printStats()
	})
	rruntime.Go(func() {
		gateway.collectMetrics()
	})
}

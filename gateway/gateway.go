package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"sort"
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
	"github.com/dgraph-io/badger"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
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
	request *http.Request
	writer  *http.ResponseWriter
	done    chan<- string
	reqType string
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

var (
	webPort, maxUserWebRequestWorkerProcess, maxDBWriterProcess int
	maxUserWebRequestBatchSize, maxDBBatchSize                  int
	userWebRequestBatchTimeout, dbBatchWriteTimeout             time.Duration
	enabledWriteKeysSourceMap                                   map[string]string
	enabledWriteKeyWebhookMap                                   map[string]string
	configSubscriberLock                                        sync.RWMutex
	maxReqSize                                                  int
	enableDedup                                                 bool
	enableRateLimit                                             bool
	enableSuppressUserFeature                                   bool
	enableEventSchemasFeature                                   bool
	dedupWindow, diagnosisTickerTime                            time.Duration
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
	jobList          []*jobsdb.JobT
	allMessageIdsSet map[string]struct{}
	respChannel      chan map[uuid.UUID]string
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
	application                                   app.Interface
	webRequestQ                                   chan *webRequestT
	userWorkerBatchRequestQ                       chan *userWorkerBatchRequestT
	batchUserWorkerBatchRequestQ                  chan *batchUserWorkerBatchRequestT
	jobsDB                                        jobsdb.JobsDB
	badgerDB                                      *badger.DB
	ackCount                                      uint64
	recvCount                                     uint64
	backendConfig                                 backendconfig.BackendConfig
	rateLimiter                                   ratelimiter.RateLimiter
	stats                                         stats.Stats
	batchSizeStat                                 stats.RudderStats
	dbWritesStat                                  stats.RudderStats
	dbWorkersBufferFullStat, dbWorkersTimeOutStat stats.RudderStats
	trackSuccessCount                             int
	trackFailureCount                             int
	requestMetricLock                             sync.RWMutex
	diagnosisTicker                               *time.Ticker
	webRequestBatchCount                          uint64
	userWebRequestWorkers                         []*userWebRequestWorkerT
	webhookHandler                                *webhook.HandleT
	suppressUserHandler                           types.SuppressUserI
	eventSchemaHandler                            types.EventSchemasI
	versionHandler                                func(w http.ResponseWriter, r *http.Request)
	logger                                        logger.LoggerI
}

func (gateway *HandleT) updateWriteKeyStats(writeKeyStats map[string]int, bucket string) {
	for writeKey, count := range writeKeyStats {
		writeKeyStatsD := gateway.stats.NewTaggedStat(bucket, stats.CountType, map[string]string{
			"writekey" : writeKey,
		})
		writeKeyStatsD.Count(count)
	}
}

func (gateway *HandleT) initUserWebRequestWorkers() {
	gateway.userWebRequestWorkers = make([]*userWebRequestWorkerT, maxUserWebRequestWorkerProcess)
	for i := 0; i < maxUserWebRequestWorkerProcess; i++ {
		gateway.logger.Debug("User Web Request Worker Started", i)
		var userWebRequestWorker *userWebRequestWorkerT
		userWebRequestWorker = &userWebRequestWorkerT{
			webRequestQ:    make(chan *webRequestT, maxUserWebRequestBatchSize),
			batchRequestQ:  make(chan *batchWebRequestT),
			reponseQ:       make(chan map[uuid.UUID]string),
			workerID:       i,
			batchTimeStat:  gateway.stats.NewBatchStat("gateway.batch_time", stats.TimerType, i),
			bufferFullStat: gateway.stats.NewBatchStat(fmt.Sprintf("gateway.user_request_worker_%d_buffer_full", i), stats.CountType, i),
			timeOutStat:    gateway.stats.NewBatchStat(fmt.Sprintf("gateway.user_request_worker_%d_time_out", i), stats.CountType, i),
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
		messageIdsArr := make([]string, 0)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			jobList = append(jobList, userWorkerBatchRequest.jobList...)

			for k := range userWorkerBatchRequest.allMessageIdsSet {
				messageIdsArr = append(messageIdsArr, k)
			}
		}

		if gwAllowPartialWriteWithErrors {
			errorMessagesMap = gateway.jobsDB.StoreWithRetryEach(jobList)
		} else {
			gateway.jobsDB.Store(jobList)
		}
		gateway.dbWritesStat.Count(1)

		gateway.writeToBadger(messageIdsArr)

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

func (gateway *HandleT) userWebRequestWorkerProcess(userWebRequestWorker *userWebRequestWorkerT) {
	for breq := range userWebRequestWorker.batchRequestQ {
		counter := atomic.AddUint64(&gateway.webRequestBatchCount, 1)
		var jobList []*jobsdb.JobT
		var jobIDReqMap = make(map[uuid.UUID]*webRequestT)
		var jobWriteKeyMap = make(map[uuid.UUID]string)
		var jobEventCountMap = make(map[uuid.UUID]int)
		var writeKeyStats = make(map[string]int)
		var writeKeyEventStats = make(map[string]int)
		var writeKeyDupStats = make(map[string]int)
		var writeKeySuccessStats = make(map[string]int)
		var writeKeySuccessEventStats = make(map[string]int)
		var writeKeyFailStats = make(map[string]int)
		var writeKeyFailEventStats = make(map[string]int)
		var workspaceDropRequestStats = make(map[string]int)
		var preDbStoreCount int
		//Saving the event data read from req.request.Body to the splice.
		//Using this to send event schema to the config backend.
		var eventBatchesToRecord []string
		userWebRequestWorker.batchTimeStat.Start()
		allMessageIdsSet := make(map[string]struct{})
		for _, req := range breq.batchRequest {
			writeKey, _, ok := req.request.BasicAuth()
			misc.IncrementMapByKey(writeKeyStats, writeKey, 1)
			if !ok || writeKey == "" {
				req.done <- response.GetStatus(response.NoWriteKeyInBasicAuth)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, "noWriteKey", 1)
				continue
			}

			ipAddr := misc.GetIPFromReq(req.request)
			if req.request.Body == nil {
				req.done <- response.GetStatus(response.RequestBodyNil)
				preDbStoreCount++
				continue
			}
			body, err := ioutil.ReadAll(req.request.Body)
			req.request.Body.Close()

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

			if err != nil {
				req.done <- response.GetStatus(response.RequestBodyReadFailed)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				continue
			}
			if !gjson.ValidBytes(body) {
				req.done <- response.GetStatus(response.InvalidJSON)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				continue
			}
			if req.reqType != "batch" {
				body, _ = sjson.SetBytes(body, "type", req.reqType)
				body, _ = sjson.SetRawBytes(BatchEvent, "batch.0", body)
			}
			totalEventsInReq := len(gjson.GetBytes(body, "batch").Array())
			misc.IncrementMapByKey(writeKeyEventStats, writeKey, totalEventsInReq)
			if len(body) > maxReqSize {
				req.done <- response.GetStatus(response.RequestBodyTooLarge)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				misc.IncrementMapByKey(writeKeyFailEventStats, writeKey, totalEventsInReq)
				continue
			}

			// store sourceID before call made to check if source is enabled
			// this prevents not setting sourceID in gw job if disabled before setting it
			sourceID := gateway.getSourceIDForWriteKey(writeKey)
			if !gateway.isWriteKeyEnabled(writeKey) {
				req.done <- response.GetStatus(response.InvalidWriteKey)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				misc.IncrementMapByKey(writeKeyFailEventStats, writeKey, totalEventsInReq)
				continue
			}

			// set anonymousId if not set in payload
			var index int
			result := gjson.GetBytes(body, "batch")

			var reqMessageIDs []string
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
				if enableDedup {
					reqMessageIDs = append(reqMessageIDs, gjson.GetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index)).String())
				}
				index++
				return true // keep iterating
			})

			if notIdentifiable {
				req.done <- response.GetStatus(response.NonIdentifiableRequest)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, "notIdentifiable", 1)
				continue
			}

			if enableDedup {
				gateway.dedup(&body, reqMessageIDs, allMessageIdsSet, writeKey, writeKeyDupStats)
				addToSet(allMessageIdsSet, reqMessageIDs)
				if len(gjson.GetBytes(body, "batch").Array()) == 0 {
					req.done <- ""
					preDbStoreCount++
					misc.IncrementMapByKey(writeKeySuccessEventStats, writeKey, totalEventsInReq)
					continue
				}
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
			jobWriteKeyMap[newJob.UUID] = writeKey
			jobEventCountMap[newJob.UUID] = totalEventsInReq
		}

		errorMessagesMap := make(map[uuid.UUID]string)
		if len(jobList) > 0 {
			gateway.userWorkerBatchRequestQ <- &userWorkerBatchRequestT{jobList: jobList,
				allMessageIdsSet: allMessageIdsSet,
				respChannel:      userWebRequestWorker.reponseQ,
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
				misc.IncrementMapByKey(writeKeyFailStats, jobWriteKeyMap[job.UUID], 1)
				misc.IncrementMapByKey(writeKeyFailEventStats, jobWriteKeyMap[job.UUID], jobEventCountMap[job.UUID])
			} else {
				misc.IncrementMapByKey(writeKeySuccessStats, jobWriteKeyMap[job.UUID], 1)
				misc.IncrementMapByKey(writeKeySuccessEventStats, jobWriteKeyMap[job.UUID], jobEventCountMap[job.UUID])
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
		gateway.updateWriteKeyStats(writeKeyStats, "gateway.write_key_requests")
		gateway.updateWriteKeyStats(writeKeySuccessStats, "gateway.write_key_successful_requests")
		gateway.updateWriteKeyStats(writeKeyFailStats, "gateway.write_key_failed_requests")
		if enableRateLimit {
			gateway.updateWriteKeyStats(workspaceDropRequestStats, "gateway.work_space_dropped_requests")
		}
		// update stats event wise
		gateway.updateWriteKeyStats(writeKeyEventStats, "gateway.write_key_events")
		gateway.updateWriteKeyStats(writeKeySuccessEventStats, "gateway.write_key_successful_events")
		gateway.updateWriteKeyStats(writeKeyFailEventStats, "gateway.write_key_failed_events")
		if enableDedup {
			gateway.updateWriteKeyStats(writeKeyDupStats, "gateway.write_key_duplicate_events")
		}
	}
}

func addToSet(set map[string]struct{}, elements []string) {
	for _, element := range elements {
		set[element] = struct{}{}
	}
}

func (gateway *HandleT) dedup(body *[]byte, messageIDs []string, allMessageIDsSet map[string]struct{}, writeKey string, writeKeyDupStats map[string]int) {
	toRemoveMessageIndexesSet := make(map[int]struct{})
	//Dedup within events batch in a web request
	messageIDSet := make(map[string]struct{})

	// Eg messageIDs: [m1, m2, m3, m1, m1, m1]
	//Constructing a set out of messageIDs
	for _, messageID := range messageIDs {
		messageIDSet[messageID] = struct{}{}
	}
	// Eg messagIDSet: [m1, m2, m3]
	//In this loop it will remove from set for first occurance and if not found in set it means its a duplicate
	for idx, messageID := range messageIDs {
		if _, ok := messageIDSet[messageID]; ok {
			delete(messageIDSet, messageID)
		} else {
			toRemoveMessageIndexesSet[idx] = struct{}{}
		}
	}

	//Dedup within batch of web requests
	for idx, messageID := range messageIDs {
		if _, ok := allMessageIDsSet[messageID]; ok {
			toRemoveMessageIndexesSet[idx] = struct{}{}
		}
	}

	//Dedup with badgerDB
	err := gateway.badgerDB.View(func(txn *badger.Txn) error {
		for idx, messageID := range messageIDs {
			_, err := txn.Get([]byte(messageID))
			if err != badger.ErrKeyNotFound {
				toRemoveMessageIndexesSet[idx] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	toRemoveMessageIndexes := make([]int, 0, len(toRemoveMessageIndexesSet))
	for k := range toRemoveMessageIndexesSet {
		toRemoveMessageIndexes = append(toRemoveMessageIndexes, k)
	}

	sort.Ints(toRemoveMessageIndexes)
	count := 0
	for _, idx := range toRemoveMessageIndexes {
		gateway.logger.Debugf("Dropping event with duplicate messageId: %s", messageIDs[idx])
		misc.IncrementMapByKey(writeKeyDupStats, writeKey, 1)
		*body, err = sjson.DeleteBytes(*body, fmt.Sprintf(`batch.%v`, idx-count))
		if err != nil {
			panic(err)
		}
		count++
	}
}

func (gateway *HandleT) writeToBadger(messageIDs []string) {
	if enableDedup {
		err := gateway.badgerDB.Update(func(txn *badger.Txn) error {
			for _, messageID := range messageIDs {
				e := badger.NewEntry([]byte(messageID), nil).WithTTL(dedupWindow * time.Second)
				if err := txn.SetEntry(e); err == badger.ErrTxnTooBig {
					_ = txn.Commit()
					txn = gateway.badgerDB.NewTransaction(true)
					_ = txn.SetEntry(e)
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}

func (gateway *HandleT) isWriteKeyEnabled(writeKey string) bool {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !misc.Contains(enabledWriteKeysSourceMap, writeKey) {
		return false
	}
	return true
}

func (gateway *HandleT) getSourceIDForWriteKey(writeKey string) string {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if _, ok := enabledWriteKeysSourceMap[writeKey]; ok {
		return enabledWriteKeysSourceMap[writeKey]
	}

	return ""
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
		latencyStat := gateway.stats.NewTaggedStat("gateway.response_time", stats.TimerType, map[string]string{})
		latencyStat.Start()
		wrappedFunc(w, r)
		latencyStat.End()
	}
}

func (gateway *HandleT) eventSchemaWebHandler(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !enableEventSchemasFeature {
			gateway.logger.Debug("EventSchemas feature is disabled. You can enabled it through enableEventSchemasFeature flag in config.toml")
			http.Error(w, "EventSchemas feature is disabled", 400)
			return
		}
		wrappedFunc(w, r)
	}
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

func (gateway *HandleT) webHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	gateway.logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	done := make(chan string, 1)
	gateway.AddToWebRequestQ(r, &w, done, reqType)

	//Wait for batcher process to be done
	errorMessage := <-done
	atomic.AddUint64(&gateway.ackCount, 1)
	gateway.trackRequestMetrics(errorMessage)
	if errorMessage != "" {
		gateway.logger.Debug(errorMessage)
		http.Error(w, errorMessage, 400)
	} else {
		gateway.logger.Debug(response.GetStatus(response.Ok))
		w.Write([]byte(response.GetStatus(response.Ok)))
	}
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
			case _ = <-gateway.diagnosisTicker.C:
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
	var dbService string = "UP"
	var enabledRouter string = "TRUE"
	var backendConfigMode string = "API"
	if !gateway.jobsDB.CheckPGHealth() {
		dbService = "DOWN"
	}
	if !config.GetBool("enableRouter", true) {
		enabledRouter = "FALSE"
	}
	if config.GetBool("BackendConfig.configFromFile", false) {
		backendConfigMode = "JSON"
	}

	healthVal := fmt.Sprintf(`{"server":"UP", "db":"%s","acceptingEvents":"TRUE","routingEvents":"%s","mode":"%s","goroutines":"%d", "backendConfigMode": "%s", "lastSync":"%s", "lastRegulationSync":"%s"}`, dbService, enabledRouter, strings.ToUpper(db.CurrentMode), runtime.NumGoroutine(), backendConfigMode, backendconfig.LastSync, backendconfig.LastRegulationSync)
	w.Write([]byte(healthVal))
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
	srvMux.HandleFunc("/v1/batch", gateway.stat(gateway.webBatchHandler))
	srvMux.HandleFunc("/v1/identify", gateway.stat(gateway.webIdentifyHandler))
	srvMux.HandleFunc("/v1/track", gateway.stat(gateway.webTrackHandler))
	srvMux.HandleFunc("/v1/page", gateway.stat(gateway.webPageHandler))
	srvMux.HandleFunc("/v1/screen", gateway.stat(gateway.webScreenHandler))
	srvMux.HandleFunc("/v1/alias", gateway.stat(gateway.webAliasHandler))
	srvMux.HandleFunc("/v1/merge", gateway.stat(gateway.webMergeHandler))
	srvMux.HandleFunc("/v1/group", gateway.stat(gateway.webGroupHandler))
	srvMux.HandleFunc("/health", gateway.healthHandler)
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
	log.Fatal(srv.ListenAndServe())
}

// Gets the config from config backend and extracts enabled writekeys
func (gateway *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	gateway.backendConfig.Subscribe(ch, backendconfig.TopicProcessConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		enabledWriteKeysSourceMap = map[string]string{}
		enabledWriteKeyWebhookMap = map[string]string{}
		sources := config.Data.(backendconfig.SourcesT)
		for _, source := range sources.Sources {
			if source.Enabled {
				enabledWriteKeysSourceMap[source.WriteKey] = source.ID
				if source.SourceDefinition.Category == "webhook" {
					enabledWriteKeyWebhookMap[source.WriteKey] = source.SourceDefinition.Name
					gateway.webhookHandler.Register(source.SourceDefinition.Name)
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (gateway *HandleT) gcBadgerDB() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := gateway.badgerDB.RunValueLogGC(0.5)
		if err == nil {
			goto again
		}
	}
}

func (gateway *HandleT) openBadger(clearDB *bool) {
	var err error
	badgerPathName := "/badgerdb"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	path := fmt.Sprintf(`%v%v`, tmpDirPath, badgerPathName)
	gateway.badgerDB, err = badger.Open(badger.DefaultOptions(path))
	if err != nil {
		panic(err)
	}
	if *clearDB {
		err = gateway.badgerDB.DropAll()
		if err != nil {
			panic(err)
		}
	}
	rruntime.Go(func() {
		gateway.gcBadgerDB()
	})
}

/*
Public methods on GatewayWebhookI
*/

// AddToWebRequestQ provides access to add a request to the gateway's webRequestQ
func (gateway *HandleT) AddToWebRequestQ(req *http.Request, writer *http.ResponseWriter, done chan string, reqType string) {

	userIDHeader := req.Header.Get("AnonymousId")
	//If necessary fetch userID from request body.
	if userIDHeader == "" {
		//If the request comes through proxy, proxy would already send this. So this shouldn't be happening in that case
		userIDHeader = uuid.NewV4().String()
	}
	userWebRequestWorker := gateway.findUserWebRequestWorker(userIDHeader)

	webReq := webRequestT{request: req, writer: writer, done: done, reqType: reqType}
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

// UpdateWriteKeyStats creates a new stat for every writekey and updates it with the corresponding count
func (gateway *HandleT) UpdateWriteKeyStats(writeKeyStats map[string]int, bucket string) {
	gateway.updateWriteKeyStats(writeKeyStats, bucket)
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

/*
Setup initializes this module:
- Monitors backend config for changes.
- Starts web request batching goroutine, that batches incoming messages.
- Starts web request batch db writer goroutine, that writes incoming batches to JobsDB.
- Starts debugging goroutine that prints gateway stats.

This function will block until backend config is initialy received.
*/
func (gateway *HandleT) Setup(application app.Interface, backendConfig backendconfig.BackendConfig, jobsDB jobsdb.JobsDB, rateLimiter ratelimiter.RateLimiter, s stats.Stats, clearDB *bool, versionHandler func(w http.ResponseWriter, r *http.Request)) {
	gateway.logger = pkgLogger
	gateway.application = application
	gateway.stats = s

	gateway.diagnosisTicker = time.NewTicker(diagnosisTickerTime)

	gateway.batchSizeStat = gateway.stats.NewStat("gateway.batch_size", stats.CountType)
	gateway.dbWritesStat = gateway.stats.NewStat("gateway.db_writes", stats.CountType)
	gateway.dbWorkersBufferFullStat = gateway.stats.NewStat("gateway.db_workers_buffer_full", stats.CountType)
	gateway.dbWorkersTimeOutStat = gateway.stats.NewStat("gateway.db_workers_time_out", stats.CountType)

	if enableDedup {
		gateway.openBadger(clearDB)
	}
	gateway.backendConfig = backendConfig
	gateway.rateLimiter = rateLimiter
	gateway.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, maxDBBatchSize)
	gateway.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, maxDBWriterProcess)
	gateway.jobsDB = jobsDB

	gateway.versionHandler = versionHandler

	gateway.webhookHandler = webhook.Setup(gateway)

	admin.RegisterStatusHandler("Gateway", &GatewayAdmin{handle: gateway})

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

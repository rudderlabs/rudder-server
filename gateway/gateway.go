package gateway

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/dgraph-io/badger"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	enabledWriteKeysSourceMap                 map[string]string
	configSubscriberLock                      sync.RWMutex
	maxReqSize                                int
	enableDedup                               bool
	enableRateLimit                           bool
	dedupWindow                               time.Duration
)

// CustomVal is used as a key in the jobsDB customval column
var CustomVal string

var batchEvent = []byte(`
	{
		"batch": [
		]
	}
`)

func loadConfig() {
	//Port where GW is running
	webPort = config.GetInt("Gateway.webPort", 8080)
	//Number of incoming requests that are batched before initiating write
	maxBatchSize = config.GetInt("Gateway.maxBatchSize", 32)
	//Timeout after which batch is formed anyway with whatever requests
	//are available
	batchTimeout = (config.GetDuration("Gateway.batchTimeoutInMS", time.Duration(20)) * time.Millisecond)
	//Multiple DB writers are used to write data to DB
	maxDBWriterProcess = config.GetInt("Gateway.maxDBWriterProcess", 4)
	// CustomVal is used as a key in the jobsDB customval column
	CustomVal = config.GetString("Gateway.CustomVal", "GW")
	// Maximum request size to gateway
	maxReqSize = config.GetInt("Gateway.maxReqSizeInKB", 100000) * 1000
	// Enable dedup of incoming events by default
	enableDedup = config.GetBool("Gateway.enableDedup", false)
	// Dedup time window in hours
	dedupWindow = config.GetDuration("Gateway.dedupWindowInS", time.Duration(86400))
	// Enable rate limit on incoming events. false by default
	enableRateLimit = config.GetBool("Gateway.enableRateLimit", false)
}

func init() {
	config.Initialize()
	loadConfig()
	loadStatusMap()
}

//HandleT is the struct returned by the Setup call
type HandleT struct {
	webRequestQ                               chan *webRequestT
	batchRequestQ                             chan *batchWebRequestT
	jobsDB                                    *jobsdb.HandleT
	badgerDB                                  *badger.DB
	ackCount                                  uint64
	recvCount                                 uint64
	rateLimiter                               *ratelimiter.HandleT
	batchSizeStat, batchTimeStat, latencyStat *stats.RudderStats
}

func updateWriteKeyStats(writeKeyStats map[string]int, bucket string) {
	for writeKey, count := range writeKeyStats {
		writeKeyStatsD := stats.NewWriteKeyStat(bucket, stats.CountType, writeKey)
		writeKeyStatsD.Count(count)
	}
}

//Function to process the batch requests. It saves data in DB and
//sends and ACK on the done channel which unblocks the HTTP handler
func (gateway *HandleT) webRequestBatchDBWriter(process int) {
	for breq := range gateway.batchRequestQ {
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
		gateway.batchTimeStat.Start()
		var allMessageIds [][]byte
		for _, req := range breq.batchRequest {
			writeKey, _, ok := req.request.BasicAuth()
			misc.IncrementMapByKey(writeKeyStats, writeKey, 1)
			if !ok || writeKey == "" {
				req.done <- getStatus(NoWriteKeyInBasicAuth)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, "noWriteKey", 1)
				continue
			}

			ipAddr := misc.GetIPFromReq(req.request)
			if req.request.Body == nil {
				req.done <- getStatus(RequestBodyNil)
				preDbStoreCount++
				continue
			}
			body, err := ioutil.ReadAll(req.request.Body)
			req.request.Body.Close()

			if enableRateLimit {
				//If ratelimiter returns true for LimitReached, Just drop the event batch and continue.
				restrictorKey := backendconfig.GetWorkspaceIDForWriteKey(writeKey)
				if gateway.rateLimiter.LimitReached(restrictorKey) {
					req.done <- getStatus(TooManyRequests)
					preDbStoreCount++
					misc.IncrementMapByKey(workspaceDropRequestStats, restrictorKey, 1)
					continue
				}
			}

			if err != nil {
				req.done <- getStatus(RequestBodyReadFailed)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				continue
			}
			if !gjson.ValidBytes(body) {
				req.done <- getStatus(InvalidJson)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				continue
			}
			totalEventsInReq := len(gjson.GetBytes(body, "batch").Array())
			misc.IncrementMapByKey(writeKeyEventStats, writeKey, totalEventsInReq)
			if len(body) > maxReqSize {
				req.done <- getStatus(RequestBodyTooLarge)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				misc.IncrementMapByKey(writeKeyFailEventStats, writeKey, totalEventsInReq)
				continue
			}
			if !gateway.isWriteKeyEnabled(writeKey) {
				req.done <- getStatus(InvalidWriteKey)
				preDbStoreCount++
				misc.IncrementMapByKey(writeKeyFailStats, writeKey, 1)
				misc.IncrementMapByKey(writeKeyFailEventStats, writeKey, totalEventsInReq)
				continue
			}

			if req.reqType != "batch" {
				body, _ = sjson.SetBytes(body, "type", req.reqType)
				body, _ = sjson.SetRawBytes(batchEvent, "batch.0", body)
			}

			// set anonymousId if not set in payload
			var index int
			result := gjson.GetBytes(body, "batch")
			newAnonymousID := uuid.NewV4().String()
			var reqMessageIDs [][]byte
			result.ForEach(func(_, _ gjson.Result) bool {
				if !gjson.GetBytes(body, fmt.Sprintf(`batch.%v.anonymousId`, index)).Exists() {
					body, _ = sjson.SetBytes(body, fmt.Sprintf(`batch.%v.anonymousId`, index), newAnonymousID)
				}
				if !gjson.GetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index)).Exists() {
					body, _ = sjson.SetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index), uuid.NewV4().String())
				}
				if enableDedup {
					reqMessageIDs = append(reqMessageIDs, []byte(gjson.GetBytes(body, fmt.Sprintf(`batch.%v.messageId`, index)).String()))
				}
				index++
				return true // keep iterating
			})

			if enableDedup {
				allMessageIds = append(allMessageIds, reqMessageIDs...)
				gateway.dedupWithBadger(&body, reqMessageIDs, writeKey, writeKeyDupStats)
				if len(gjson.GetBytes(body, "batch").Array()) == 0 {
					req.done <- ""
					preDbStoreCount++
					misc.IncrementMapByKey(writeKeySuccessEventStats, writeKey, totalEventsInReq)
					continue
				}
			}

			logger.Debug("IP address is ", ipAddr)
			body, _ = sjson.SetBytes(body, "requestIP", ipAddr)
			body, _ = sjson.SetBytes(body, "writeKey", writeKey)
			body, _ = sjson.SetBytes(body, "receivedAt", time.Now().Format(misc.RFC3339Milli))
			eventBatchesToRecord = append(eventBatchesToRecord, fmt.Sprintf("%s", body))

			id := uuid.NewV4()
			//Should be function of body
			newJob := jobsdb.JobT{
				UUID:         id,
				Parameters:   []byte(fmt.Sprintf(`{"source_id": "%v"}`, enabledWriteKeysSourceMap[writeKey])),
				CustomVal:    CustomVal,
				EventPayload: []byte(body),
			}
			jobList = append(jobList, &newJob)
			jobIDReqMap[newJob.UUID] = req
			jobWriteKeyMap[newJob.UUID] = writeKey
			jobEventCountMap[newJob.UUID] = totalEventsInReq
		}

		errorMessagesMap := gateway.jobsDB.Store(jobList)

		gateway.writeToBadger(allMessageIds)

		if preDbStoreCount+len(errorMessagesMap) != len(breq.batchRequest) {
			panic(fmt.Errorf("preDbStoreCount:%d+len(errorMessagesMap):%d != len(breq.batchRequest):%d",
				preDbStoreCount, len(errorMessagesMap), len(breq.batchRequest)))
		}
		for uuid, err := range errorMessagesMap {
			if err != "" {
				misc.IncrementMapByKey(writeKeyFailStats, jobWriteKeyMap[uuid], 1)
				misc.IncrementMapByKey(writeKeyFailEventStats, jobWriteKeyMap[uuid], jobEventCountMap[uuid])
			} else {
				misc.IncrementMapByKey(writeKeySuccessStats, jobWriteKeyMap[uuid], 1)
				misc.IncrementMapByKey(writeKeySuccessEventStats, jobWriteKeyMap[uuid], jobEventCountMap[uuid])
			}
			jobIDReqMap[uuid].done <- err
		}

		//Sending events to config backend
		for _, event := range eventBatchesToRecord {
			sourcedebugger.RecordEvent(gjson.Get(event, "writeKey").Str, event)
		}

		gateway.batchTimeStat.End()
		gateway.batchSizeStat.Count(len(breq.batchRequest))
		// update stats request wise
		updateWriteKeyStats(writeKeyStats, "gateway.write_key_requests")
		updateWriteKeyStats(writeKeySuccessStats, "gateway.write_key_successful_requests")
		updateWriteKeyStats(writeKeyFailStats, "gateway.write_key_failed_requests")
		if enableRateLimit {
			updateWriteKeyStats(workspaceDropRequestStats, "gateway.work_space_dropped_requests")
		}
		// update stats event wise
		updateWriteKeyStats(writeKeyEventStats, "gateway.write_key_events")
		updateWriteKeyStats(writeKeySuccessEventStats, "gateway.write_key_successful_events")
		updateWriteKeyStats(writeKeyFailEventStats, "gateway.write_key_failed_events")
		if enableDedup {
			updateWriteKeyStats(writeKeyDupStats, "gateway.write_key_duplicate_events")
		}
	}
}

func (gateway *HandleT) dedupWithBadger(body *[]byte, messageIDs [][]byte, writeKey string, writeKeyDupStats map[string]int) {
	var toRemoveMessageIndexes []int
	err := gateway.badgerDB.View(func(txn *badger.Txn) error {
		for idx, messageID := range messageIDs {
			_, err := txn.Get([]byte(messageID))
			if err != badger.ErrKeyNotFound {
				toRemoveMessageIndexes = append(toRemoveMessageIndexes, idx)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	count := 0
	for _, idx := range toRemoveMessageIndexes {
		logger.Debugf("Dropping event with duplicate messageId: %s", messageIDs[idx])
		misc.IncrementMapByKey(writeKeyDupStats, writeKey, 1)
		*body, err = sjson.DeleteBytes(*body, fmt.Sprintf(`batch.%v`, idx-count))
		if err != nil {
			panic(err)
		}
		count++
	}
}

func (gateway *HandleT) writeToBadger(messageIDs [][]byte) {
	if enableDedup {
		err := gateway.badgerDB.Update(func(txn *badger.Txn) error {
			// Your code here…
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

//Function to batch incoming web requests
func (gateway *HandleT) webRequestBatcher() {
	var reqBuffer = make([]*webRequestT, 0)
	timeout := time.After(batchTimeout)
	for {
		select {
		case req := <-gateway.webRequestQ:
			//Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxBatchSize {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				gateway.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(batchTimeout)
			if len(reqBuffer) > 0 {
				breq := batchWebRequestT{batchRequest: reqBuffer}
				gateway.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		}
	}
}

func (gateway *HandleT) printStats() {
	for {
		time.Sleep(10 * time.Second)
		logger.Info("Gateway Recv/Ack ", gateway.recvCount, gateway.ackCount)
	}
}

func (gateway *HandleT) stat(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		gateway.latencyStat.Start()
		wrappedFunc(w, r)
		gateway.latencyStat.End()
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

func (gateway *HandleT) webGroupHandler(w http.ResponseWriter, r *http.Request) {
	gateway.webHandler(w, r, "group")
}

func (gateway *HandleT) webHandler(w http.ResponseWriter, r *http.Request, reqType string) {
	logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)
	done := make(chan string)
	req := webRequestT{request: r, writer: &w, done: done, reqType: reqType}
	gateway.webRequestQ <- &req
	//Wait for batcher process to be done
	errorMessage := <-done
	atomic.AddUint64(&gateway.ackCount, 1)
	if errorMessage != "" {
		logger.Debug(errorMessage)
		http.Error(w, errorMessage, 400)
	} else {
		logger.Debug(getStatus(Ok))
		w.Write([]byte(getStatus(Ok)))
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

	healthVal := fmt.Sprintf(`{"server":"UP", "db":"%s","acceptingEvents":"TRUE","routingEvents":"%s","mode":"%s","goroutines":"%d", "backendConfigMode": "%s", "lastSync":"%s"}`, dbService, enabledRouter, strings.ToUpper(db.CurrentMode), runtime.NumGoroutine(), backendConfigMode, backendconfig.LastSync)
	w.Write([]byte(healthVal))
}

func reflectOrigin(origin string) bool {
	return true
}

func (gateway *HandleT) startWebHandler() {

	logger.Infof("Starting in %d", webPort)

	http.HandleFunc("/v1/batch", gateway.stat(gateway.webBatchHandler))
	http.HandleFunc("/v1/identify", gateway.stat(gateway.webIdentifyHandler))
	http.HandleFunc("/v1/track", gateway.stat(gateway.webTrackHandler))
	http.HandleFunc("/v1/page", gateway.stat(gateway.webPageHandler))
	http.HandleFunc("/v1/screen", gateway.stat(gateway.webScreenHandler))
	http.HandleFunc("/v1/alias", gateway.stat(gateway.webAliasHandler))
	http.HandleFunc("/v1/group", gateway.stat(gateway.webGroupHandler))
	http.HandleFunc("/health", gateway.healthHandler)

	c := cors.New(cors.Options{
		AllowOriginFunc:  reflectOrigin,
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
	})

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(webPort), c.Handler(bugsnag.Handler(nil))))
}

// Gets the config from config backend and extracts enabled writekeys
func backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "processConfig")
	for {
		config := <-ch
		configSubscriberLock.Lock()
		enabledWriteKeysSourceMap = map[string]string{}
		sources := config.Data.(backendconfig.SourcesT)
		for _, source := range sources.Sources {
			if source.Enabled {
				enabledWriteKeysSourceMap[source.WriteKey] = source.ID
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

//Setup initializes this module
func (gateway *HandleT) Setup(jobsDB *jobsdb.HandleT, rateLimiter *ratelimiter.HandleT, clearDB *bool) {
	gateway.latencyStat = stats.NewStat("gateway.response_time", stats.TimerType)
	gateway.batchSizeStat = stats.NewStat("gateway.batch_size", stats.CountType)
	gateway.batchTimeStat = stats.NewStat("gateway.batch_time", stats.TimerType)

	if enableDedup {
		gateway.openBadger(clearDB)
		defer gateway.badgerDB.Close()
	}
	gateway.rateLimiter = rateLimiter
	gateway.webRequestQ = make(chan *webRequestT)
	gateway.batchRequestQ = make(chan *batchWebRequestT)
	gateway.jobsDB = jobsDB
	rruntime.Go(func() {
		gateway.webRequestBatcher()
	})
	rruntime.Go(func() {
		backendConfigSubscriber()
	})
	for i := 0; i < maxDBWriterProcess; i++ {
		j := i
		rruntime.Go(func() {
			gateway.webRequestBatchDBWriter(j)
		})
	}
	backendconfig.WaitForConfig()
	rruntime.Go(func() {
		gateway.printStats()
	})
	gateway.startWebHandler()
}

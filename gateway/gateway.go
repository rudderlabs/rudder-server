package gateway

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugsnag/bugsnag-go"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/rudderlabs/rudder-server/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
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
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

var (
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	respMessage                               string
	enabledWriteKeys                          []string
	configSubscriberLock                      sync.RWMutex
	maxReqSize                                int
)

// CustomVal is used as a key in the jobsDB customval column
var CustomVal string

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
	//Reponse message sent to client
	respMessage = config.GetString("Gateway.respMessage", "OK")
	// Maximum request size to gateway
	maxReqSize = config.GetInt("Gateway.maxReqSizeInKB", 100000) * 1000
}

func init() {
	config.Initialize()
	loadConfig()
}

//HandleT is the struct returned by the Setup call
type HandleT struct {
	webRequestQ   chan *webRequestT
	batchRequestQ chan *batchWebRequestT
	jobsDB        *jobsdb.HandleT
	ackCount      uint64
	recvCount     uint64
}

//Function to process the batch requests. It saves data in DB and
//sends and ACK on the done channel which unblocks the HTTP handler
func (gateway *HandleT) webRequestBatchDBWriter(process int) {

	for breq := range gateway.batchRequestQ {

		var jobList []*jobsdb.JobT
		var jobIDReqMap = make(map[uuid.UUID]*webRequestT)
		var preDbStoreCount int
		for _, req := range breq.batchRequest {
			if req.request.Body == nil {
				preDbStoreCount++
				continue
			}
			body, err := ioutil.ReadAll(req.request.Body)
			req.request.Body.Close()
			if err != nil {
				fmt.Println("Failed to read body from request")
				req.done <- "Failed to read body from request"
				preDbStoreCount++
				continue
			}
			if len(body) > maxReqSize {
				req.done <- "Request size exceeds max limit"
				preDbStoreCount++
				continue
			}
			if !gateway.verifyRequestBodyConfig(body) {
				req.done <- "Invalid Write Key"
				preDbStoreCount++
				continue
			}
			id := uuid.NewV4()
			//Should be function of body
			newJob := jobsdb.JobT{
				UUID:         id,
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    CustomVal,
				EventPayload: []byte(body),
			}
			jobList = append(jobList, &newJob)
			jobIDReqMap[newJob.UUID] = req
		}

		errorMessagesMap := gateway.jobsDB.Store(jobList)
		misc.Assert(preDbStoreCount+len(errorMessagesMap) == len(breq.batchRequest))
		for key, val := range errorMessagesMap {
			jobIDReqMap[key].done <- val
		}
	}
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func (gateway *HandleT) verifyRequestBodyConfig(body []byte) bool {
	bodyJSON := fmt.Sprintf("%s", body)
	writeKey := gjson.Get(bodyJSON, "writeKey")
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	if !contains(enabledWriteKeys, writeKey.Str) {
		return false
	}
	return true
}

//Function to batch incoming web requests
func (gateway *HandleT) webRequestBatcher() {
	var reqBuffer = make([]*webRequestT, 0)
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
		case <-time.After(batchTimeout):
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
		fmt.Println("Gateway Recv/Ack", gateway.recvCount, gateway.ackCount)
	}
}

//Main handler function for incoming requets
func (gateway *HandleT) webHandler(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&gateway.recvCount, 1)
	done := make(chan string)
	req := webRequestT{request: r, writer: &w, done: done}
	gateway.webRequestQ <- &req
	//Wait for batcher process to be done
	errorMessage := <-done
	atomic.AddUint64(&gateway.ackCount, 1)
	if errorMessage != "" {
		http.Error(w, errorMessage, 400)
	} else {
		w.Write([]byte(respMessage))
	}

}

func (gateway *HandleT) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("success"))
}

func (gateway *HandleT) startWebHandler() {
	fmt.Printf("Starting in %d\n", webPort)
	http.HandleFunc("/hello", gateway.webHandler)
	http.HandleFunc("/health", gateway.healthHandler)
	http.ListenAndServe(":"+strconv.Itoa(webPort), bugsnag.Handler(nil))
}

// Gets the config from config backend and extracts enabled writekeys
func backendConfigSubscriber() {
	ch1 := make(chan utils.DataEvent)
	backendconfig.Eb.Subscribe("backendconfig", ch1)
	for {
		config := <-ch1
		configSubscriberLock.Lock()
		enabledWriteKeys = []string{}
		sources := config.Data.(backendconfig.SourcesT)
		for _, source := range sources.Sources {
			if source.Enabled {
				enabledWriteKeys = append(enabledWriteKeys, source.WriteKey)
			}
		}
		configSubscriberLock.Unlock()
	}
}

//Setup initializes this module
func (gateway *HandleT) Setup(jobsDB *jobsdb.HandleT) {
	gateway.webRequestQ = make(chan *webRequestT)
	gateway.batchRequestQ = make(chan *batchWebRequestT)
	gateway.jobsDB = jobsDB
	go gateway.webRequestBatcher()
	go gateway.printStats()
	go backendConfigSubscriber()
	for i := 0; i < maxDBWriterProcess; i++ {
		go gateway.webRequestBatchDBWriter(i)
	}
	gateway.startWebHandler()

}

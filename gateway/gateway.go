package gateway

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	uuid "github.com/satori/go.uuid"
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
	done    chan<- struct{}
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

var (
	webPort, maxBatchSize, maxDBWriterProcess int
	batchTimeout                              time.Duration
	respMessage                               string
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
	fmt.Println(webPort, maxDBWriterProcess, maxBatchSize, batchTimeout, CustomVal, respMessage)
}

//HandleT is the struct returned by the Setup call
type HandleT struct {
	webRequestQ   chan *webRequestT
	batchRequestQ chan *batchWebRequestT
	jobsDB        *jobsdb.HandleT
}

//Function to process the batch requests. It saves data in DB and
//sends and ACK on the done channel which unblocks the HTTP handler
func (gateway *HandleT) webRequestBatchDBWriter(process int) {

	for breq := range gateway.batchRequestQ {

		var jobList []*jobsdb.JobT
		for _, req := range breq.batchRequest {
			body, err := ioutil.ReadAll(req.request.Body)
			if err != nil {
				fmt.Println("Failed to read body from request")
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
		}
		gateway.jobsDB.Store(jobList)

		// ACK the http requests
		for _, req := range breq.batchRequest {
			req.done <- struct{}{}
		}

	}
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

//Main handler function for incoming requets
func (gateway *HandleT) webHandler(w http.ResponseWriter, r *http.Request) {
	done := make(chan struct{})
	req := webRequestT{request: r, writer: &w, done: done}
	gateway.webRequestQ <- &req
	//Wait for batcher process to be done
	<-done
	w.Write([]byte(respMessage))

}

func (gateway *HandleT) startWebHandler() {
	fmt.Printf("Starting in %d\n", webPort)
	http.HandleFunc("/hello", gateway.webHandler)
	http.ListenAndServe(":"+strconv.Itoa(webPort), nil)
}

//Setup initializes this module
func (gateway *HandleT) Setup(jobsDB *jobsdb.HandleT) {
	loadConfig()
	gateway.webRequestQ = make(chan *webRequestT)
	gateway.batchRequestQ = make(chan *batchWebRequestT)
	gateway.jobsDB = jobsDB
	go gateway.webRequestBatcher()
	for i := 0; i < maxDBWriterProcess; i++ {
		go gateway.webRequestBatchDBWriter(i)
	}
	gateway.startWebHandler()

}

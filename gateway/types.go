package gateway

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// RequestHandler interface for abstracting out server-side import request processing and rest of the calls
type RequestHandler interface {
	ProcessRequest(gateway *Handle, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string
}

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
	errors         []string
}

type batchWebRequestT struct {
	batchRequest []*webRequestT
}

type userWorkerBatchRequestT struct {
	jobBatches  [][]*jobsdb.JobT
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
	batchTimeStat               stats.Measurement
	bufferFullStat, timeOutStat stats.Measurement
}

type jobFromReq struct {
	jobs      []*jobsdb.JobT
	numEvents int
	botEvents int
	version   string
}

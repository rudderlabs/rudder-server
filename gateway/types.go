package gateway

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// RequestHandler interface for abstracting out server-side import request processing and rest of the calls
type RequestHandler interface {
	ProcessRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, arctx *gwtypes.AuthRequestContext) string
}

// webRequestT acts as a basic unit for web requests.
// Contains some payload, could be of several types(batch, identify, track etc.)
// has a `done` channel that receives a response(error if any)
type webRequestT struct {
	done           chan<- string
	reqType        string
	requestPayload []byte
	authContext    *gwtypes.AuthRequestContext
	traceParent    string
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

// userWebRequestWorkerT is a basic worker unit that works on incoming webRequests.
// It has three channels used to communicate between the two goroutines each worker runs:
// - one to receive new webRequests,
// - one to send batches of said webRequests
// - one to receive errors if any in response to sending the said batches to dbWriterWorker
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

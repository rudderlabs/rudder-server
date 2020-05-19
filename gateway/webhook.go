package gateway

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/sjson"
)

var (
	sourceTransformerURL  string
	webhookBatchTimeout   time.Duration
	maxTransformerProcess int
	maxWebhookBatchSize   int
	webhookRetryMax       int
	webhookRetryWaitMax   time.Duration
)

type webhookT struct {
	request    *http.Request
	writer     *http.ResponseWriter
	done       chan<- string
	reqType    string
	sourceType string
	writeKey   string
}

type batchWebhookT struct {
	batchRequest []*webhookT
	sourceType   string
}

type webhookHandleT struct {
	requestQ      map[string]chan *webhookT
	batchRequestQ chan *batchWebhookT
	netClient     *retryablehttp.Client
	gwHandle      *HandleT
	// stats
	perfStats          *misc.PerfStats
	sentStat           stats.RudderStats
	receivedStat       stats.RudderStats
	failedStat         stats.RudderStats
	transformTimerStat stats.RudderStats
	sourceStats        map[string]*WebhookStatT
}

type WebhookStatT struct {
	id              string
	numEvents       stats.RudderStats
	numOutputEvents stats.RudderStats
	sourceTransform stats.RudderStats
}

func parseWriteKey(req *http.Request) (writeKey string, found bool) {
	queryParams := req.URL.Query()
	writeKeys, found := queryParams["writeKey"]
	if found {
		writeKey = writeKeys[0]
	} else {
		writeKey, _, found = req.BasicAuth()
	}
	return
}

func (webhook *webhookHandleT) failRequest(w http.ResponseWriter, r *http.Request, reason string, stat string) {
	var writeKeyFailStats = make(map[string]int)
	misc.IncrementMapByKey(writeKeyFailStats, stat, 1)
	webhook.gwHandle.updateWriteKeyStats(writeKeyFailStats, "gateway.write_key_failed_requests")
	logger.Debug(reason)
	http.Error(w, reason, 400)
	atomic.AddUint64(&webhook.gwHandle.ackCount, 1)
}

func (webhook *webhookHandleT) batchHandler(w http.ResponseWriter, r *http.Request) {
	logger.LogRequest(r)
	atomic.AddUint64(&webhook.gwHandle.recvCount, 1)

	writeKey, ok := parseWriteKey(r)
	if !ok {
		webhook.failRequest(w, r, getStatus(InvalidWriteKey), "noWriteKey")
		return
	}

	configSubscriberLock.RLock()
	sourceDefName, ok := enabledWriteKeySourceDefMap[writeKey]
	configSubscriberLock.RUnlock()
	if !ok {
		webhook.failRequest(w, r, getStatus(InvalidWriteKey), writeKey)
		return
	}
	if !misc.ContainsString(webhookSources, sourceDefName) {
		webhook.failRequest(w, r, getStatus(InvalidWebhookSource), writeKey)
		return
	}

	done := make(chan string)
	req := webhookT{request: r, writer: &w, done: done, sourceType: sourceDefName, writeKey: writeKey}
	webhook.requestQ[sourceDefName] <- &req

	//Wait for batcher process to be done
	errorMessage := <-done
	atomic.AddUint64(&webhook.gwHandle.ackCount, 1)
	webhook.gwHandle.trackRequestMetrics(errorMessage)
	if errorMessage != "" {
		logger.Debug(errorMessage)
		http.Error(w, errorMessage, 400)
	} else {
		logger.Debug(getStatus(Ok))
		w.Write([]byte(getStatus(Ok)))
	}
}

func (webhook *webhookHandleT) batcher(sourceDef string) {
	var reqBuffer = make([]*webhookT, 0)
	timeout := time.After(webhookBatchTimeout)
	for {
		select {
		case req := <-webhook.requestQ[sourceDef]:
			//Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxWebhookBatchSize {
				breq := batchWebhookT{batchRequest: reqBuffer, sourceType: sourceDef}
				webhook.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webhookT, 0)
			}
		case <-timeout:
			timeout = time.After(webhookBatchTimeout)
			if len(reqBuffer) > 0 {
				breq := batchWebhookT{batchRequest: reqBuffer, sourceType: sourceDef}
				webhook.batchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webhookT, 0)
			}
		}
	}
}

func (webhook *webhookHandleT) batchTransform(process int) {
	for breq := range webhook.batchRequestQ {
		payloadArr := [][]byte{}
		webRequests := []*webhookT{}
		for _, req := range breq.batchRequest {
			body, err := ioutil.ReadAll(req.request.Body)
			req.request.Body.Close()

			if err != nil {
				req.done <- getStatus(RequestBodyReadFailed)
				continue
			}

			payloadArr = append(payloadArr, body)
			webRequests = append(webRequests, req)
		}

		if len(payloadArr) == 0 {
			continue
		}

		// stats
		webhook.perfStats.Start()
		webhook.sourceStats[breq.sourceType].numEvents.Count(len(payloadArr))
		webhook.sourceStats[breq.sourceType].sourceTransform.Start()

		batchResponse := webhook.transform(payloadArr, breq.sourceType)

		// stats
		webhook.sourceStats[breq.sourceType].sourceTransform.End()
		webhook.perfStats.End(len(payloadArr))
		webhook.perfStats.Print()

		if batchResponse.batchError != nil {
			for _, req := range breq.batchRequest {
				req.done <- batchResponse.batchError.Error()
			}
			continue
		}

		if len(batchResponse.responses) != len(payloadArr) {
			panic("webhook.transform() response events size does not equal sent events size")
		}

		webhook.sourceStats[breq.sourceType].numOutputEvents.Count(len(batchResponse.responses))

		for idx, resp := range batchResponse.responses {
			webRequest := webRequests[idx]
			output := resp.output
			if resp.err != "" {
				webRequests[idx].done <- resp.err
				continue
			}
			rruntime.Go(func() {
				webhook.enqueueToWebRequestQ(webRequest, output)
			})
		}
	}
}

func (webhook *webhookHandleT) enqueueToWebRequestQ(req *webhookT, payload []byte) {
	rudderEvent, _ := sjson.SetRawBytes(batchEvent, "batch.0", payload)
	// repalce body with transformed event in rudder event form
	req.request.Body = ioutil.NopCloser(bytes.NewReader(rudderEvent))
	// set write key in basic auth header
	req.request.SetBasicAuth(req.writeKey, "")
	done := make(chan string)
	rudderFormatReq := webRequestT{request: req.request, writer: req.writer, done: done, reqType: "batch"}
	webhook.gwHandle.webRequestQ <- &rudderFormatReq

	//Wait for batcher process to be done
	errorMessage := <-done
	req.done <- errorMessage
}

func (webhook *webhookHandleT) register(name string) {
	if _, ok := webhook.requestQ[name]; !ok {
		webhook.requestQ[name] = make(chan *webhookT)
		webhook.sourceStats[name] = newWebhookStat(name)
		rruntime.Go(func() {
			webhook.batcher(name)
		})
	}
}

func newWebhookStat(destID string) *WebhookStatT {
	numEvents := stats.NewDestStat("webhook_num_events", stats.CountType, destID)
	numOutputEvents := stats.NewDestStat("webhook_num_output_events", stats.CountType, destID)
	sourceTransform := stats.NewDestStat("webhook_dest_transform", stats.TimerType, destID)
	return &WebhookStatT{
		id:              destID,
		numEvents:       numEvents,
		numOutputEvents: numOutputEvents,
		sourceTransform: sourceTransform,
	}
}

func (gateway *HandleT) setupWebhookHandler() (webhook *webhookHandleT) {
	webhook = &webhookHandleT{gwHandle: gateway}
	webhook.requestQ = make(map[string](chan *webhookT))
	webhook.batchRequestQ = make(chan *batchWebhookT)
	webhook.netClient = retryablehttp.NewClient()
	webhook.netClient.Logger = nil // to avoid debug logs
	webhook.netClient.RetryWaitMax = webhookRetryWaitMax
	webhook.netClient.RetryMax = webhookRetryMax
	for i := 0; i < maxTransformerProcess; i++ {
		j := i
		rruntime.Go(func() {
			webhook.batchTransform(j)
		})
	}

	webhook.sentStat = stats.NewStat("webhook.transformer_sent", stats.CountType)
	webhook.receivedStat = stats.NewStat("webhook.transformer_received", stats.CountType)
	webhook.failedStat = stats.NewStat("webhook.transformer_failed", stats.CountType)
	webhook.transformTimerStat = stats.NewStat("webhook.transformation_time", stats.TimerType)
	webhook.perfStats = &misc.PerfStats{}
	webhook.perfStats.Setup("Webhook")
	webhook.sourceStats = make(map[string]*WebhookStatT)
	return webhook
}

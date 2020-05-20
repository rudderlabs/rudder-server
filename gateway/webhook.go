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
	ackCount      uint64
	recvCount     uint64
}

type webhookSourceStatT struct {
	id              string
	numEvents       stats.RudderStats
	numOutputEvents stats.RudderStats
	sourceTransform stats.RudderStats
}

type webhookStatsT struct {
	sentStat           stats.RudderStats
	receivedStat       stats.RudderStats
	failedStat         stats.RudderStats
	transformTimerStat stats.RudderStats
	sourceStats        map[string]*webhookSourceStatT
}

type batchWebhookTransformerT struct {
	webhook *webhookHandleT
	stats   *webhookStatsT
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
	atomic.AddUint64(&webhook.recvCount, 1)

	writeKey, ok := parseWriteKey(r)
	if !ok {
		webhook.failRequest(w, r, getStatus(InvalidWriteKey), "noWriteKey")
		atomic.AddUint64(&webhook.ackCount, 1)
		return
	}

	configSubscriberLock.RLock()
	sourceDefName, ok := enabledWriteKeySourceDefMap[writeKey]
	configSubscriberLock.RUnlock()
	if !ok {
		webhook.failRequest(w, r, getStatus(InvalidWriteKey), writeKey)
		atomic.AddUint64(&webhook.ackCount, 1)
		return
	}
	if !misc.ContainsString(webhookSources, sourceDefName) {
		webhook.failRequest(w, r, getStatus(InvalidWebhookSource), writeKey)
		atomic.AddUint64(&webhook.ackCount, 1)
		return
	}

	done := make(chan string)
	req := webhookT{request: r, writer: &w, done: done, sourceType: sourceDefName, writeKey: writeKey}
	webhook.requestQ[sourceDefName] <- &req

	//Wait for batcher process to be done
	errorMessage := <-done
	atomic.AddUint64(&webhook.gwHandle.ackCount, 1)
	atomic.AddUint64(&webhook.ackCount, 1)
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

func (bt *batchWebhookTransformerT) batchTransform() {
	for breq := range bt.webhook.batchRequestQ {
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

		if _, ok := bt.stats.sourceStats[breq.sourceType]; !ok {
			bt.stats.sourceStats[breq.sourceType] = newWebhookStat(breq.sourceType)
		}

		// stats
		bt.stats.sourceStats[breq.sourceType].numEvents.Count(len(payloadArr))
		bt.stats.sourceStats[breq.sourceType].sourceTransform.Start()

		batchResponse := bt.transform(payloadArr, breq.sourceType)

		// stats
		bt.stats.sourceStats[breq.sourceType].sourceTransform.End()

		if batchResponse.batchError != nil {
			for _, req := range breq.batchRequest {
				req.done <- batchResponse.batchError.Error()
			}
			continue
		}

		if len(batchResponse.responses) != len(payloadArr) {
			panic("webhook batchtransform response events size does not equal sent events size")
		}

		bt.stats.sourceStats[breq.sourceType].numOutputEvents.Count(len(batchResponse.responses))

		for idx, resp := range batchResponse.responses {
			webRequest := webRequests[idx]
			output := resp.output
			if resp.err != "" {
				webRequests[idx].done <- resp.err
				continue
			}
			rruntime.Go(func() {
				bt.webhook.enqueueToWebRequestQ(webRequest, output)
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
		// webhook.sourceStats[name] = newWebhookStat(name)
		rruntime.Go(func() {
			webhook.batcher(name)
		})
	}
}

func newWebhookStat(destID string) *webhookSourceStatT {
	numEvents := stats.NewDestStat("webhook_num_events", stats.CountType, destID)
	numOutputEvents := stats.NewDestStat("webhook_num_output_events", stats.CountType, destID)
	sourceTransform := stats.NewDestStat("webhook_dest_transform", stats.TimerType, destID)
	return &webhookSourceStatT{
		id:              destID,
		numEvents:       numEvents,
		numOutputEvents: numOutputEvents,
		sourceTransform: sourceTransform,
	}
}

func (webhook *webhookHandleT) printStats() {
	var lastRecvCount, lastAckCount uint64
	for {
		time.Sleep(10 * time.Second)
		if lastRecvCount != webhook.recvCount || lastAckCount != webhook.ackCount {
			lastRecvCount = webhook.recvCount
			lastAckCount = webhook.ackCount
			logger.Info("Webhook Recv/Ack ", webhook.recvCount, webhook.ackCount)
		}
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
		rruntime.Go(func() {
			wStats := webhookStatsT{}
			wStats.sentStat = stats.NewStat("webhook.transformer_sent", stats.CountType)
			wStats.receivedStat = stats.NewStat("webhook.transformer_received", stats.CountType)
			wStats.failedStat = stats.NewStat("webhook.transformer_failed", stats.CountType)
			wStats.transformTimerStat = stats.NewStat("webhook.transformation_time", stats.TimerType)
			wStats.sourceStats = make(map[string]*webhookSourceStatT)
			bt := batchWebhookTransformerT{
				webhook: webhook,
				stats:   &wStats,
			}
			bt.batchTransform()
		})
	}
	rruntime.Go(func() {
		webhook.printStats()
	})
	return webhook
}

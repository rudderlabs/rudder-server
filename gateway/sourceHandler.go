package gateway

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/sjson"
)

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

func (gateway *HandleT) failGatewaySourceRequest(w http.ResponseWriter, r *http.Request, reason string, stat string) {
	var writeKeyFailStats = make(map[string]int)
	misc.IncrementMapByKey(writeKeyFailStats, stat, 1)
	gateway.updateWriteKeyStats(writeKeyFailStats, "gateway.write_key_failed_requests")
	logger.Debug(reason)
	http.Error(w, reason, 400)
	atomic.AddUint64(&gateway.ackCount, 1)
}

func (gateway *HandleT) webSourceBatchHandler(w http.ResponseWriter, r *http.Request) {
	logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)

	writeKey, ok := parseWriteKey(r)
	if !ok {
		gateway.failGatewaySourceRequest(w, r, getStatus(InvalidWriteKey), "noWriteKey")
		return
	}

	configSubscriberLock.RLock()
	sourceDefName, ok := enabledWriteKeySourceDefMap[writeKey]
	configSubscriberLock.RUnlock()
	if !ok {
		gateway.failGatewaySourceRequest(w, r, getStatus(InvalidWriteKey), writeKey)
		return
	}
	if !misc.ContainsString(webhookSources, sourceDefName) {
		gateway.failGatewaySourceRequest(w, r, getStatus(InvalidWebhookSource), writeKey)
		return
	}

	done := make(chan string)
	req := webRequestT{request: r, writer: &w, done: done, sourceType: sourceDefName, writeKey: writeKey}
	gateway.sourceRequestQ[sourceDefName] <- &req

	//Wait for batcher process to be done
	errorMessage := <-done
	atomic.AddUint64(&gateway.ackCount, 1)
	gateway.trackRequestMetrics(errorMessage)
	if errorMessage != "" {
		logger.Debug(errorMessage)
		http.Error(w, errorMessage, 400)
	} else {
		logger.Debug(getStatus(Ok))
		w.Write([]byte(getStatus(Ok)))
	}
}

func (gateway *HandleT) sourceRequestBatcher(sourceDef string) {
	var reqBuffer = make([]*webRequestT, 0)
	timeout := time.After(batchTimeout)
	for {
		select {
		case req := <-gateway.sourceRequestQ[sourceDef]:
			//Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == maxBatchSize {
				breq := batchWebRequestT{batchRequest: reqBuffer, sourceType: sourceDef}
				gateway.sourceBatchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(batchTimeout)
			if len(reqBuffer) > 0 {
				breq := batchWebRequestT{batchRequest: reqBuffer, sourceType: sourceDef}
				gateway.sourceBatchRequestQ <- &breq
				reqBuffer = nil
				reqBuffer = make([]*webRequestT, 0)
			}
		}
	}
}

func (gateway *HandleT) sourceRequestBatchDBWriter(process int) {
	for breq := range gateway.sourceBatchRequestQ {
		payloadArr := [][]byte{}
		webRequests := []*webRequestT{}
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

		batchResponse := gateway.sourceTransform(payloadArr, breq.sourceType)

		if batchResponse.batchError != nil {
			for _, req := range breq.batchRequest {
				req.done <- batchResponse.batchError.Error()
			}
			continue
		}

		if len(batchResponse.responses) != len(payloadArr) {
			panic("sourceTransform() response size does not equal sent events size")
		}

		for idx, resp := range batchResponse.responses {
			if resp.err != "" {
				webRequests[idx].done <- resp.err
				continue
			}
			rruntime.Go(func() {
				gateway.enqueueToWebRequestQ(webRequests[idx], resp.output)
			})
		}
	}
}

func (gateway *HandleT) enqueueToWebRequestQ(req *webRequestT, payload []byte) {
	rudderEvent, _ := sjson.SetRawBytes(batchEvent, "batch.0", payload)
	// repalce body with transformed event in rudder event form
	req.request.Body = ioutil.NopCloser(bytes.NewReader(rudderEvent))
	// set write key in basic auth header
	req.request.SetBasicAuth(req.writeKey, "")
	done := make(chan string)
	rudderFormatReq := webRequestT{request: req.request, writer: req.writer, done: done, reqType: "batch"}
	gateway.webRequestQ <- &rudderFormatReq

	//Wait for batcher process to be done
	errorMessage := <-done
	req.done <- errorMessage
}

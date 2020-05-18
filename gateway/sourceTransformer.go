package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
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

func (gateway *HandleT) webSourceBatchHandler(w http.ResponseWriter, r *http.Request) {
	logger.LogRequest(r)
	atomic.AddUint64(&gateway.recvCount, 1)

	writeKey, ok := parseWriteKey(r)
	if !ok {
		status := getStatus(InvalidWriteKey)
		logger.Debug(status)
		http.Error(w, status, 400)
		return
	}

	configSubscriberLock.RLock()
	sourceDefName, ok := enabledWriteKeySourceDefMap[writeKey]
	configSubscriberLock.RUnlock()
	if !ok {
		status := getStatus(InvalidWriteKey)
		logger.Debug(status)
		http.Error(w, status, 400)
		return
	}
	if !misc.ContainsString(webhookSources, sourceDefName) {
		status := getStatus(InvalidWebhookSource)
		logger.Debug(status)
		http.Error(w, status, 400)
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

//Function to batch incoming web requests
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

		payload := misc.MakeJSONArray(payloadArr)
		url := fmt.Sprintf(`%s/%s`, sourceTransformerURL, strings.ToLower(breq.sourceType))
		resp, err := gateway.transformerClient.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payload))

		if err != nil {
			failBatchRequest(breq.batchRequest, getStatus(SourceTransformerFailed))
			continue
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			failBatchRequest(breq.batchRequest, getStatus(SourceTrasnformerResponseReadFailed))
			continue
		}

		var responses []interface{}
		err = json.Unmarshal(respBody, &responses)

		for idx, response := range responses {
			webRequest := webRequests[idx]
			respElemMap, castOk := response.(map[string]interface{})
			if castOk {
				outputInterface, ok := respElemMap["output"]
				if !ok {
					failBatchRequest([]*webRequestT{webRequest}, getStatus(SourceTrasnformerResponseReadFailed))
				}

				output, ok := outputInterface.(map[string]interface{})
				if !ok {
					failBatchRequest([]*webRequestT{webRequest}, getStatus(SourceTrasnformerResponseReadFailed))
				}

				if statusCode, found := output["statusCode"]; found && fmt.Sprintf("%v", statusCode) == "400" {
					var errorMessage interface{}
					if errorMessage, ok = output["error"]; !ok {
						errorMessage = getStatus(SourceTrasnformerResponseReadFailed)
					}
					webRequests[idx].done <- fmt.Sprintf("%v", errorMessage)
					continue
				}
				rruntime.Go(func() {
					marshalledOutput, _ := json.Marshal(output)
					gateway.enqueueToWebRequestQ(webRequest, marshalledOutput)
				})

			} else {
				failBatchRequest(breq.batchRequest, getStatus(SourceTrasnformerResponseReadFailed))
			}
		}

	}
}

func failBatchRequest(batchRequest []*webRequestT, reason string) {
	for _, req := range batchRequest {
		req.done <- getStatus(SourceTransformerFailed)
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

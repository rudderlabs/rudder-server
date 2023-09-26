package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type outputToSource struct {
	Body        []byte `json:"body"`
	ContentType string `json:"contentType"`
}

// transformerResponse will be populated using JSON unmarshall
// so we need to make fields public
type transformerResponse struct {
	Output         map[string]interface{} `json:"output"`
	Err            string                 `json:"error"`
	StatusCode     int                    `json:"statusCode"`
	OutputToSource *outputToSource        `json:"outputToSource"`
}

type transformerBatchResponseT struct {
	batchError error
	responses  []transformerResponse
	statusCode int
}

func (bt *batchWebhookTransformerT) markResponseFail(reason string) transformerResponse {
	statusCode := response.GetErrorStatusCode(reason)
	resp := transformerResponse{
		Err:        response.GetStatus(reason),
		StatusCode: statusCode,
	}
	bt.stats.failedStat.Count(1)
	return resp
}

func (bt *batchWebhookTransformerT) sendToTransformer(events [][]byte, url string) (*http.Response, error) {
	transformStart := time.Now()
	payload := misc.MakeJSONArray(events)
	resp, err := bt.webhook.netClient.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payload))
	bt.stats.transformTimerStat.Since(transformStart)
	return resp, err
}

func (bt *batchWebhookTransformerT) transform(events [][]byte, payloadArr [][]byte, sourceType string, version string) transformerBatchResponseT {
	bt.stats.sentStat.Count(len(events))
	url := fmt.Sprintf(`%s/%s`, bt.sourceTransformerURL, strings.ToLower(sourceType))

	resp, err := bt.sendToTransformer(events, url)

	if err != nil {
		err := fmt.Errorf("JS HTTP connection error to source transformer: URL: %v Error: %+v", url, err)
		return transformerBatchResponseT{batchError: err, statusCode: http.StatusServiceUnavailable}
	}

	respBody, err := io.ReadAll(resp.Body)
	func() { httputil.CloseResponse(resp) }()

	if err != nil {
		bt.stats.failedStat.Count(len(events))
		statusCode := response.GetErrorStatusCode(response.RequestBodyReadFailed)
		err := errors.New(response.GetStatus(response.RequestBodyReadFailed))
		return transformerBatchResponseT{batchError: err, statusCode: statusCode}
	}
	/*
		Following 404 case can be in that case when v1 endpoint is not there
		that is new server is hitting old transformer( when there used to be no source sent to tranformer)
	*/
	if resp.StatusCode == 404 {
		// v1 endpoint not available so falling back to v0 endpoint
		url := strings.ReplaceAll(bt.sourceTransformerURL, "v1", "v0")
		resp, err = bt.sendToTransformer(events, url)
		respBody, err = io.ReadAll(resp.Body)
		func() { httputil.CloseResponse(resp) }()
	}
	if resp.StatusCode != http.StatusOK {
		bt.webhook.logger.Errorf("source Transformer returned non-success statusCode: %v, Error: %v", resp.StatusCode, resp.Status)
		bt.stats.failedStat.Count(len(events))
		err := fmt.Errorf("source Transformer returned non-success statusCode: %v, Error: %v", resp.StatusCode, resp.Status)
		return transformerBatchResponseT{batchError: err}
	}

	/*
		expected response format
		[
			------Output to Gateway only---------
			{
				output: {
					batch: [
						{
							context: {...},
							properties: {...},
							userId: "U123"
						}
					]
				}
			}

			------Output to Source only---------
			{
				outputToSource: {
					"body": "eyJhIjoxfQ==", // base64 encode string
					"contentType": "application/json"
				}
			}

			------Output to Both Gateway and Source---------
			{
				output: {
					batch: [
						{
							context: {...},
							properties: {...},
							userId: "U123"
						}
					]
				},
				outputToSource: {
					"body": "eyJhIjoxfQ==", // base64 encode string
					"contentType": "application/json"
				}
			}

			------Error example---------
			{
				statusCode: 400,
				error: "event type is not supported"
			}

		]
	*/
	var responses []transformerResponse
	err = json.Unmarshal(respBody, &responses)

	if err != nil {
		statusCode := response.GetErrorStatusCode(response.SourceTransformerInvalidResponseFormat)
		err := errors.New(response.GetStatus(response.SourceTransformerInvalidResponseFormat))
		return transformerBatchResponseT{batchError: err, statusCode: statusCode}
	}
	if len(responses) != len(events) {
		statusCode := response.GetErrorStatusCode(response.SourceTransformerInvalidResponseFormat)
		err := errors.New(response.GetStatus(response.SourceTransformerInvalidResponseFormat))
		bt.webhook.logger.Errorf("source rudder-transformer response size does not equal sent events size")
		return transformerBatchResponseT{batchError: err, statusCode: statusCode}
	}

	batchResponse := transformerBatchResponseT{responses: make([]transformerResponse, len(events))}
	for idx, resp := range responses {
		if resp.Err != "" {
			batchResponse.responses[idx] = resp
			bt.stats.failedStat.Count(1)
			continue
		}
		if resp.Output == nil && resp.OutputToSource == nil {
			batchResponse.responses[idx] = bt.markResponseFail(response.SourceTransformerFailedToReadOutput)
			continue
		}
		bt.stats.receivedStat.Count(1)
		batchResponse.responses[idx] = resp
	}
	return batchResponse
}

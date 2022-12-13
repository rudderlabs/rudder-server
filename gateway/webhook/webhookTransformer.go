package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/services/stats"
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

func (bt *batchWebhookTransformerT) markResponseFail(sourceType, reason string) transformerResponse {
	resp := transformerResponse{
		Err:        response.GetStatus(reason),
		StatusCode: response.GetErrorStatusCode(reason),
	}
	bt.stats.failedStat.Count(1)
	stats.Default.NewTaggedStat("source_transform_failed_jobs", stats.CountType, stats.Tags{
		"sourceType": sourceType,
		"statusCode": strconv.Itoa(response.GetErrorStatusCode(reason)),
	}).Count(1)
	return resp
}

func (bt *batchWebhookTransformerT) transform(events [][]byte, sourceType string) transformerBatchResponseT {
	bt.stats.sentStat.Count(len(events))
	bt.stats.transformTimerStat.Start()

	stats.Default.NewTaggedStat("source_transform_received_jobs", stats.CountType, stats.Tags{
		"sourceType": sourceType,
	}).Count(len(events))

	payload := misc.MakeJSONArray(events)
	url := fmt.Sprintf(`%s/%s`, bt.sourceTransformerURL, strings.ToLower(sourceType))
	resp, err := bt.webhook.netClient.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payload))

	bt.stats.transformTimerStat.End()
	if err != nil {
		err := fmt.Errorf("JS HTTP connection error to source transformer: URL: %v Error: %+v", url, err)
		return transformerBatchResponseT{batchError: err}
	}

	respBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if err != nil {
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: errors.New(response.GetStatus(response.RequestBodyReadFailed)), statusCode: response.GetErrorStatusCode(response.RequestBodyReadFailed)}
	}

	if resp.StatusCode != http.StatusOK {
		pkgLogger.Errorf("Source Transformer returned status code: %v", resp.StatusCode)
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: fmt.Errorf("source Transformer returned non-success status code: %v, Error: %v", resp.StatusCode, resp.Status)}
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
		return transformerBatchResponseT{
			batchError: errors.New(response.GetStatus(response.SourceTransformerInvalidResponseFormat)),
			statusCode: response.GetErrorStatusCode(response.SourceTransformerInvalidResponseFormat),
		}
	}
	if len(responses) != len(events) {
		pkgLogger.Errorf("source rudder-transformer response size does not equal sent events size")
		return transformerBatchResponseT{
			batchError: errors.New(response.GetStatus(response.SourceTransformerInvalidResponseFormat)),
			statusCode: response.GetErrorStatusCode(response.SourceTransformerInvalidResponseFormat),
		}
	}

	batchResponse := transformerBatchResponseT{responses: make([]transformerResponse, len(events))}
	for idx, resp := range responses {
		if resp.Err != "" {
			batchResponse.responses[idx] = resp
			bt.stats.failedStat.Count(1)
			continue
		}
		if resp.Output == nil && resp.OutputToSource == nil {
			batchResponse.responses[idx] = bt.markResponseFail(sourceType, response.SourceTransformerFailedToReadOutput)
			continue
		}
		bt.stats.receivedStat.Count(1)
		batchResponse.responses[idx] = resp
	}
	return batchResponse
}

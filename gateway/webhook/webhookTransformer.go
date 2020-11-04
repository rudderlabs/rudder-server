package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type transformerResponseT struct {
	output     []byte
	err        string
	statusCode int
}

type transformerBatchResponseT struct {
	batchError error
	responses  []transformerResponseT
	statusCode int
}

func (bt *batchWebhookTransformerT) markRepsonseFail(reason string) transformerResponseT {
	resp := transformerResponseT{
		err:        response.GetStatus(reason),
		statusCode: response.GetStatusCode(reason),
	}
	bt.stats.failedStat.Count(1)
	return resp
}

func (bt *batchWebhookTransformerT) transform(events [][]byte, sourceType string) transformerBatchResponseT {
	bt.stats.sentStat.Count(len(events))
	bt.stats.transformTimerStat.Start()

	payload := misc.MakeJSONArray(events)
	url := fmt.Sprintf(`%s/%s`, sourceTransformerURL, strings.ToLower(sourceType))
	resp, err := bt.webhook.netClient.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(payload))

	bt.stats.transformTimerStat.End()
	if err != nil {
		pkgLogger.Error(err)
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: errors.New("Internal server error in source transformer")}
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: err}
	}

	if resp.StatusCode != http.StatusOK {
		pkgLogger.Errorf("Source Transformer returned status code: %v", resp.StatusCode)
		bt.stats.failedStat.Count(len(events))
		return transformerBatchResponseT{batchError: fmt.Errorf(`Source Transformer returned non-success status code: %v, Error: %v`, resp.StatusCode, string(respBody))}
	}

	/*
		expected response format
		[
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
			},
			// for event that give an error from source trasnformer
			{
				statusCode: 400,
				error: "event type is not supported"
			}
		]
	*/
	var responses []interface{}
	err = json.Unmarshal(respBody, &responses)

	if err != nil {
		return transformerBatchResponseT{
			batchError: errors.New(response.GetStatus(response.SourceTransformerInvalidResponseFormat)),
			statusCode: response.GetStatusCode(response.SourceTransformerInvalidResponseFormat),
		}
	}

	batchResponse := transformerBatchResponseT{responses: make([]transformerResponseT, len(events))}

	if len(responses) != len(events) {
		panic("Source rudder-transformer response size does not equal sent events size")
	}

	for idx, resp := range responses {
		respElemMap, castOk := resp.(map[string]interface{})
		if castOk {
			if statusCode, found := respElemMap["statusCode"]; found && fmt.Sprintf("%v", statusCode) != "200" {
				var errorMessage interface{}
				var ok bool
				code, _ := statusCode.(int)
				if errorMessage, ok = respElemMap["error"]; !ok {
					errorMessage = response.GetStatus(response.SourceTransformerResponseErrorReadFailed)
				}
				batchResponse.responses[idx] = transformerResponseT{
					err:        fmt.Sprintf("%v", errorMessage),
					statusCode: code,
				}
				bt.stats.failedStat.Count(1)
				continue
			}

			outputInterface, ok := respElemMap["output"]
			if !ok {
				batchResponse.responses[idx] = bt.markRepsonseFail(response.SourceTransformerFailedToReadOutput)
				continue
			}

			output, ok := outputInterface.(map[string]interface{})
			if !ok {
				batchResponse.responses[idx] = bt.markRepsonseFail(response.SourceTransformerInvalidOutputFormatInResponse)
				continue
			}

			_, ok = output["batch"].(interface{})
			if !ok {
				batchResponse.responses[idx] = bt.markRepsonseFail(response.SourceTransformerInvalidOutputFormatInResponse)
				continue
			}

			marshalledOutput, err := json.Marshal(output)
			if err != nil {
				batchResponse.responses[idx] = bt.markRepsonseFail(response.SourceTransformerInvalidOutputJSON)
				continue
			}

			bt.stats.receivedStat.Count(1)
			batchResponse.responses[idx] = transformerResponseT{output: marshalledOutput}
		} else {
			batchResponse.responses[idx] = bt.markRepsonseFail(response.SourceTransformerInvalidResponseFormat)
		}
	}
	return batchResponse
}

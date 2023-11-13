package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type sourceTransformAdapter interface {
	getTransformerEvent(authCtx *gwtypes.AuthRequestContext, body []byte) ([]byte, error)
	getTransformerURL(sourceType string) (string, error)
}

type v0Adapter struct{}

type v1Adapter struct{}

type V1TransformerEvent struct {
	Event  json.RawMessage       `json:"event"`
	Source backendconfig.SourceT `json:"source"`
}

func (v0 *v0Adapter) getTransformerEvent(authCtx *gwtypes.AuthRequestContext, body []byte) ([]byte, error) {
	return body, nil
}

func (v0 *v0Adapter) getTransformerURL(sourceType string) (string, error) {
	return getTransformerURL(transformer.V0, sourceType)
}

func (v1 *v1Adapter) getTransformerEvent(authCtx *gwtypes.AuthRequestContext, body []byte) ([]byte, error) {
	source := authCtx.Source

	v1TransformerEvent := V1TransformerEvent{
		Event: body,
		Source: backendconfig.SourceT{
			ID:               source.ID,
			OriginalID:       source.OriginalID,
			Name:             source.Name,
			SourceDefinition: source.SourceDefinition,
			Config:           source.Config,
			Enabled:          source.Enabled,
			WorkspaceID:      source.WorkspaceID,
			WriteKey:         source.WriteKey,
			Transient:        source.Transient,
		},
	}

	return json.Marshal(v1TransformerEvent)
}

func (v1 *v1Adapter) getTransformerURL(sourceType string) (string, error) {
	return getTransformerURL(transformer.V1, sourceType)
}

func newSourceTransformAdapter(version string) sourceTransformAdapter {
	switch version {
	case "v1":
		return &v1Adapter{}
	}

	return &v0Adapter{}
}

func getTransformerURL(version, sourceType string) (string, error) {
	baseURL := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	return url.JoinPath(baseURL, version, "sources", strings.ToLower(sourceType))
}

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

func (bt *batchWebhookTransformerT) transform(events [][]byte, sourceTransformerURL string) transformerBatchResponseT {
	bt.stats.sentStat.Count(len(events))
	transformStart := time.Now()

	payload := misc.MakeJSONArray(events)
	resp, err := bt.webhook.netClient.Post(sourceTransformerURL, "application/json; charset=utf-8", bytes.NewBuffer(payload))

	bt.stats.transformTimerStat.Since(transformStart)
	if err != nil {
		err := fmt.Errorf("JS HTTP connection to source transformer (URL: %q): %w", sourceTransformerURL, err)
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

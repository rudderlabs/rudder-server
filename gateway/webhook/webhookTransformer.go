package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/requesttojson"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type sourceTransformAdapter interface {
	getTransformerEvent(authCtx *gwtypes.AuthRequestContext, eventRequest []byte) ([]byte, error)
	getTransformerURL(sourceType string) (string, error)
	getAdapterVersion() string
}

// ----- v1 adapter ---------

type v1Adapter struct{}

type V1TransformerEvent struct {
	EventRequest json.RawMessage       `json:"event"`
	Source       backendconfig.SourceT `json:"source"`
}

func (v1 *v1Adapter) getTransformerEvent(authCtx *gwtypes.AuthRequestContext, eventRequest []byte) ([]byte, error) {
	source := authCtx.Source

	v1TransformerEvent := V1TransformerEvent{
		EventRequest: eventRequest,
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

func (v1 *v1Adapter) getAdapterVersion() string {
	return transformer.V1
}

// ----- v2 adapter -----

type v2Adapter struct{}

type V2TransformerEvent struct {
	EventRequest json.RawMessage       `json:"request"`
	Source       backendconfig.SourceT `json:"source"`
}

func (v2 *v2Adapter) getTransformerEvent(authCtx *gwtypes.AuthRequestContext, eventRequest []byte) ([]byte, error) {
	source := authCtx.Source

	v2TransformerEvent := V2TransformerEvent{
		EventRequest: eventRequest,
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

	return json.Marshal(v2TransformerEvent)
}

func (v2 *v2Adapter) getTransformerURL(sourceType string) (string, error) {
	return getTransformerURL(transformer.V2, sourceType)
}

func (v2 *v2Adapter) getAdapterVersion() string {
	return transformer.V2
}

// ------------------------------

func newSourceTransformAdapter(version string) sourceTransformAdapter {
	// V0 Deprecation: this function returns v1 adapter by default, thereby deprecating v0
	if version == transformer.V2 {
		return &v2Adapter{}
	}
	return &v1Adapter{}
}

// --- utilities -----

func getTransformerURL(version, sourceType string) (string, error) {
	baseURL := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	return url.JoinPath(baseURL, version, "sources", strings.ToLower(sourceType))
}

func prepareTransformerEventRequestV1(req *http.Request, sourceType string, sourceListForParsingParams []string) ([]byte, error) {
	defer func() {
		if req.Body != nil {
			_ = req.Body.Close()
		}
	}()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, errors.New(response.RequestBodyReadFailed)
	}

	if len(body) == 0 {
		body = []byte("{}") // If body is empty, set it to an empty JSON object
	}

	if slices.Contains(sourceListForParsingParams, strings.ToLower(sourceType)) {
		queryParams := req.URL.Query()
		return sjson.SetBytes(body, "query_parameters", queryParams)
	}

	return body, nil
}

func prepareTransformerEventRequestV2(req *http.Request) ([]byte, error) {
	requestJson, err := requesttojson.RequestToJSON(req, "{}")
	if err != nil {
		return nil, err
	}

	return json.Marshal(requestJson)
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

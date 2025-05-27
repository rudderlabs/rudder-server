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

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"

	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/requesttojson"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	contentTypeJsonUTF8 = "application/json; charset=utf-8"
)

type sourceTransformAdapter interface {
	getTransformerEvent(authCtx *gwtypes.AuthRequestContext, eventRequest []byte) ([]byte, error)
	getTransformerURL(sourceType string) (string, error)
	getAdapterVersion() string
}

// ----- v1 adapter ---------

type v1Adapter struct {
	baseTransformerURL string
}

type V1TransformerEvent struct {
	EventRequest json.RawMessage       `json:"event"`
	Source       backendconfig.SourceT `json:"source"`
}

func (v1 *v1Adapter) getTransformerEvent(authCtx *gwtypes.AuthRequestContext, eventRequest []byte) ([]byte, error) {
	source := authCtx.SourceDetails

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
		},
	}

	return jsonrs.Marshal(v1TransformerEvent)
}

func (v1 *v1Adapter) getTransformerURL(sourceType string) (string, error) {
	return getTransformerURL(transformer.V1, sourceType, v1.baseTransformerURL)
}

func (v1 *v1Adapter) getAdapterVersion() string {
	return transformer.V1
}

// ----- v2 adapter -----

type v2Adapter struct {
	baseTransformerURL string
}

type V2TransformerEvent struct {
	EventRequest json.RawMessage       `json:"request"`
	Source       backendconfig.SourceT `json:"source"`
}

func (v2 *v2Adapter) getTransformerEvent(authCtx *gwtypes.AuthRequestContext, eventRequest []byte) ([]byte, error) {
	source := authCtx.SourceDetails

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
		},
	}

	return jsonrs.Marshal(v2TransformerEvent)
}

func (v2 *v2Adapter) getTransformerURL(sourceType string) (string, error) {
	return getTransformerURL(transformer.V2, sourceType, v2.baseTransformerURL)
}

func (v2 *v2Adapter) getAdapterVersion() string {
	return transformer.V2
}

// ------------------------------

func newSourceTransformAdapter(version string, conf *config.Config) sourceTransformAdapter {
	// V0 Deprecation: this function returns v1 adapter by default, thereby deprecating v0
	baseTransformerUrl := conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	if version == transformer.V2 {
		return &v2Adapter{
			baseTransformerURL: baseTransformerUrl,
		}
	}
	return &v1Adapter{
		baseTransformerURL: baseTransformerUrl,
	}
}

// --- utilities -----

func getTransformerURL(version, sourceType, baseURL string) (string, error) {
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
		return nil, errors.New(strings.ToLower(response.RequestBodyReadFailed))
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

	return jsonrs.Marshal(requestJson)
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

	var resp *http.Response
	payload := misc.MakeJSONArray(events)
	bt.stats.transformTimerStat.Since(transformStart)
	resp, err := bt.postWithRetry(sourceTransformerURL, bytes.NewBuffer(payload))
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
	err = jsonrs.Unmarshal(respBody, &responses)
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

func (bt *batchWebhookTransformerT) postWithRetry(transformerURL string, body io.Reader) (*http.Response, error) {
	var (
		resp *http.Response
		err  error
	)

	err = backoff.RetryNotify(
		func() error {
			var err error
			req, err := http.NewRequest("POST", transformerURL, body)
			if err == nil {
				req.Header.Set("Content-Type", contentTypeJsonUTF8)
				resp, err = bt.webhook.httpClient.Do(req) // nolint: bodyclose
				if err == nil && resp.StatusCode != http.StatusOK {
					return fmt.Errorf("non-success status code: %d", resp.StatusCode)
				}
			}
			return err
		},
		backoff.WithMaxRetries(
			backoff.NewExponentialBackOff(
				backoff.WithInitialInterval(bt.webhook.config.transformer.initialInterval.Load()),
				backoff.WithMaxInterval(bt.webhook.config.transformer.maxInterval.Load()),
				backoff.WithMaxElapsedTime(bt.webhook.config.transformer.maxElapsedTime.Load()),
				backoff.WithMultiplier(bt.webhook.config.transformer.multiplier.Load()),
			),
			uint64(bt.webhook.config.transformer.maxRetry.Load()),
		),
		func(err error, duration time.Duration) {
			bt.webhook.logger.Warnn("Failed to send events to transformer",
				logger.NewStringField("url", transformerURL),
				logger.NewDurationField("backoffDelay", duration),
				obskit.Error(err),
			)
		},
	)
	return resp, err
}

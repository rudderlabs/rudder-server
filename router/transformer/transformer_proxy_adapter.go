package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/processor/integrations"
)

type transformerProxyAdapter interface {
	getPayload(proxyReqParams *ProxyRequestParams) ([]byte, error)
	getProxyURL(destType string) (string, error)
	getResponse(response []byte, respCode int, metadata []ProxyRequestMetadata) (TransResponse, error)
}

type ProxyRequestPayloadV0 struct {
	integrations.PostParametersT
	Metadata           ProxyRequestMetadata `json:"metadata"`
	DestinationConfig  map[string]any       `json:"destinationConfig"`
	DestinationVersion int                  `json:"destinationVersion,omitempty"`
}

type ProxyResponseV0 struct {
	Message             string            `json:"message"`
	DestinationResponse any               `json:"destinationResponse"`
	AuthErrorCategory   string            `json:"authErrorCategory"`
	StatTags            map[string]string `json:"statTags"`
}

type ProxyResponseV1 struct {
	Message           string            `json:"message"`
	Response          []TPDestResponse  `json:"response"`
	AuthErrorCategory string            `json:"authErrorCategory"`
	StatTags          map[string]string `json:"statTags"`
}

type TransResponse struct {
	routerJobResponseCodes       map[int64]int
	routerJobResponseBodys       map[int64]string
	routerJobDontBatchDirectives map[int64]bool
	authErrorCategory            string
	statTags                     map[string]string
	// jobIDMismatch reports that the transformer returned results for a different set of jobIDs
	// than were sent, meaning per-job results are attributed to the wrong jobs. The adapter only
	// reports it; the caller records the metric, so every breach reason goes through one stats
	// handle. jobIDsInMetadata/jobIDsInResponse are the two sorted sets, and their difference is
	// what makes the breach diagnosable.
	jobIDMismatch    bool
	jobIDsInMetadata []int64
	jobIDsInResponse []int64
}

type TPDestResponse struct {
	StatusCode int                  `json:"statusCode"`
	Metadata   ProxyRequestMetadata `json:"metadata"`
	Error      string               `json:"error"`
}

type (
	v0Adapter struct {
		logger logger.Logger
	}
	v1Adapter struct {
		logger logger.Logger
	}
)

func (v0 *v0Adapter) getPayload(proxyReqParams *ProxyRequestParams) ([]byte, error) {
	params := proxyReqParams.ResponseData
	proxyReqPayload := &ProxyRequestPayloadV0{
		PostParametersT:    params.PostParametersT,
		Metadata:           params.Metadata[0],
		DestinationConfig:  params.DestinationConfig,
		DestinationVersion: params.DestinationVersion,
	}
	return jsonrs.Marshal(proxyReqPayload)
}

func (v0 *v0Adapter) getProxyURL(destType string) (string, error) {
	return getTransformerProxyURL("v0", destType)
}

func (v0 *v0Adapter) getResponse(respData []byte, respCode int, metadata []ProxyRequestMetadata) (TransResponse, error) {
	routerJobResponseCodes := make(map[int64]int)
	routerJobResponseBodys := make(map[int64]string)
	routerJobDontBatchDirectives := make(map[int64]bool)
	for _, m := range metadata {
		routerJobDontBatchDirectives[m.JobID] = m.DontBatch
	}

	transformerResponse := ProxyResponseV0{
		Message: "[TransformerProxy]:: Default Message TransResponseT",
	}
	err := jsonrs.Unmarshal(respData, &transformerResponse)
	if err != nil {
		return TransResponse{
				routerJobResponseCodes:       routerJobResponseCodes,
				routerJobResponseBodys:       routerJobResponseBodys,
				routerJobDontBatchDirectives: routerJobDontBatchDirectives,
				authErrorCategory:            "",
			},
			fmt.Errorf("[TransformerProxy Unmarshalling]:: respData: %s, err: %w", string(respData), err)
	}

	for _, m := range metadata {
		routerJobResponseCodes[m.JobID] = respCode
		routerJobResponseBodys[m.JobID] = string(respData)
	}

	return TransResponse{
			routerJobResponseCodes:       routerJobResponseCodes,
			routerJobResponseBodys:       routerJobResponseBodys,
			routerJobDontBatchDirectives: routerJobDontBatchDirectives,
			authErrorCategory:            transformerResponse.AuthErrorCategory,
			statTags:                     transformerResponse.StatTags,
		},
		nil
}

func (v1 *v1Adapter) getPayload(proxyReqParams *ProxyRequestParams) ([]byte, error) {
	return jsonrs.Marshal(proxyReqParams.ResponseData)
}

func (v1 *v1Adapter) getProxyURL(destType string) (string, error) {
	return getTransformerProxyURL("v1", destType)
}

func (v1 *v1Adapter) getResponse(respData []byte, respCode int, metadata []ProxyRequestMetadata) (TransResponse, error) {
	routerJobResponseCodes := make(map[int64]int)
	routerJobResponseBodys := make(map[int64]string)
	routerJobDontBatchDirectives := make(map[int64]bool)
	for _, m := range metadata {
		routerJobDontBatchDirectives[m.JobID] = m.DontBatch
	}

	transformerResponse := ProxyResponseV1{
		Message: "[TransformerProxy]:: Default Message TransResponseT",
	}
	err := jsonrs.Unmarshal(respData, &transformerResponse)
	if err != nil {
		return TransResponse{
				routerJobResponseCodes:       routerJobResponseCodes,
				routerJobResponseBodys:       routerJobResponseBodys,
				routerJobDontBatchDirectives: routerJobDontBatchDirectives,
				authErrorCategory:            "",
			},
			fmt.Errorf("[TransformerProxy Unmarshalling]:: respData: %s, err: %w", string(respData), err)
	}

	// Compared as sets: a batch can legitimately carry the same JobID more than once (router/worker.go
	// only dedupes when building the final job responses), and the transformer may either collapse
	// those duplicates or echo one entry per input item. Both are valid, so deduping only one side
	// would make the comparison sensitive to which one it does - and this now pages.
	jobIDsInMetadata := lo.Uniq(lo.Map(metadata, func(m ProxyRequestMetadata, _ int) int64 {
		return m.JobID
	}))
	slices.Sort(jobIDsInMetadata)
	jobIDsInResponse := lo.Uniq(lo.Map(transformerResponse.Response, func(resp TPDestResponse, _ int) int64 {
		return resp.Metadata.JobID
	}))
	slices.Sort(jobIDsInResponse)

	// Report the mismatch rather than recording it here. Emitting from the adapter would write this
	// metric through the package-global stats handle while the other breach reasons use the
	// caller's, splitting one metric across two sinks and hiding this reason from an injected mock.
	jobIDMismatch := !reflect.DeepEqual(jobIDsInMetadata, jobIDsInResponse)

	for _, resp := range transformerResponse.Response {
		routerJobResponseCodes[resp.Metadata.JobID] = resp.StatusCode
		routerJobResponseBodys[resp.Metadata.JobID] = resp.Error
		routerJobDontBatchDirectives[resp.Metadata.JobID] = resp.Metadata.DontBatch
	}

	return TransResponse{
			routerJobResponseCodes:       routerJobResponseCodes,
			routerJobResponseBodys:       routerJobResponseBodys,
			routerJobDontBatchDirectives: routerJobDontBatchDirectives,
			authErrorCategory:            transformerResponse.AuthErrorCategory,
			statTags:                     transformerResponse.StatTags,
			jobIDMismatch:                jobIDMismatch,
			jobIDsInMetadata:             jobIDsInMetadata,
			jobIDsInResponse:             jobIDsInResponse,
		},
		nil
}

// router/transformer/transformer_proxy_adapter.go
// getTransformerProxyURL constructs the transformer proxy URL, prioritizing DELIVERY_TRANSFORMER_URL for dedicated deployments.
// Prefer DELIVERY_TRANSFORMER_URL for dedicated deployments, fallback to DEST_TRANSFORM_URL for backward compatibility
func getTransformerProxyURL(version, destType string) (string, error) {
	baseURL := config.GetStringVar("", "DELIVERY_TRANSFORMER_URL")
	if baseURL == "" {
		baseURL = config.GetStringVar("http://localhost:9090", "DEST_TRANSFORM_URL")
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	return url.JoinPath(baseURL, version, "destinations", strings.ToLower(destType), "proxy")
}

func NewTransformerProxyAdapter(version string, logger logger.Logger) transformerProxyAdapter {
	switch version {
	case "v1":
		return &v1Adapter{logger: logger}
	}
	return &v0Adapter{logger: logger}
}

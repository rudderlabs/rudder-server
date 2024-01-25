package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/processor/integrations"
)

type transformerProxyAdapter interface {
	getPayload(proxyReqParams *ProxyRequestParams) ([]byte, error)
	getProxyURL(destType string) (string, error)
	getResponse(response []byte, respCode int, metadata []ProxyRequestMetadata) (TransResponse, error)
}

type ProxyRequestPayloadV0 struct {
	integrations.PostParametersT
	Metadata          ProxyRequestMetadata   `json:"metadata"`
	DestinationConfig map[string]interface{} `json:"destinationConfig"`
}

type ProxyResponseV0 struct {
	Message             string      `json:"message"`
	DestinationResponse interface{} `json:"destinationResponse"`
	AuthErrorCategory   string      `json:"authErrorCategory"`
}

type ProxyResponseV1 struct {
	Message           string           `json:"message"`
	Response          []TPDestResponse `json:"response"`
	AuthErrorCategory string           `json:"authErrorCategory"`
}

type TransResponse struct {
	routerJobResponseCodes       map[int64]int
	routerJobResponseBodys       map[int64]string
	routerJobDontBatchDirectives map[int64]bool
	authErrorCategory            string
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
	proxyReqPayload := &ProxyRequestPayloadV0{
		PostParametersT:   proxyReqParams.ResponseData.PostParametersT,
		Metadata:          proxyReqParams.ResponseData.Metadata[0],
		DestinationConfig: proxyReqParams.ResponseData.DestinationConfig,
	}
	return jsonfast.Marshal(proxyReqPayload)
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
	err := jsonfast.Unmarshal(respData, &transformerResponse)
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
		},
		nil
}

func (v1 *v1Adapter) getPayload(proxyReqParams *ProxyRequestParams) ([]byte, error) {
	return jsonfast.Marshal(proxyReqParams.ResponseData)
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
	err := jsonfast.Unmarshal(respData, &transformerResponse)
	if err != nil {
		return TransResponse{
				routerJobResponseCodes:       routerJobResponseCodes,
				routerJobResponseBodys:       routerJobResponseBodys,
				routerJobDontBatchDirectives: routerJobDontBatchDirectives,
				authErrorCategory:            "",
			},
			fmt.Errorf("[TransformerProxy Unmarshalling]:: respData: %s, err: %w", string(respData), err)
	}

	jobIDsInMetadata := lo.Map(metadata, func(m ProxyRequestMetadata, _ int) int64 {
		return m.JobID
	})
	sort.Slice(jobIDsInMetadata, func(i, j int) bool {
		return jobIDsInMetadata[i] < jobIDsInMetadata[j]
	})
	jobIDsInResponse := lo.Map(transformerResponse.Response, func(resp TPDestResponse, _ int) int64 {
		return resp.Metadata.JobID
	})
	sort.Slice(jobIDsInResponse, func(i, j int) bool {
		return jobIDsInResponse[i] < jobIDsInResponse[j]
	})

	if !reflect.DeepEqual(jobIDsInMetadata, jobIDsInResponse) {
		stats.Default.NewTaggedStat(`router.transformerproxy.invalid.response`, stats.CountType, stats.Tags{
			"reason": "in out mismatch",
		}).Increment()
		v1.logger.Warnf("[TransformerProxy] JobIDs in out mismatch. jobIDsInMetadata: %v, jobIDsInResponse: %v", jobIDsInMetadata, jobIDsInResponse)
	}

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
		},
		nil
}

func getTransformerProxyURL(version, destType string) (string, error) {
	baseURL := strings.TrimSuffix(config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"), "/")
	return url.JoinPath(baseURL, version, "destinations", strings.ToLower(destType), "proxy")
}

func NewTransformerProxyAdapter(version string, logger logger.Logger) transformerProxyAdapter {
	switch version {
	case "v1":
		return &v1Adapter{logger: logger}
	}
	return &v0Adapter{logger: logger}
}

//go:generate mockgen --build_flags=--mod=mod -destination=../../mocks/services/transformer/mock_features.go -package mock_features github.com/rudderlabs/rudder-server/services/transformer FeaturesService

package transformer

import (
	"context"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/rruntime"
)

const (
	V0 = "v0"
	V1 = "v1"
	V2 = "v2"
)

type FeaturesServiceOptions struct {
	PollInterval             time.Duration
	TransformerURL           string
	FeaturesRetryMaxAttempts int
}

type FeaturesService interface {
	Regulations() []string
	SourceTransformerVersion() string
	RouterTransform(destType string) bool
	// TransformerProxy reports whether the transformer declares destType deliverable through the
	// transformer proxy. Distinct from TransformerProxyVersion, which reports the proxy protocol
	// the transformer speaks.
	TransformerProxy(destType string) bool
	TransformerProxyVersion() string
	SupportDestTransformCompactedPayloadV1() bool
	Wait() chan struct{}
}

// defaultTransformerFeatures is the feature set served before the first successful /features fetch.
var defaultTransformerFeatures = &featuresPayload{
	RouterTransform: map[string]bool{
		"MARKETO": true,
		"HS":      true,
	},
	Regulations:                 []string{"AM"},
	SupportSourceTransformV1:    true,
	UpgradedToSourceTransformV2: true,
}

func NewFeaturesService(ctx context.Context, config *config.Config, featConfig FeaturesServiceOptions) FeaturesService {
	handler := &featuresService{
		logger:   logger.NewLogger().Child("transformer-features"),
		waitChan: make(chan struct{}),
		options:  featConfig,
		client: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives:   config.GetBoolVar(true, "Transformer.Client.disableKeepAlives"),
				MaxConnsPerHost:     config.GetIntVar(100, 1, "Transformer.Client.maxHTTPConnections"),
				MaxIdleConnsPerHost: config.GetIntVar(10, 1, "Transformer.Client.maxHTTPIdleConnections"),
				IdleConnTimeout:     config.GetDurationVar(30, time.Second, "Transformer.Client.maxIdleConnDuration"),
			},
			Timeout: config.GetDurationVar(30, time.Second, "HttpClient.processor.timeout"),
		},
	}
	handler.features.Store(defaultTransformerFeatures)

	rruntime.Go(func() { handler.syncTransformerFeatureJson(ctx) })

	return handler
}

func NewNoOpService() FeaturesService {
	return &noopService{}
}

type noopService struct{}

func (*noopService) Regulations() []string {
	return []string{}
}

func (*noopService) SourceTransformerVersion() string {
	// v0 is deprecated and upgrading to v2
	return V2
}

func (*noopService) TransformerProxyVersion() string {
	return V0
}

func (*noopService) Wait() chan struct{} {
	dummyChan := make(chan struct{})
	close(dummyChan)
	return dummyChan
}

func (*noopService) RouterTransform(_ string) bool {
	return false
}

func (*noopService) TransformerProxy(_ string) bool {
	return false
}

func (*noopService) SupportDestTransformCompactedPayloadV1() bool {
	return false
}

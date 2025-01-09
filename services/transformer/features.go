//go:generate mockgen --build_flags=--mod=mod -destination=../../mocks/services/transformer/mock_features.go -package mock_features github.com/rudderlabs/rudder-server/services/transformer FeaturesService

package transformer

import (
	"context"
	"encoding/json"
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
	TransformerProxyVersion() string
	Wait() chan struct{}
}

var defaultTransformerFeatures = `{
	"routerTransform": {
	  "MARKETO": true,
	  "HS": true
	},
	"regulations": ["AM"],
	"supportSourceTransformV1": true,
	"upgradedToSourceTransformV2": false,
  }`

func NewFeaturesService(ctx context.Context, config *config.Config, featConfig FeaturesServiceOptions) FeaturesService {
	handler := &featuresService{
		features: json.RawMessage(defaultTransformerFeatures),
		logger:   logger.NewLogger().Child("transformer-features"),
		waitChan: make(chan struct{}),
		options:  featConfig,
		client: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives:   config.GetBool("Transformer.Client.disableKeepAlives", true),
				MaxConnsPerHost:     config.GetInt("Transformer.Client.maxHTTPConnections", 100),
				MaxIdleConnsPerHost: config.GetInt("Transformer.Client.maxHTTPIdleConnections", 10),
				IdleConnTimeout:     config.GetDuration("Transformer.Client.maxIdleConnDuration", 30, time.Second),
			},
			Timeout: config.GetDuration("HttpClient.processor.timeout", 30, time.Second),
		},
	}

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

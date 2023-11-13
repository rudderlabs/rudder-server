package transformer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/rruntime"
)

const (
	V0 = "v0"
	V1 = "v1"
)

type FeaturesServiceConfig struct {
	PollInterval             time.Duration
	TransformerURL           string
	FeaturesRetryMaxAttempts int
}

type FeaturesService interface {
	SourceTransformerVersion() string
	RouterTransform(destType string) bool
	TransformerProxyVersion() string
	Wait() chan struct{}
}

var defaultTransformerFeatures = `{
	"routerTransform": {
	  "MARKETO": true,
	  "HS": true
	}
  }`

func NewFeaturesService(ctx context.Context, config FeaturesServiceConfig) FeaturesService {
	handler := &featuresService{
		features: json.RawMessage(defaultTransformerFeatures),
		logger:   logger.NewLogger().Child("transformer-features"),
		waitChan: make(chan struct{}),
		config:   config,
	}

	rruntime.Go(func() { handler.syncTransformerFeatureJson(ctx) })

	return handler
}

func NewNoOpService() FeaturesService {
	return &noopService{}
}

type noopService struct{}

func (*noopService) SourceTransformerVersion() string {
	return V0
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

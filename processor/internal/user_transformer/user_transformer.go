package user_transformer

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/internal/http_client"
	"github.com/rudderlabs/rudder-server/processor/types"
)

type UserTransformer struct {
	config struct {
		userTransformationURL      string
		maxRetry                   config.ValueLoader[int]
		failOnUserTransformTimeout config.ValueLoader[bool]
		failOnError                config.ValueLoader[bool]
		maxRetryBackoffInterval    config.ValueLoader[time.Duration]
	}
	conf   *config.Config
	log    logger.Logger
	stat   stats.Stats
	client http_client.HTTPDoer
}

func (u *UserTransformer) SendRequest(ctx context.Context, clientEvents []types.TransformerEvent, batchSize int) types.Response {
	_ = u.userTransformURL()
	return types.Response{}
}

func NewUserTransformer(conf *config.Config, log logger.Logger, stat stats.Stats) *UserTransformer {
	handle := &UserTransformer{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.client = http_client.NewHTTPClient(conf)
	handle.config.userTransformationURL = handle.conf.GetString("USER_TRANSFORM_URL", "http://localhost:9090")
	handle.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnUserTransformTimeout")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.maxRetryBackoffInterval")
	return handle
}

func (u *UserTransformer) userTransformURL() string {
	return u.config.userTransformationURL + "/customTransform"
}

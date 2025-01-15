package trackingplan_validation

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-server/processor/types"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/internal/http_client"
)

type TPValidator struct {
	config struct {
		destTransformationURL   string
		maxRetry                config.ValueLoader[int]
		failOnError             config.ValueLoader[bool]
		maxRetryBackoffInterval config.ValueLoader[time.Duration]
	}
	conf   *config.Config
	log    logger.Logger
	stat   stats.Stats
	client http_client.HTTPDoer
}

func (t *TPValidator) SendRequest(ctx context.Context, clientEvents []types.TransformerEvent, batchSize int) types.Response {
	_ = t.trackingPlanValidationURL()
	return types.Response{}
}

func NewTPValidator(conf *config.Config, log logger.Logger, stat stats.Stats) *TPValidator {
	handle := &TPValidator{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.client = http_client.NewHTTPClient(conf)
	handle.config.destTransformationURL = handle.conf.GetString("Warehouse.destTransformationURL", "http://localhost:9090")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.maxRetryBackoffInterval")
	return handle
}

func (t *TPValidator) trackingPlanValidationURL() string {
	return t.config.destTransformationURL + "/v0/validate"
}

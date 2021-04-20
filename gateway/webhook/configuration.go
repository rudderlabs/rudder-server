package webhook

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

func loadConfig() {
	config.Initialize()

	sourceTransformerURL = strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/v0/sources"
	// Number of incoming webhooks that are batched before calling source transformer
	maxWebhookBatchSize = config.GetInt("Gateway.webhook.maxBatchSize", 32)
	config.RegisterIntConfigVariable(32, &maxWebhookBatchSize, true, 1, "Gateway.webhook.maxBatchSize")
	// Timeout after which batch is formed anyway with whatever webhooks are available
	config.RegisterDurationConfigVariable(time.Duration(20), &webhookBatchTimeout, true, time.Millisecond, "Gateway.webhook.batchTimeoutInMS")
	// Multiple source transformers are used to generate rudder events from webhooks
	maxTransformerProcess = config.GetInt("Gateway.webhook.maxTransformerProcess", 64)
	// Max time till when retries to source transformer are done
	webhookRetryWaitMax = (config.GetDuration("Gateway.webhook.maxRetryTimeInS", time.Duration(10)) * time.Second)
	// Max retry attempts to source transformer
	webhookRetryMax = config.GetInt("Gateway.webhook.maxRetry", 5)
}

func webhookReloadableConfig() {
	_webhookBatchTimeout := (config.GetDuration("Gateway.webhook.batchTimeoutInMS", time.Duration(20)) * time.Millisecond)
	if _webhookBatchTimeout != webhookBatchTimeout {
		webhookBatchTimeout = _webhookBatchTimeout
		pkgLogger.Info("Gateway.webhook.batchTimeoutInMS changes to ", webhookBatchTimeout)
	}
	_maxWebhookBatchSize := config.GetInt("Gateway.webhook.maxBatchSize", 32)
	if _maxWebhookBatchSize != maxWebhookBatchSize {
		maxWebhookBatchSize = _maxWebhookBatchSize
		pkgLogger.Info("Gateway.webhook.maxBatchSize changes to ", maxWebhookBatchSize)
	}
}

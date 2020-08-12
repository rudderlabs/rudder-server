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
	// Timeout after which batch is formed anyway with whatever webhooks are available
	webhookBatchTimeout = (config.GetDuration("Gateway.webhook.batchTimeoutInMS", time.Duration(20)) * time.Millisecond)
	// Multiple source transformers are used to generate rudder events from webhooks
	maxTransformerProcess = config.GetInt("Gateway.webhook.maxTransformerProcess", 64)
	// Max time till when retries to source transformer are done
	webhookRetryWaitMax = (config.GetDuration("Gateway.webhook.maxRetryTimeInS", time.Duration(10)) * time.Second)
	// Max retry attempts to source transformer
	webhookRetryMax = config.GetInt("Gateway.webhook.maxRetry", 5)
}

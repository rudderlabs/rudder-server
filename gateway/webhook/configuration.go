package webhook

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// nolint:staticcheck // SA1019: config Register reloadable functions are deprecated
func loadConfig() {
	sourceTransformerURL = strings.TrimSuffix(config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/v0/sources"
	// Number of incoming webhooks that are batched before calling source transformer
	config.RegisterIntConfigVariable(32, &maxWebhookBatchSize, true, 1, "Gateway.webhook.maxBatchSize")
	// Timeout after which batch is formed anyway with whatever webhooks are available
	config.RegisterDurationConfigVariable(20, &webhookBatchTimeout, true, time.Millisecond, []string{"Gateway.webhook.batchTimeout", "Gateway.webhook.batchTimeoutInMS"}...)
	// Multiple source transformers are used to generate rudder events from webhooks
	config.RegisterIntConfigVariable(64, &maxTransformerProcess, false, 1, "Gateway.webhook.maxTransformerProcess")
	// Max time till when retries to source transformer are done
	config.RegisterDurationConfigVariable(10, &webhookRetryWaitMax, false, time.Second, []string{"Gateway.webhook.maxRetryTime", "Gateway.webhook.maxRetryTimeInS"}...)
	// Min time gap when retries to source transformer are done
	config.RegisterDurationConfigVariable(100, &webhookRetryWaitMin, false, time.Millisecond, []string{"Gateway.webhook.minRetryTime", "Gateway.webhook.minRetryTimeInMS"}...)
	// Max retry attempts to source transformer
	config.RegisterIntConfigVariable(5, &webhookRetryMax, false, 1, "Gateway.webhook.maxRetry")
	// Parse all query params from sources mentioned in this list
	config.RegisterStringSliceConfigVariable(make([]string, 0), &sourceListForParsingParams, false, "Gateway.webhook.sourceListForParsingParams")
	// lowercasing the strings in sourceListForParsingParams
	for i, s := range sourceListForParsingParams {
		sourceListForParsingParams[i] = strings.ToLower(s)
	}
}

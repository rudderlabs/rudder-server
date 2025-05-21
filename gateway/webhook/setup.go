//go:generate mockgen --build_flags=--mod=mod -destination=./../mocks/mockwebhook.go -package mockwebhook github.com/rudderlabs/rudder-server/gateway/webhook Gateway

package webhook

import (
	"context"
	"net/http"
	"strings"
	"time"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/crash"
)

type Gateway interface {
	RequestMetricsTracker
	WebhookRequestProcessor
}

type WebhookRequestProcessor interface {
	// ProcessTransformedWebhookRequest processes the transformed webhook request and save it to the gw jobsDB
	ProcessTransformedWebhookRequest(writer *http.ResponseWriter, req *http.Request, reqType string, requestPayload []byte, arctx *gwtypes.AuthRequestContext) string
	// SaveWebhookFailures saves the webhook failures to the procErr jobsDB
	SaveWebhookFailures([]*model.FailedWebhookPayload) error
}

// StatReporterCreator is a function type that creates StatReporter instances
type StatReporterCreator func(authContext *gwtypes.AuthRequestContext, requestType string) gwtypes.StatReporter

// RequestMetricsTracker is used to track webhook request metrics on a request basis for OSS customers
type RequestMetricsTracker interface {
	TrackRequestMetrics(errorMessage string)
}

type TransformerFeaturesService interface {
	SourceTransformerVersion() string
}

func newWebhookStats(stat stats.Stats) *webhookStatsT {
	wStats := webhookStatsT{}
	wStats.sentStat = stat.NewStat("webhook.transformer_sent", stats.CountType)
	wStats.receivedStat = stat.NewStat("webhook.transformer_received", stats.CountType)
	wStats.failedStat = stat.NewStat("webhook.transformer_failed", stats.CountType)
	wStats.transformTimerStat = stat.NewStat("webhook.transformation_time", stats.TimerType)
	wStats.sourceStats = make(map[string]*webhookSourceStatT)
	return &wStats
}

func Setup(gwHandle Gateway, transformerFeaturesService TransformerFeaturesService, stat stats.Stats, conf *config.Config, statReporterCreator StatReporterCreator, opts ...batchTransformerOption) *HandleT {
	webhook := &HandleT{gwHandle: gwHandle, stats: stat, logger: logger.NewLogger().Child("gateway").Child("webhook")}
	// Number of incoming webhooks that are batched before calling source transformer
	webhook.config.maxWebhookBatchSize = conf.GetReloadableIntVar(32, 1, "Gateway.webhook.maxBatchSize")
	// Timeout after which batch is formed anyway with whatever webhooks are available
	webhook.config.webhookBatchTimeout = conf.GetReloadableDurationVar(20, time.Millisecond, []string{"Gateway.webhook.batchTimeout", "Gateway.webhook.batchTimeoutInMS"}...)
	// Multiple source transformers are used to generate rudder events from webhooks
	maxTransformerProcess := conf.GetIntVar(64, 1, "Gateway.webhook.maxTransformerProcess")
	// Parse all query params from sources mentioned in this list
	webhook.config.sourceListForParsingParams = conf.GetStringSliceVar([]string{"Shopify", "adjust"}, "Gateway.webhook.sourceListForParsingParams")
	// Maximum request size to gateway
	webhook.config.maxReqSize = conf.GetReloadableIntVar(4000, 1024, "Gateway.maxReqSizeInKB")

	webhook.config.forwardGetRequestForSrcMap = lo.SliceToMap(
		conf.GetStringSliceVar([]string{"adjust"}, "Gateway.webhook.forwardGetRequestForSrcs"),
		func(item string) (string, struct{}) {
			return strings.ToLower(item), struct{}{}
		},
	)
	// enable webhook v2 handler
	webhook.config.webhookV2HandlerEnabled = conf.GetBoolVar(true, "Gateway.webhookV2HandlerEnabled")
	webhook.config.cslbEnabled = conf.GetReloadableBoolVar(true, "Gateway.webhook.cslbEnabled")

	// lowercasing the strings in sourceListForParsingParams
	for i, s := range webhook.config.sourceListForParsingParams {
		webhook.config.sourceListForParsingParams[i] = strings.ToLower(s)
	}

	webhook.requestQ = make(map[string]chan *webhookT)
	webhook.batchRequestQ = make(chan *batchWebhookT)
	webhook.netClient = retryablehttp.NewClient()
	webhook.netClient.HTTPClient.Timeout = conf.GetDuration("HttpClient.webhook.timeout", 30, time.Second)
	webhook.netClient.Logger = nil // to avoid debug logs
	webhook.netClient.RetryWaitMin = conf.GetDurationVar(100, time.Millisecond, []string{"Gateway.webhook.minRetryTime", "Gateway.webhook.minRetryTimeInMS"}...)
	webhook.netClient.RetryWaitMax = conf.GetDurationVar(10, time.Second, []string{"Gateway.webhook.maxRetryTime", "Gateway.webhook.maxRetryTimeInS"}...)
	webhook.netClient.RetryMax = conf.GetIntVar(5, 1, "Gateway.webhook.maxRetry")

	webhook.httpClient = transformerclient.NewRetryableClient(transformerHttpClientConfig(conf))
	ctx, cancel := context.WithCancel(context.Background())
	webhook.backgroundCancel = cancel

	webhook.statReporterCreator = statReporterCreator

	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < maxTransformerProcess; i++ {
		g.Go(crash.Wrapper(func() error {
			bt := batchWebhookTransformerT{
				webhook: webhook,
				stats:   newWebhookStats(stat),
				sourceTransformAdapter: func(ctx context.Context) (sourceTransformAdapter, error) {
					return newSourceTransformAdapter(transformerFeaturesService.SourceTransformerVersion(), conf), nil
				},
			}
			for _, opt := range opts {
				opt(&bt)
			}
			bt.batchTransformLoop()
			return nil
		}))
	}
	g.Go(crash.Wrapper(func() error {
		webhook.printStats(ctx)
		return nil
	}))

	webhook.backgroundWait = g.Wait
	return webhook
}

func transformerHttpClientConfig(conf *config.Config) *transformerclient.RetryableClientConfig {
	transformerClientConfig := &transformerclient.ClientConfig{
		ClientTimeout: conf.GetDurationVar(30, time.Second, "HttpClient.sourceTransformer.timeout", "HttpClient.webhook.timeout"),
		ClientTTL:     conf.GetDurationVar(10, time.Second, "Transformer.Client.sourceTransformer.ttl", "Transformer.Client.ttl"),
		ClientType:    conf.GetStringVar("httplb", "Transformer.Client.sourceTransformer.type", "Transformer.Client.type"),
		PickerType:    conf.GetStringVar("power_of_two", "Transformer.Client.sourceTransformer.httplb.pickerType", "Transformer.Client.httplb.pickerType"),
	}
	transformerClientConfig.TransportConfig.DisableKeepAlives = conf.GetBoolVar(true, "Transformer.Client.sourceTransformer.disableKeepAlives", "Transformer.Client.disableKeepAlives")
	transformerClientConfig.TransportConfig.MaxConnsPerHost = conf.GetIntVar(100, 1, "Transformer.Client.sourceTransformer.maxHTTPConnections", "Transformer.Client.maxHTTPConnections")
	transformerClientConfig.TransportConfig.MaxIdleConnsPerHost = conf.GetIntVar(1, 1, "Transformer.Client.sourceTransformer.maxHTTPIdleConnections", "Transformer.Client.maxHTTPIdleConnections")
	transformerClientConfig.TransportConfig.IdleConnTimeout = conf.GetDurationVar(5, time.Second, "Transformer.Client.sourceTransformer.maxIdleConnDuration", "Transformer.Client.maxIdleConnDuration")
	transformerClientConfig.Recycle = conf.GetBoolVar(false, "Transformer.Client.sourceTransformer.recycle", "Transformer.Client.recycle")
	transformerClientConfig.RecycleTTL = conf.GetDurationVar(60, time.Second, "Transformer.Client.sourceTransformer.recycleTTL", "Transformer.Client.recycleTTL")
	transformerRetryableClientConf := &transformerclient.RetryableClientConfig{
		ClientConfig: transformerClientConfig,
		RetryMax:     conf.GetIntVar(5, 1, "Gateway.webhook.maxRetry"),
		RetryWaitMin: conf.GetDurationVar(100, time.Millisecond, "Gateway.webhook.minRetryTime", "Gateway.webhook.minRetryTimeInMS"),
		RetryWaitMax: conf.GetDurationVar(10, time.Second, "Gateway.webhook.maxRetryTime", "Gateway.webhook.maxRetryTimeInS"),
	}
	return transformerRetryableClientConf
}

//go:generate mockgen --build_flags=--mod=mod -destination=./../mocks/mockwebhook.go -package mockwebhook github.com/rudderlabs/rudder-server/gateway/webhook Gateway

package webhook

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/crash"
)

type Gateway interface {
	TrackRequestMetrics(errorMessage string)
	ProcessWebRequest(writer *http.ResponseWriter, req *http.Request, reqType string, requestPayload []byte, arctx *gwtypes.AuthRequestContext) string
	NewSourceStat(arctx *gwtypes.AuthRequestContext, reqType string) *gwstats.SourceStat
	SaveWebhookFailures([]*model.FailedWebhookPayload) error
}

type WebHookI interface {
	RequestHandler(w http.ResponseWriter, r *http.Request)
	Register(name string)
}

func newWebhookStats() *webhookStatsT {
	wStats := webhookStatsT{}
	wStats.sentStat = stats.Default.NewStat("webhook.transformer_sent", stats.CountType)
	wStats.receivedStat = stats.Default.NewStat("webhook.transformer_received", stats.CountType)
	wStats.failedStat = stats.Default.NewStat("webhook.transformer_failed", stats.CountType)
	wStats.transformTimerStat = stats.Default.NewStat("webhook.transformation_time", stats.TimerType)
	wStats.sourceStats = make(map[string]*webhookSourceStatT)
	return &wStats
}

func Setup(gwHandle Gateway, transformerFeaturesService transformer.FeaturesService, stat stats.Stats, opts ...batchTransformerOption) *HandleT {
	webhook := &HandleT{gwHandle: gwHandle, stats: stat, logger: logger.NewLogger().Child("gateway").Child("webhook")}
	// Number of incoming webhooks that are batched before calling source transformer
	webhook.config.maxWebhookBatchSize = config.GetReloadableIntVar(32, 1, "Gateway.webhook.maxBatchSize")
	// Timeout after which batch is formed anyway with whatever webhooks are available
	webhook.config.webhookBatchTimeout = config.GetReloadableDurationVar(20, time.Millisecond, []string{"Gateway.webhook.batchTimeout", "Gateway.webhook.batchTimeoutInMS"}...)
	// Multiple source transformers are used to generate rudder events from webhooks
	maxTransformerProcess := config.GetIntVar(64, 1, "Gateway.webhook.maxTransformerProcess")
	// Parse all query params from sources mentioned in this list
	webhook.config.sourceListForParsingParams = config.GetStringSliceVar([]string{"Shopify", "adjust"}, "Gateway.webhook.sourceListForParsingParams")
	// Maximum request size to gateway
	webhook.config.maxReqSize = config.GetReloadableIntVar(4000, 1024, "Gateway.maxReqSizeInKB")

	webhook.config.forwardGetRequestForSrcMap = lo.SliceToMap(
		config.GetStringSliceVar([]string{"adjust"}, "Gateway.webhook.forwardGetRequestForSrcs"),
		func(item string) (string, struct{}) {
			return strings.ToLower(item), struct{}{}
		},
	)

	// lowercasing the strings in sourceListForParsingParams
	for i, s := range webhook.config.sourceListForParsingParams {
		webhook.config.sourceListForParsingParams[i] = strings.ToLower(s)
	}

	webhook.requestQ = make(map[string]chan *webhookT)
	webhook.batchRequestQ = make(chan *batchWebhookT)
	webhook.netClient = retryablehttp.NewClient()
	webhook.netClient.HTTPClient.Timeout = config.GetDuration("HttpClient.webhook.timeout", 30, time.Second)
	webhook.netClient.Logger = nil // to avoid debug logs
	webhook.netClient.RetryWaitMin = config.GetDurationVar(100, time.Millisecond, []string{"Gateway.webhook.minRetryTime", "Gateway.webhook.minRetryTimeInMS"}...)
	webhook.netClient.RetryWaitMax = config.GetDurationVar(10, time.Second, []string{"Gateway.webhook.maxRetryTime", "Gateway.webhook.maxRetryTimeInS"}...)
	webhook.netClient.RetryMax = config.GetIntVar(5, 1, "Gateway.webhook.maxRetry")

	ctx, cancel := context.WithCancel(context.Background())
	webhook.backgroundCancel = cancel

	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < maxTransformerProcess; i++ {
		g.Go(crash.Wrapper(func() error {
			bt := batchWebhookTransformerT{
				webhook: webhook,
				stats:   newWebhookStats(),
				sourceTransformAdapter: func(ctx context.Context) (sourceTransformAdapter, error) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-transformerFeaturesService.Wait():
						return newSourceTransformAdapter(transformerFeaturesService.SourceTransformerVersion()), nil
					}
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

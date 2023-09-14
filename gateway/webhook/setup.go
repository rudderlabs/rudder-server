//go:generate mockgen --build_flags=--mod=mod -destination=./../mocks/mockwebhook.go -package mockwebhook github.com/rudderlabs/rudder-server/gateway/webhook Gateway

package webhook

import (
	"context"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/gateway/webhook/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type Gateway interface {
	TrackRequestMetrics(errorMessage string)
	ProcessWebRequest(writer *http.ResponseWriter, req *http.Request, reqType string, requestPayload []byte, arctx *gwtypes.AuthRequestContext) string
	NewSourceStat(arctx *gwtypes.AuthRequestContext, reqType string) *gwstats.SourceStat
	SaveWebhookFailures([]*model.FailedWebhookPayload) error
	GetSourceConfig(sourceID string) (*backendconfig.SourceT, error)
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

func Setup(gwHandle Gateway, stat stats.Stats, opts ...batchTransformerOption) *HandleT {
	webhook := &HandleT{gwHandle: gwHandle, stats: stat}
	webhook.requestQ = make(map[string]chan *webhookT)
	webhook.batchRequestQ = make(chan *batchWebhookT)
	webhook.netClient = retryablehttp.NewClient()
	webhook.netClient.HTTPClient.Timeout = config.GetDuration("HttpClient.webhook.timeout", 30, time.Second)
	webhook.netClient.Logger = nil // to avoid debug logs
	webhook.netClient.RetryWaitMin = webhookRetryWaitMin
	webhook.netClient.RetryWaitMax = webhookRetryWaitMax
	webhook.netClient.RetryMax = webhookRetryMax

	ctx, cancel := context.WithCancel(context.Background())
	webhook.backgroundCancel = cancel

	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < maxTransformerProcess; i++ {
		g.Go(misc.WithBugsnag(func() error {
			bt := batchWebhookTransformerT{
				webhook:              webhook,
				stats:                newWebhookStats(),
				sourceTransformerURL: sourceTransformerURL,
			}
			for _, opt := range opts {
				opt(&bt)
			}
			bt.batchTransformLoop()
			return nil
		}))
	}
	g.Go(misc.WithBugsnag(func() error {
		webhook.printStats(ctx)
		return nil
	}))

	webhook.backgroundWait = g.Wait
	return webhook
}

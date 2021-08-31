package webhook

import (
	"context"
	"net/http"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"
)

type GatewayI interface {
	IncrementRecvCount(count uint64)
	IncrementAckCount(count uint64)
	UpdateSourceStats(writeKeyStats map[string]int, bucket string, sourceTagMap map[string]string)
	TrackRequestMetrics(errorMessage string)
	ProcessWebRequest(writer *http.ResponseWriter, req *http.Request, reqType string, requestPayload []byte, writeKey string) string
	GetWebhookSourceDefName(writeKey string) (name string, ok bool)
}

type WebHookI interface {
	RequestHandler(w http.ResponseWriter, r *http.Request)
	Register(name string)
}

func Setup(gwHandle GatewayI) *HandleT {
	webhook := &HandleT{gwHandle: gwHandle}
	webhook.requestQ = make(map[string](chan *webhookT))
	webhook.batchRequestQ = make(chan *batchWebhookT)
	webhook.netClient = retryablehttp.NewClient()
	webhook.netClient.Logger = nil // to avoid debug logs
	webhook.netClient.RetryWaitMin = webhookRetryWaitMin
	webhook.netClient.RetryWaitMax = webhookRetryWaitMax
	webhook.netClient.RetryMax = webhookRetryMax

	ctx, cancel := context.WithCancel(context.Background())
	webhook.backgroundCancel = cancel

	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < maxTransformerProcess; i++ {
		g.Go(misc.WithBugsnag(func() error {
			wStats := webhookStatsT{}
			wStats.sentStat = stats.NewStat("webhook.transformer_sent", stats.CountType)
			wStats.receivedStat = stats.NewStat("webhook.transformer_received", stats.CountType)
			wStats.failedStat = stats.NewStat("webhook.transformer_failed", stats.CountType)
			wStats.transformTimerStat = stats.NewStat("webhook.transformation_time", stats.TimerType)
			wStats.sourceStats = make(map[string]*webhookSourceStatT)
			bt := batchWebhookTransformerT{
				webhook: webhook,
				stats:   &wStats,
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

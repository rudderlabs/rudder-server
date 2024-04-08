package transformer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/types"
)

func BenchmarkHTTP(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("apiVersion", strconv.Itoa(types.SupportedTransformerApiVersion))
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	guardConcurrency := 200000 // let's make sure this won't become a bottleneck!

	b.Run("fasthttp", func(b *testing.B) {
		tr := handle{}
		tr.stat = stats.NOP
		tr.logger = logger.NOP
		tr.conf = config.New()
		tr.config.useFasthttpClient = func() bool { return true }
		tr.fasthttpClient = &fasthttp.Client{
			MaxConnsPerHost:     1000,
			MaxIdleConnDuration: 30 * time.Second,
			MaxConnWaitTimeout:  3 * time.Second,
		}
		tr.guardConcurrency = make(chan struct{}, guardConcurrency)
		tr.receivedStat = tr.stat.NewStat("transformer_received", stats.CountType)
		tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)

		tr.config.maxRetry = config.SingleValueLoader(1)
		rawJSON, _ := json.Marshal([]TransformerEvent{
			{
				Metadata: Metadata{
					MessageID: "messageID-1",
				},
				Message: map[string]interface{}{
					"src-key-1": "messageID-1",
				},
			},
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tr.doFasthttpPost(context.Background(), rawJSON, srv.URL, "test-stage", nil)
		}
	})

	b.Run("http", func(b *testing.B) {
		tr := handle{}
		tr.stat = stats.NOP
		tr.logger = logger.NOP
		tr.conf = config.New()
		tr.config.useFasthttpClient = func() bool { return false }
		tr.client = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        1000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     30 * time.Second,
			},
			Timeout: 3 * time.Second,
		}
		tr.guardConcurrency = make(chan struct{}, guardConcurrency)
		tr.receivedStat = tr.stat.NewStat("transformer_received", stats.CountType)
		tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)

		tr.config.maxRetry = config.SingleValueLoader(1)
		rawJSON, _ := json.Marshal([]TransformerEvent{
			{
				Metadata: Metadata{
					MessageID: "messageID-1",
				},
				Message: map[string]interface{}{
					"src-key-1": "messageID-1",
				},
			},
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tr.doPost(context.Background(), rawJSON, srv.URL, "test-stage", nil)
		}
	})
}

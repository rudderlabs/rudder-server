package http_client

import (
	"fmt"
	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/resolver"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	"net"
	"net/http"
	"time"
)

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPHandle struct {
	sentStat     stats.Measurement
	receivedStat stats.Measurement
	cpDownGauge  stats.Measurement

	conf   *config.Config
	logger logger.Logger
	stat   stats.Stats

	httpClient HTTPDoer

	guardConcurrency chan struct{}

	config struct {
		maxConcurrency         int
		maxHTTPConnections     int
		maxHTTPIdleConnections int
		maxIdleConnDuration    time.Duration
		disableKeepAlives      bool

		timeoutDuration time.Duration

		maxRetry                   config.ValueLoader[int]
		failOnUserTransformTimeout config.ValueLoader[bool]
		failOnError                config.ValueLoader[bool]
		maxRetryBackoffInterval    config.ValueLoader[time.Duration]

		destTransformationURL string
		userTransformationURL string
	}
}

type HTTPLBTransport struct {
	*http.Transport
}

func (t *HTTPLBTransport) NewRoundTripper(scheme, target string, config httplb.TransportConfig) httplb.RoundTripperResult {
	return httplb.RoundTripperResult{RoundTripper: t.Transport, Close: t.CloseIdleConnections}
}

func NewHTTPClient(conf *config.Config, log logger.Logger, stat stats.Stats) (HTTPHandle, error) {
	trans := HTTPHandle{}

	trans.conf = conf
	trans.logger = log.Child("transformer")
	trans.stat = stat

	trans.sentStat = stat.NewStat("processor.transformer_sent", stats.CountType)
	trans.receivedStat = stat.NewStat("processor.transformer_received", stats.CountType)
	trans.cpDownGauge = stat.NewStat("processor.control_plane_down", stats.GaugeType)

	trans.config.maxConcurrency = conf.GetInt("Processor.maxConcurrency", 200)
	trans.config.maxHTTPConnections = conf.GetInt("Transformer.Client.maxHTTPConnections", 100)
	trans.config.maxHTTPIdleConnections = conf.GetInt("Transformer.Client.maxHTTPIdleConnections", 10)
	trans.config.maxIdleConnDuration = conf.GetDuration("Transformer.Client.maxIdleConnDuration", 30, time.Second)
	trans.config.disableKeepAlives = conf.GetBool("Transformer.Client.disableKeepAlives", true)
	trans.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	trans.config.destTransformationURL = conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	trans.config.userTransformationURL = conf.GetString("USER_TRANSFORM_URL", trans.config.destTransformationURL)

	trans.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	trans.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnUserTransformTimeout")
	trans.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")

	trans.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.Transformer.maxRetryBackoffInterval")

	trans.guardConcurrency = make(chan struct{}, trans.config.maxConcurrency)

	clientType := conf.GetString("Transformer.Client.type", "stdlib")

	transport := &http.Transport{
		DisableKeepAlives:   trans.config.disableKeepAlives,
		MaxConnsPerHost:     trans.config.maxHTTPConnections,
		MaxIdleConnsPerHost: trans.config.maxHTTPIdleConnections,
		IdleConnTimeout:     trans.config.maxIdleConnDuration,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   trans.config.timeoutDuration,
	}

	switch clientType {
	case "stdlib":
		trans.httpClient = client
	case "recycled":
		trans.httpClient = sysUtils.NewRecycledHTTPClient(func() *http.Client {
			return client
		}, config.GetDuration("Transformer.Client.ttl", 120, time.Second))
	case "httplb":
		trans.httpClient = httplb.NewClient(
			httplb.WithTransport("http", &HTTPLBTransport{
				Transport: transport,
			}),
			httplb.WithResolver(
				resolver.NewDNSResolver(
					net.DefaultResolver,
					resolver.PreferIPv6,
					config.GetDuration("Transformer.Client.ttl", 120, time.Second), // TTL value
				),
			),
		)
	default:
		return HTTPHandle{}, fmt.Errorf("unknown transformer client type: %s", clientType)
	}

	return trans, nil
}

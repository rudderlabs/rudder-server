package http_client

import (
	"net"
	"net/http"
	"time"

	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/resolver"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPLBTransport struct {
	*http.Transport
}

func (t *HTTPLBTransport) NewRoundTripper(scheme, target string, config httplb.TransportConfig) httplb.RoundTripperResult {
	return httplb.RoundTripperResult{RoundTripper: t.Transport, Close: t.CloseIdleConnections}
}

func NewHTTPClient(conf *config.Config) HTTPDoer {
	clientType := conf.GetString("Transformer.Client.type", "stdlib")

	transport := &http.Transport{
		DisableKeepAlives:   conf.GetBool("Transformer.Client.disableKeepAlives", true),
		MaxConnsPerHost:     conf.GetInt("Transformer.Client.maxHTTPConnections", 100),
		MaxIdleConnsPerHost: conf.GetInt("Transformer.Client.maxHTTPIdleConnections", 10),
		IdleConnTimeout:     conf.GetDuration("Transformer.Client.maxIdleConnDuration", 30, time.Second),
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second),
	}

	switch clientType {
	case "stdlib":
		return client
	case "recycled":
		return sysUtils.NewRecycledHTTPClient(func() *http.Client {
			return client
		}, config.GetDuration("Transformer.Client.ttl", 120, time.Second))
	case "httplb":
		return httplb.NewClient(
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
		return client
	}
}

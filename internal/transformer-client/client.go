//go:generate mockgen -destination=../../mocks/transformer-client/mock_transformer_client.go -package=mocks_transformer_client github.com/rudderlabs/rudder-server/internal/transformer-client Client

package transformerclient

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/conn"
	"github.com/bufbuild/httplb/picker"
	"github.com/bufbuild/httplb/resolver"

	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

type ClientConfig struct {
	TransportConfig struct {
		DisableKeepAlives   bool          //	true
		MaxConnsPerHost     int           //	100
		MaxIdleConnsPerHost int           //	10
		IdleConnTimeout     time.Duration //	30*time.Second
	}

	ClientTimeout time.Duration //	600*time.Second
	ClientTTL     time.Duration //	10*time.Second

	ClientType string // stdlib(default), recycled, httplb

	PickerType string // power_of_two(default), round_robin, least_loaded_random, least_loaded_round_robin, random
}

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

func NewClient(config *ClientConfig) Client {
	transport := &http.Transport{
		DisableKeepAlives:   true,
		MaxConnsPerHost:     100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   600 * time.Second,
	}
	if config == nil {
		return client
	}

	transport.DisableKeepAlives = config.TransportConfig.DisableKeepAlives
	if config.TransportConfig.MaxConnsPerHost != 0 {
		transport.MaxConnsPerHost = config.TransportConfig.MaxConnsPerHost
	}
	if config.TransportConfig.MaxIdleConnsPerHost != 0 {
		transport.MaxIdleConnsPerHost = config.TransportConfig.MaxIdleConnsPerHost
	}
	if config.TransportConfig.IdleConnTimeout != 0 {
		transport.IdleConnTimeout = config.TransportConfig.IdleConnTimeout
	}

	if config.ClientTimeout != 0 {
		client.Timeout = config.ClientTimeout
	}

	clientTTL := 10 * time.Second
	if config.ClientTTL != 0 {
		clientTTL = config.ClientTTL
	}

	switch config.ClientType {
	case "stdlib":
		return client
	case "recycled":
		return sysUtils.NewRecycledHTTPClient(func() *http.Client {
			return client
		}, clientTTL)
	case "httplb":
		tr := &httplbtransport{
			MaxConnsPerHost:     transport.MaxConnsPerHost,
			MaxIdleConnsPerHost: transport.MaxIdleConnsPerHost,
		}
		return httplb.NewClient(
			httplb.WithRootContext(context.TODO()),
			httplb.WithPicker(getPicker(config.PickerType)),
			httplb.WithIdleConnectionTimeout(transport.IdleConnTimeout),
			httplb.WithRequestTimeout(client.Timeout),
			httplb.WithResolver(resolver.NewDNSResolver(net.DefaultResolver, resolver.PreferIPv4, clientTTL)),
			httplb.WithTransport("http", tr),
			httplb.WithTransport("https", tr),
		)
	default:
		return client
	}
}

func getPicker(pickerType string) func(prev picker.Picker, allConns conn.Conns) picker.Picker {
	switch pickerType {
	case "power_of_two":
		return picker.NewPowerOfTwo
	case "round_robin":
		return picker.NewRoundRobin
	case "least_loaded_random":
		return picker.NewLeastLoadedRandom
	case "least_loaded_round_robin":
		return picker.NewLeastLoadedRoundRobin
	case "random":
		return picker.NewRandom
	default:
		return picker.NewPowerOfTwo
	}
}

type httplbtransport struct {
	MaxConnsPerHost     int
	MaxIdleConnsPerHost int
	*http.Transport
}

func (s httplbtransport) NewRoundTripper(_, _ string, opts httplb.TransportConfig) httplb.RoundTripperResult {
	transport := &http.Transport{
		Proxy:                  opts.ProxyFunc,
		GetProxyConnectHeader:  opts.ProxyConnectHeadersFunc,
		DialContext:            opts.DialFunc,
		ForceAttemptHTTP2:      true,
		MaxConnsPerHost:        s.MaxConnsPerHost,
		MaxIdleConns:           s.MaxIdleConnsPerHost,
		MaxIdleConnsPerHost:    s.MaxIdleConnsPerHost,
		IdleConnTimeout:        opts.IdleConnTimeout,
		TLSHandshakeTimeout:    opts.TLSHandshakeTimeout,
		TLSClientConfig:        opts.TLSClientConfig,
		MaxResponseHeaderBytes: opts.MaxResponseHeaderBytes,
		ExpectContinueTimeout:  1 * time.Second,
	}
	return httplb.RoundTripperResult{RoundTripper: transport, Close: transport.CloseIdleConnections}
}

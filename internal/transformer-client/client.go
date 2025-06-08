//go:generate mockgen -destination=../../mocks/transformer-client/mock_transformer_client.go -package=mocks_transformer_client github.com/rudderlabs/rudder-server/internal/transformer-client Client

package transformerclient

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/conn"
	"github.com/bufbuild/httplb/picker"
	"github.com/bufbuild/httplb/resolver"
	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/retryablehttp"
	"github.com/rudderlabs/rudder-go-kit/stats"
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
	ClientType    string        // stdlib(default), httplb
	PickerType    string        // power_of_two(default), round_robin, least_loaded_random, least_loaded_round_robin, random
	Recycle       bool          // false
	RecycleTTL    time.Duration // 60s
}

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

func NewClient(conf *config.Config, config *ClientConfig) Client {
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
		return newRetryableHTTPClient(conf, client)
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
	recycleTTL := 60 * time.Second
	if config.RecycleTTL != 0 {
		recycleTTL = config.RecycleTTL
	}

	switch config.ClientType {
	case "httplb":
		tr := &httplbtransport{
			MaxConnsPerHost:     transport.MaxConnsPerHost,
			MaxIdleConnsPerHost: transport.MaxIdleConnsPerHost,
		}
		options := []httplb.ClientOption{
			httplb.WithPicker(getPicker(config.PickerType)),
			httplb.WithIdleConnectionTimeout(transport.IdleConnTimeout),
			httplb.WithRequestTimeout(client.Timeout),
			httplb.WithResolver(resolver.NewDNSResolver(net.DefaultResolver, resolver.PreferIPv4, clientTTL)),
			httplb.WithTransport("http", tr),
			httplb.WithTransport("https", tr),
		}
		if config.Recycle {
			options = append(options, httplb.WithRoundTripperMaxLifetime(recycleTTL))
		}
		return newRetryableHTTPClient(conf, httplb.NewClient(options...))
	default:
		return newRetryableHTTPClient(conf, client)
	}
}

func newRetryableHTTPClient(conf *config.Config, baseClient Client) Client {
	cfg := &retryablehttp.Config{
		MaxRetry:        conf.GetInt("Transformer.Client.maxRetry", 0),
		InitialInterval: conf.GetDuration("Transformer.Client.Retryable.initialInterval", 1, time.Second),
		MaxInterval:     conf.GetDuration("Transformer.Client.maxInterval", 30, time.Second),
		MaxElapsedTime:  conf.GetDuration("Transformer.Client.maxElapsedTime", 0, time.Second),
		Multiplier:      conf.GetFloat64("Transformer.Client.multiplier", 2.0),
	}
	return retryablehttp.NewRetryableHTTPClient(
		cfg,
		retryablehttp.WithHttpClient(baseClient),
		retryablehttp.WithCustomRetryStrategy(func(resp *http.Response, err error) (bool, error) {
			if err != nil {
				return false, backoff.Permanent(err)
			}
			if resp.StatusCode == http.StatusServiceUnavailable &&
				strings.ToLower(resp.Header.Get("X-Rudder-Should-Retry")) == "true" {
				reason := resp.Header.Get("X-Rudder-Error-Reason")
				stats.Default.NewTaggedStat("transformer_client_perpetual_retry_count", stats.CountType, stats.Tags{"reason": reason}).Count(1)
				resp.Body.Close()
				return true, fmt.Errorf("Got retryable error response from transformer: %s", reason)
			}
			return false, nil
		}),
	)
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
		DisableCompression:     opts.DisableCompression,
	}
	return httplb.RoundTripperResult{RoundTripper: transport, Close: transport.CloseIdleConnections}
}

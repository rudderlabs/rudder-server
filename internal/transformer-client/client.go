package transformerclient

import (
	"context"
	"github.com/bufbuild/httplb/resolver"
	"net"
	"net/http"
	"time"

	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/conn"
	"github.com/bufbuild/httplb/health"
	"github.com/bufbuild/httplb/picker"

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
	ClientTTL     time.Duration //	120*time.Second

	ClientType string // stdlib(default), recycled, httplb

	PickerType string // power_of_two(default), round_robin, least_loaded_random, least_loaded_round_robin, random

	CheckerType string // nop, polling
	CheckURL    string
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

	if !config.TransportConfig.DisableKeepAlives {
		transport.DisableKeepAlives = false
	}
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

	clientTTL := 120 * time.Second
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
		checkerType := config.CheckerType
		if config.CheckURL == "" {
			checkerType = "nop"
		}
		return httplb.NewClient(
			httplb.WithRootContext(context.TODO()),
			httplb.WithPicker(getPicker(config.PickerType)),
			httplb.WithHealthChecks(getChecker(checkerType, config.CheckURL)),
			httplb.WithIdleConnectionTimeout(transport.IdleConnTimeout),
			httplb.WithRequestTimeout(client.Timeout),
			httplb.WithRoundTripperMaxLifetime(transport.IdleConnTimeout),
			httplb.WithIdleTransportTimeout(2*transport.IdleConnTimeout),
			httplb.WithResolver(resolver.NewDNSResolver(net.DefaultResolver, resolver.PreferIPv4, clientTTL)),
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

type HTTPLBTransport struct {
	*http.Transport
}

func (t *HTTPLBTransport) NewRoundTripper(scheme, target string, config httplb.TransportConfig) httplb.RoundTripperResult {
	return httplb.RoundTripperResult{RoundTripper: t.Transport, Close: t.CloseIdleConnections}
}

func getChecker(checkerType, url string) health.Checker {
	switch checkerType {
	case "nop":
		return health.NopChecker
	case "polling":
		return health.NewPollingChecker(
			health.PollingCheckerConfig{},
			health.NewSimpleProber(url),
		)
	default:
		return health.NopChecker
	}
}

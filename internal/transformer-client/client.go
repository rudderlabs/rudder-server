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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
)

type ClientConfig struct {
	TransportConfig struct {
		DisableKeepAlives   config.ValueLoader[bool]          //	true
		MaxConnsPerHost     config.ValueLoader[int]           //	100
		MaxIdleConnsPerHost config.ValueLoader[int]           //	10
		IdleConnTimeout     config.ValueLoader[time.Duration] //	30*time.Second
	}

	ClientTimeout config.ValueLoader[time.Duration] //	600*time.Second
	ClientTTL     config.ValueLoader[time.Duration] //	10*time.Second

	ClientType config.ValueLoader[string] // stdlib(default), recycled, httplb

	PickerType config.ValueLoader[string] // power_of_two(default), round_robin, least_loaded_random, least_loaded_round_robin, random
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

	transport.DisableKeepAlives = config.TransportConfig.DisableKeepAlives.Load()
	if config.TransportConfig.MaxConnsPerHost.Load() != 0 {
		transport.MaxConnsPerHost = config.TransportConfig.MaxConnsPerHost.Load()
	}
	if config.TransportConfig.MaxIdleConnsPerHost.Load() != 0 {
		transport.MaxIdleConnsPerHost = config.TransportConfig.MaxIdleConnsPerHost.Load()
	}
	if config.TransportConfig.IdleConnTimeout.Load() != 0 {
		transport.IdleConnTimeout = config.TransportConfig.IdleConnTimeout.Load()
	}

	if config.ClientTimeout.Load() != 0 {
		client.Timeout = config.ClientTimeout.Load()
	}

	clientTTL := 10 * time.Second
	if config.ClientTTL.Load() != 0 {
		clientTTL = config.ClientTTL.Load()
	}

	switch config.ClientType.Load() {
	case "stdlib":
		return client
	case "recycled":
		return sysUtils.NewRecycledHTTPClient(func() *http.Client {
			return client
		}, clientTTL)
	case "httplb":
		return httplb.NewClient(
			httplb.WithRootContext(context.TODO()),
			httplb.WithPicker(getPicker(config.PickerType.Load())),
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

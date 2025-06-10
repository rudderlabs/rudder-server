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

const (
	defaultDisableKeepAlives   = true
	defaultMaxConnsPerHost     = 100
	defaultMaxIdleConnsPerHost = 10
	defaultIdleConnTimeout     = 30 * time.Second
	defaultClientTimeout       = 600 * time.Second
	defaultClientTTL           = 10 * time.Second
	defaultRecycleTTL          = 60 * time.Second
	defaultMaxRetry            = -1
	defaultInitialInterval     = 1 * time.Second
	defaultMaxInterval         = 30 * time.Second
	defaultMaxElapsedTime      = 0
	defaultMultiplier          = 2.0

	// Special values
	noRetries = 0 // Explicit value for no retries allowed
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

	// Configuration for retryable HTTP client in case of [X-Rudder-Should-Retry: true] HTTP 503 responses
	RetryRudderErrors struct {
		Enabled         bool          // true
		MaxRetry        int           // -1 - no limit
		InitialInterval time.Duration // 1s
		MaxInterval     time.Duration // 30s
		MaxElapsedTime  time.Duration // 0s - no limit
		Multiplier      float64       // 2.0
	}
}

type Client interface {
	Do(req *http.Request) (*http.Response, error)
}

func NewClient(config *ClientConfig) Client {
	if config == nil {
		return newRetryableHTTPClient(buildDefaultClient(), nil)
	}

	switch config.ClientType {
	case "httplb":
		return buildHTTPLBClient(config)
	default:
		return buildStandardClient(config)
	}
}

// buildDefaultClient creates a client with default settings
func buildDefaultClient() *http.Client {
	transport := buildDefaultTransport()
	return &http.Client{
		Transport: transport,
		Timeout:   defaultClientTimeout,
	}
}

// buildDefaultTransport creates a transport with default settings
func buildDefaultTransport() *http.Transport {
	return &http.Transport{
		DisableKeepAlives:   defaultDisableKeepAlives,
		MaxConnsPerHost:     defaultMaxConnsPerHost,
		MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
		IdleConnTimeout:     defaultIdleConnTimeout,
	}
}

// buildStandardClient creates a standard HTTP client with configuration applied
func buildStandardClient(config *ClientConfig) Client {
	transport := buildConfiguredTransport(config)
	client := &http.Client{
		Transport: transport,
		Timeout:   getClientTimeout(config),
	}

	retryableConfig := buildRetryableConfig(config)
	if retryableConfig != nil {
		return newRetryableHTTPClient(client, retryableConfig)
	}
	return client
}

// buildHTTPLBClient creates an HTTP load balancer client
func buildHTTPLBClient(config *ClientConfig) Client {
	transport := buildConfiguredTransport(config)

	tr := &httplbtransport{
		MaxConnsPerHost:     transport.MaxConnsPerHost,
		MaxIdleConnsPerHost: transport.MaxIdleConnsPerHost,
	}

	options := []httplb.ClientOption{
		httplb.WithPicker(getPicker(config.PickerType)),
		httplb.WithIdleConnectionTimeout(transport.IdleConnTimeout),
		httplb.WithRequestTimeout(getClientTimeout(config)),
		httplb.WithResolver(resolver.NewDNSResolver(net.DefaultResolver, resolver.PreferIPv4, getClientTTL(config))),
		httplb.WithTransport("http", tr),
		httplb.WithTransport("https", tr),
	}

	if config.Recycle {
		options = append(options, httplb.WithRoundTripperMaxLifetime(getRecycleTTL(config)))
	}

	client := httplb.NewClient(options...)
	retryableConfig := buildRetryableConfig(config)

	if retryableConfig != nil {
		return newRetryableHTTPClient(client, retryableConfig)
	}
	return client
}

// buildConfiguredTransport creates a transport with configuration applied
func buildConfiguredTransport(config *ClientConfig) *http.Transport {
	transport := buildDefaultTransport()

	if config != nil {
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
	}

	return transport
}

// buildRetryableConfig creates retryable configuration if enabled
func buildRetryableConfig(clientConfig *ClientConfig) *retryablehttp.Config {
	if clientConfig == nil || !clientConfig.RetryRudderErrors.Enabled {
		return nil
	}

	// Use ClientConfig values directly
	retryConfig := &retryablehttp.Config{
		MaxRetry:        clientConfig.RetryRudderErrors.MaxRetry,
		InitialInterval: clientConfig.RetryRudderErrors.InitialInterval,
		MaxInterval:     clientConfig.RetryRudderErrors.MaxInterval,
		MaxElapsedTime:  clientConfig.RetryRudderErrors.MaxElapsedTime,
		Multiplier:      clientConfig.RetryRudderErrors.Multiplier,
	}

	// Check if this looks like an explicit configuration (has other values set)
	hasExplicitConfig := clientConfig.RetryRudderErrors.InitialInterval != 0 ||
		clientConfig.RetryRudderErrors.MaxInterval != 0 ||
		clientConfig.RetryRudderErrors.Multiplier != 0

	// Apply defaults only for truly unset values, but respect explicit MaxRetry=0
	if retryConfig.MaxRetry == 0 && !hasExplicitConfig {
		retryConfig.MaxRetry = defaultMaxRetry
	}
	if retryConfig.InitialInterval == 0 {
		retryConfig.InitialInterval = defaultInitialInterval
	}
	if retryConfig.MaxInterval == 0 {
		retryConfig.MaxInterval = defaultMaxInterval
	}
	if retryConfig.MaxElapsedTime == 0 {
		retryConfig.MaxElapsedTime = defaultMaxElapsedTime
	}
	if retryConfig.Multiplier == 0 {
		retryConfig.Multiplier = defaultMultiplier
	}

	return retryConfig
}

// Helper functions to get configuration values with defaults
func getClientTimeout(config *ClientConfig) time.Duration {
	if config != nil && config.ClientTimeout != 0 {
		return config.ClientTimeout
	}
	return defaultClientTimeout
}

func getClientTTL(config *ClientConfig) time.Duration {
	if config != nil && config.ClientTTL != 0 {
		return config.ClientTTL
	}
	return defaultClientTTL
}

func getRecycleTTL(config *ClientConfig) time.Duration {
	if config != nil && config.RecycleTTL != 0 {
		return config.RecycleTTL
	}
	return defaultRecycleTTL
}

// Generic helper function for getting values with defaults
func getValueOrDefault[T comparable](value, defaultValue T) T {
	var zero T
	if value != zero {
		return value
	}
	return defaultValue
}

func newRetryableHTTPClient(baseClient Client, retryableConfig *retryablehttp.Config) Client {
	if retryableConfig == nil {
		retryableConfig = &retryablehttp.Config{
			MaxRetry:        config.GetIntVar(defaultMaxRetry, defaultMaxRetry, "Transformer.Client.Retryable.maxRetry"),
			InitialInterval: config.GetDurationVar(1, time.Second, "Transformer.Client.Retryable.initialInterval"),
			MaxInterval:     config.GetDurationVar(30, time.Second, "Transformer.Client.Retryable.maxInterval"),
			MaxElapsedTime:  config.GetDurationVar(0, time.Second, "Transformer.Client.Retryable.maxElapsedTime"),
			Multiplier:      config.GetFloat64Var(defaultMultiplier, "Transformer.Client.Retryable.multiplier"),
		}
	}

	return retryablehttp.NewRetryableHTTPClient(
		retryableConfig,
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
				return true, fmt.Errorf("got retryable error response from transformer: %s", reason)
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

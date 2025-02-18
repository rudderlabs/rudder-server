package utils

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
)

const (
	StatusCPDown              = 809
	TransformerRequestFailure = 909
	TransformerRequestTimeout = 919
)

func IsJobTerminated(status int) bool {
	if status == http.StatusTooManyRequests || status == http.StatusRequestTimeout {
		return false
	}
	return status >= http.StatusOK && status < http.StatusInternalServerError
}

func TransformerClientConfig(conf *config.Config, configPrefix string) *transformerclient.ClientConfig {
	transformerClientConfig := &transformerclient.ClientConfig{
		ClientTimeout: conf.GetReloadableDurationVar(600, time.Second, fmt.Sprintf("HttpClient.procTransformer.%s.timeout", configPrefix), "HttpClient.procTransformer.timeout"),
		ClientTTL:     conf.GetReloadableDurationVar(10, time.Second, fmt.Sprintf("Transformer.Client.%s.ttl", configPrefix), "Transformer.Client.ttl"),
		ClientType:    conf.GetReloadableStringVar("stdlib", fmt.Sprintf("Transformer.Client.%s.type", configPrefix), "Transformer.Client.type"),
		PickerType:    conf.GetReloadableStringVar("power_of_two", fmt.Sprintf("Transformer.Client.%s.httplb.pickerType", configPrefix), "Transformer.Client.httplb.pickerType"),
	}
	transformerClientConfig.TransportConfig.DisableKeepAlives = conf.GetReloadableBoolVar(true, fmt.Sprintf("Transformer.Client.%s.disableKeepAlives", configPrefix), "Transformer.Client.disableKeepAlives")
	transformerClientConfig.TransportConfig.MaxConnsPerHost = conf.GetReloadableIntVar(100, 1, fmt.Sprintf("Transformer.Client.%s.maxHTTPConnections", configPrefix), "Transformer.Client.maxHTTPConnections")
	transformerClientConfig.TransportConfig.MaxIdleConnsPerHost = conf.GetReloadableIntVar(10, 1, fmt.Sprintf("Transformer.Client.%s.maxHTTPIdleConnections", configPrefix), "Transformer.Client.maxHTTPIdleConnections")
	transformerClientConfig.TransportConfig.IdleConnTimeout = conf.GetReloadableDurationVar(30, time.Second, fmt.Sprintf("Transformer.Client.%s.maxIdleConnDuration", configPrefix), "Transformer.Client.maxIdleConnDuration")
	return transformerClientConfig
}

func TrackLongRunningTransformation(ctx context.Context, stage string, timeout time.Duration, log logger.Logger) {
	start := time.Now()
	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Errorw("Long running transformation detected",
				"stage", stage,
				"duration", time.Since(start).String())
		}
	}
}

// GetEndpointFromURL is a helper function to extract hostname from URL
func GetEndpointFromURL(urlStr string) string {
	// Parse URL and extract hostname
	if parsedURL, err := url.Parse(urlStr); err == nil {
		return parsedURL.Host
	}
	return ""
}

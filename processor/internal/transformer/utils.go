package utils

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	"github.com/rudderlabs/rudder-server/processor/types"
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
		ClientTimeout: conf.GetDurationVar(600, time.Second, fmt.Sprintf("HttpClient.procTransformer.%s.timeout", configPrefix), "HttpClient.procTransformer.timeout"),
		ClientTTL:     conf.GetDurationVar(10, time.Second, fmt.Sprintf("Transformer.Client.%s.ttl", configPrefix), "Transformer.Client.ttl"),
		ClientType:    conf.GetStringVar("stdlib", fmt.Sprintf("Transformer.Client.%s.type", configPrefix), "Transformer.Client.type"),
		PickerType:    conf.GetStringVar("power_of_two", fmt.Sprintf("Transformer.Client.%s.httplb.pickerType", configPrefix), "Transformer.Client.httplb.pickerType"),
	}
	transformerClientConfig.TransportConfig.DisableKeepAlives = conf.GetBoolVar(true, fmt.Sprintf("Transformer.Client.%s.disableKeepAlives", configPrefix), "Transformer.Client.disableKeepAlives")
	transformerClientConfig.TransportConfig.MaxConnsPerHost = conf.GetIntVar(100, 1, fmt.Sprintf("Transformer.Client.%s.maxHTTPConnections", configPrefix), "Transformer.Client.maxHTTPConnections")
	transformerClientConfig.TransportConfig.MaxIdleConnsPerHost = conf.GetIntVar(1, 1, fmt.Sprintf("Transformer.Client.%s.maxHTTPIdleConnections", configPrefix), "Transformer.Client.maxHTTPIdleConnections")
	transformerClientConfig.TransportConfig.IdleConnTimeout = conf.GetDurationVar(5, time.Second, fmt.Sprintf("Transformer.Client.%s.maxIdleConnDuration", configPrefix), "Transformer.Client.maxIdleConnDuration")
	transformerClientConfig.Recycle = conf.GetBoolVar(false, fmt.Sprintf("Transformer.Client.%s.recycle", configPrefix), "Transformer.Client.recycle")
	transformerClientConfig.RecycleTTL = conf.GetDurationVar(60, time.Second, fmt.Sprintf("Transformer.Client.%s.recycleTTL", configPrefix), "Transformer.Client.recycleTTL")
	transformerClientConfig.RetryRudderErrors.Enabled = conf.GetBoolVar(true, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.enabled", configPrefix), "Transformer.Client.retryRudderErrors.enabled")
	transformerClientConfig.RetryRudderErrors.MaxRetry = conf.GetIntVar(-1, 1, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.maxRetry", configPrefix), "Transformer.Client.retryRudderErrors.maxRetry")
	transformerClientConfig.RetryRudderErrors.InitialInterval = conf.GetDurationVar(1, time.Second, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.initialInterval", configPrefix), "Transformer.Client.retryRudderErrors.initialInterval")
	transformerClientConfig.RetryRudderErrors.MaxInterval = conf.GetDurationVar(30, time.Second, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.maxInterval", configPrefix), "Transformer.Client.retryRudderErrors.maxInterval")
	transformerClientConfig.RetryRudderErrors.MaxElapsedTime = conf.GetDurationVar(0, time.Second, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.maxElapsedTime", configPrefix), "Transformer.Client.retryRudderErrors.maxElapsedTime")
	transformerClientConfig.RetryRudderErrors.Multiplier = conf.GetFloat64Var(2.0, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.multiplier", configPrefix), "Transformer.Client.retryRudderErrors.multiplier")
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
			log.Errorn("Long running transformation detected",
				logger.NewStringField("stage", stage),
				logger.NewDurationField("duration", time.Since(start)),
			)
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

// WithProcTransformReqTimeStat is a wrapper function to measure time taken by a request function as stats, capturing error rates as well, through a [success] label
func WithProcTransformReqTimeStat(request func() error, stat stats.Stats, labels types.TransformerMetricLabels) func() error {
	return func() error {
		start := time.Now()
		err := request()
		tags := labels.ToStatsTag()
		if err == nil {
			tags["success"] = "true"
		} else {
			tags["success"] = "false"
		}
		stat.NewTaggedStat("processor_transformer_request_time", stats.TimerType, tags).SendTiming(time.Since(start))
		return err
	}
}

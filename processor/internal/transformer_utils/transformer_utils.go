package transformer_utils

import (
	"context"
	"net/http"
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

func TransformerClientConfig(conf *config.Config) *transformerclient.ClientConfig {
	transformerClientConfig := &transformerclient.ClientConfig{
		ClientTimeout: conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second),
		ClientTTL:     conf.GetDuration("Transformer.Client.ttl", 10, time.Second),
		ClientType:    conf.GetString("Transformer.Client.type", "stdlib"),
		PickerType:    conf.GetString("Transformer.Client.httplb.pickerType", "power_of_two"),
	}
	transformerClientConfig.TransportConfig.DisableKeepAlives = conf.GetBool("Transformer.Client.disableKeepAlives", true)
	transformerClientConfig.TransportConfig.MaxConnsPerHost = conf.GetInt("Transformer.Client.maxHTTPConnections", 100)
	transformerClientConfig.TransportConfig.MaxIdleConnsPerHost = conf.GetInt("Transformer.Client.maxHTTPIdleConnections", 10)
	transformerClientConfig.TransportConfig.IdleConnTimeout = conf.GetDuration("Transformer.Client.maxIdleConnDuration", 30, time.Second)
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

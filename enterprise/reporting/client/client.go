package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jsonrs"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const (
	StatRequestTotalBytes     = "reporting_client_http_request_total_bytes"
	StatTotalDurationsSeconds = "reporting_client_http_total_duration_seconds"
	StatRequestLatency        = "reporting_client_http_request_latency"
	StatHttpRequest           = "reporting_client_http_request"
)

const (
	PathMetrics      Path = "/metrics?version=v1"
	PathRecordErrors Path = "/recordErrors"
	PathTrackedUsers Path = "/trackedUser"
)

type Path string

// Client handles sending metrics to the reporting service
type Client struct {
	path                Path
	reportingServiceURL string

	httpClient *http.Client
	backoff    backoff.BackOff
	stats      stats.Stats
	log        logger.Logger

	moduleName string
	instanceID string
}

func backOffFromConfig(conf *config.Config) backoff.BackOff {
	var opts []backoff.ExponentialBackOffOpts
	var b backoff.BackOff

	// exponential backoff related config can be added here

	b = backoff.NewExponentialBackOff(opts...)

	if conf.IsSet("Reporting.httpClient.backoff.maxRetries") {
		b = backoff.WithMaxRetries(b, uint64(conf.GetInt64("Reporting.httpClient.backoff.maxRetries", 0)))
	}

	return b
}

// New creates a new reporting client
func New(path Path, conf *config.Config, log logger.Logger, stats stats.Stats) *Client {
	reportingServiceURL := conf.GetString("REPORTING_URL", "https://reporting.dev.rudderlabs.com")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")

	return &Client{
		httpClient: &http.Client{
			Timeout: conf.GetDurationVar(60, time.Second, "Reporting.httpClient.timeout", "HttpClient.reporting.timeout"),
		},
		reportingServiceURL: reportingServiceURL,
		backoff:             backOffFromConfig(conf),
		path:                path,
		instanceID:          conf.GetString("INSTANCE_ID", "1"),
		moduleName:          conf.GetString("clientName", ""),
		stats:               stats,
		log:                 log,
	}
}

func (c *Client) Send(ctx context.Context, payload any) error {
	payloadBytes, err := jsonrs.Marshal(payload)
	if err != nil {
		return err
	}

	u, err := url.JoinPath(c.reportingServiceURL, string(c.path))
	if err != nil {
		return err
	}

	o := func() error {
		req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		httpRequestStart := time.Now()
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}

		tags := c.getTags()
		duration := time.Since(httpRequestStart)

		c.stats.NewTaggedStat(StatRequestLatency, stats.TimerType, tags).Since(httpRequestStart)

		httpStatTags := lo.Assign(tags, map[string]string{"status": strconv.Itoa(resp.StatusCode)})
		c.stats.NewTaggedStat(StatHttpRequest, stats.CountType, httpStatTags).Count(1)

		// Record total bytes sent
		c.stats.NewTaggedStat(StatRequestTotalBytes, stats.CountType, tags).Count(len(payloadBytes))

		// Record request duration
		c.stats.NewTaggedStat(StatTotalDurationsSeconds, stats.CountType, tags).Count(int(duration.Seconds()))

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading response body from reporting service: %q: %w", c.path, err)
		}

		if !c.isHTTPRequestSuccessful(resp.StatusCode) {
			err = fmt.Errorf("received unexpected response from reporting service: %q: statusCode: %d body: %v", c.path, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(c.backoff, ctx)
	err = backoff.RetryNotify(o, b, func(err error, t time.Duration) {
		c.log.Warnn(`Error reporting to service, retrying`, obskit.Error(err))
	})
	if err != nil {
		c.log.Errorn(`Error making request to reporting service`, obskit.Error(err))
	}
	return err
}

// getTags returns the common tags for reporting metrics
func (c *Client) getTags() stats.Tags {
	serverURL, _ := url.Parse(c.reportingServiceURL)
	return stats.Tags{
		"module":     c.moduleName,
		"instanceId": c.instanceID,
		"endpoint":   serverURL.Host,
		"path":       string(c.path),
	}
}

func (f *Client) isHTTPRequestSuccessful(status int) bool {
	return status >= 200 && status < 300
}

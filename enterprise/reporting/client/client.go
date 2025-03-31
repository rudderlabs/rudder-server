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
	StatTotalDurationsSeconds = "reporting_client_http_total_durations_seconds"
	StatRequestLatency        = "reporting_client_http_request_latency"
	StatHttpRequest           = "reporting_client_http_request"
)

const (
	PathMetrics      Path = "/metrics?version=v1"
	PathRecordErrors Path = "/recordErrors"
	PathAggregates   Path = "/aggregates"
)

type Path string

// Client handles sending metrics to the reporting service
type Client struct {
	httpClient          *http.Client
	reportingServiceURL string
	region              string
	label               string
	stats               stats.Stats
	log                 logger.Logger
	instanceID          string
	path                Path
}

// NewClient creates a new reporting client
func NewClient(reportingServiceURL string, path Path, conf *config.Config, log logger.Logger, stats stats.Stats) *Client {
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")
	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr, Timeout: conf.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}

	return &Client{
		httpClient:          netClient,
		reportingServiceURL: reportingServiceURL,
		path:                path,
		region:              conf.GetString("region", ""),
		instanceID:          conf.GetString("INSTANCE_ID", "1"),
		label:               conf.GetString("clientName", ""),
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
			return fmt.Errorf("error response body from reporting %w", err)
		}

		if !c.isHTTPRequestSuccessful(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
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
		"module":     c.label,
		"instanceId": c.instanceID,
		"endpoint":   serverURL.Host,
		"path":       string(c.path),
	}
}

func (f *Client) isHTTPRequestSuccessful(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

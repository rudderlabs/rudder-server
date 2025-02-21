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
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/jsonrs"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	StatRequestTotalBytes     = "reporting_client_http_request_total_bytes"
	StatRequestMetricsTotal   = "reporting_client_http_request_total_metrics"
	StatTotalDurationsSeconds = "reporting_client_http_total_durations_seconds"
	StatRequestLatency        = "reporting_client_http_request_latency"
	StatHttpRequest           = "reporting_client_http_request"

	StatReportingHttpReqLatency        = "reporting_client_http_request_latency"
	StatReportingHttpReq               = "reporting_client_http_request"
	StatReportingRequestTotalBytes     = "reporting_client_http_request_total_bytes"
	StatReportingRequestMetricsTotal   = "reporting_client_http_request_total_metrics"
	StatReportingTotalDurationsSeconds = "reporting_client_http_total_durations_seconds"

	// Legacy metrics:
	StatErrorDetailReportingHttpReqLatency = "error_detail_reporting_http_request_latency"
	StatErrorDetailReportingHttpReq        = "error_detail_reporting_http_request"
	LegacyStatFlusherHTTPRequestDuration   = "reporting_flusher_http_request_duration_seconds"
	LegacyStatFlusherHTTPRequestTotal      = "reporting_flusher_http_requests_total"
	LegacyStatFlusherSentBytes             = "reporting_flusher_sent_bytes"
)

// Client handles sending metrics to the reporting service
type Client struct {
	httpClient          *http.Client
	reportingServiceURL string
	region              string
	label               string
	stats               stats.Stats
	log                 logger.Logger
	instanceID          string
	path                string
}

// NewClient creates a new reporting client
func NewClient(reportingServiceURL, path string, conf *config.Config, log logger.Logger, stats stats.Stats) *Client {
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

// SendMetric sends a regular metric to the reporting service
func (c *Client) SendMetric(ctx context.Context, metric *types.Metric) error {
	payload, err := jsonrs.Marshal(metric)
	if err != nil {
		return fmt.Errorf("marshal failure: %w", err)
	}
	tags := c.getTags(metric.WorkspaceID, "metrics")

	operation := func() error {
		uri := fmt.Sprintf("%s/metrics?version=v1", c.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		if c.region != "" {
			q := req.URL.Query()
			q.Add("region", c.region)
			req.URL.RawQuery = q.Encode()
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")

		httpRequestStart := time.Now()
		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.log.Error(err.Error())
			return err
		}

		duration := time.Since(httpRequestStart)
		c.stats.NewTaggedStat(StatRequestLatency, stats.TimerType, tags).Since(httpRequestStart)

		httpStatTags := lo.Assign(tags, map[string]string{"status": strconv.Itoa(resp.StatusCode)})
		c.stats.NewTaggedStat(StatHttpRequest, stats.CountType, httpStatTags).Count(1)

		// Record total bytes sent
		c.stats.NewTaggedStat(StatRequestTotalBytes, stats.CountType, tags).Count(len(payload))

		// Record total metrics sent
		c.stats.NewTaggedStat(StatRequestMetricsTotal, stats.CountType, tags).Count(len(metric.StatusDetails))

		// Record request duration
		c.stats.NewTaggedStat(StatTotalDurationsSeconds, stats.CountType, tags).Count(int(duration.Seconds()))

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			c.log.Error(err.Error())
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		c.log.Errorf(`[ Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		c.log.Errorf(`[ Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

// SendErrorMetric sends an error detail metric to the reporting service
func (c *Client) SendErrorMetric(ctx context.Context, metric *types.EDMetric) error {
	payload, err := jsonrs.Marshal(metric)
	if err != nil {
		return fmt.Errorf("marshal failure: %w", err)
	}

	tags := c.getTags(metric.WorkspaceID, "recordErrors")

	operation := func() error {
		uri := fmt.Sprintf("%s/recordErrors", c.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		if c.region != "" {
			q := req.URL.Query()
			q.Add("region", c.region)
			req.URL.RawQuery = q.Encode()
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")

		httpRequestStart := time.Now()
		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.log.Errorf("Sending request failed: %v", err)
			return err
		}

		duration := time.Since(httpRequestStart)
		httpStatTags := lo.Assign(map[string]string{}, tags, map[string]string{"status": strconv.Itoa(resp.StatusCode)})
		// StatErrorDetailReportingHttpReq is legacy metric
		c.stats.NewTaggedStat(StatErrorDetailReportingHttpReq, stats.CountType, httpStatTags).Count(1)

		c.stats.NewTaggedStat(StatHttpRequest, stats.CountType, httpStatTags).Count(1)

		// Record total bytes sent
		c.stats.NewTaggedStat(StatRequestTotalBytes, stats.CountType, tags).Count(len(payload))

		// Record total metrics sent
		c.stats.NewTaggedStat(StatRequestMetricsTotal, stats.CountType, tags).Count(len(metric.Errors))

		// Record request duration
		c.stats.NewTaggedStat(StatTotalDurationsSeconds, stats.CountType, tags).Count(int(duration.Seconds()))

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		c.log.Debugf("[ErrorDetailReporting]Response from ReportingAPI: %v\n", string(respBody))
		if err != nil {
			c.log.Errorf("Reading response failed: %w", err)
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode: %d error: %v`, resp.StatusCode, string(respBody))
			c.log.Error(err.Error())
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		c.log.Errorf(`[ Error Detail Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		c.log.Errorf(`[ Error Detail Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

func (c *Client) SendAggregates(ctx context.Context, agg []aggregator.Aggregate) error {
	payloadBytes, err := jsonrs.Marshal(agg)
	if err != nil {
		return err
	}

	u := c.reportingServiceURL + url.PathEscape(c.path)

	o := func() error {
		req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		start := time.Now()
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}

		// TODO: get workspaceID from the aggregate
		tags := c.getTags("")

		c.stats.NewTaggedStat(LegacyStatFlusherHTTPRequestDuration, stats.TimerType, tags).Since(start)
		c.stats.NewTaggedStat(LegacyStatFlusherHTTPRequestTotal, stats.CountType, tags).Count(1)
		c.stats.NewTaggedStat(LegacyStatFlusherSentBytes, stats.HistogramType, tags).Observe(float64(len(payloadBytes)))

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
func (c *Client) getTags(workspaceID string) stats.Tags {
	serverURL, _ := url.Parse(c.reportingServiceURL)
	return stats.Tags{
		"workspaceId": workspaceID,
		"module":      c.label,
		"instanceId":  c.instanceID,
		"endpoint":    serverURL.Host,
		"path":        c.path,

		//legacy tags
		"clientName": c.label,
	}
}

func (f *Client) isHTTPRequestSuccessful(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

func isMetricPosted(status int) bool {
	return status >= 200 && status < 300
}

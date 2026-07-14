package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const (
	StatRequestTotalBytes      = "reporting_client_http_request_total_bytes"
	StatRequestCompressedBytes = "reporting_client_http_request_compressed_bytes"
	StatTotalDurationsSeconds  = "reporting_client_http_total_duration_seconds"
	StatRequestLatency         = "reporting_client_http_request_latency"
	StatHttpRequest            = "reporting_client_http_request"
)

const (
	RouteMetrics           Route = "/metrics?version=v1"
	RouteRecordErrors      Route = "/recordErrors"
	RouteTrackedUsers      Route = "/trackedUser"
	RouteActivationRecords Route = "/activationRecords"
)

// Route contains the HTTP path and query string for the service.
type Route string

// URL returns the absolute URL for the route, given a base URL.
// * baseURL provides only the scheme, host, and port.
// * Route provides path and query parameters.
func (p Route) URL(baseURL string) (url.URL, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return url.URL{}, fmt.Errorf("parsing base URL: %w", err)
	}

	pathURL, err := url.Parse(string(p))
	if err != nil {
		return url.URL{}, fmt.Errorf("parsing service endpoint: %w", err)
	}

	u.Path = pathURL.Path
	u.RawQuery = pathURL.RawQuery

	return *u, nil
}

// Client handles sending metrics to the reporting service
type Client struct {
	route               Route
	reportingServiceURL string
	userName            string
	password            string

	httpClient *http.Client
	stats      stats.Stats
	log        logger.Logger

	moduleName string
	instanceID string

	compress         config.ValueLoader[bool]
	compressionLevel config.ValueLoader[int]

	conf *config.Config
}

func backoffOptsFromConfig(conf *config.Config) (opts []backoff.RetryOption) {
	opts = append(opts, backoff.WithBackOff(backoff.NewExponentialBackOff()))
	if conf.IsSet("Reporting.httpClient.backoff.maxRetries") {
		opts = append(opts, backoff.WithMaxTries(uint(conf.GetIntVar(0, 1, "Reporting.httpClient.backoff.maxRetries")+1)))
	}
	return opts
}

// New creates a new reporting client
func New(path Route, conf *config.Config, log logger.Logger, stats stats.Stats) *Client {
	reportingServiceURL := conf.GetStringVar("https://reporting.dev.rudderlabs.com", "REPORTING_URL")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")

	return &Client{
		httpClient: &http.Client{
			Timeout:   conf.GetDurationVar(60, time.Second, "Reporting.httpClient.timeout", "HttpClient.reporting.timeout"),
			Transport: &http.Transport{},
		},
		reportingServiceURL: reportingServiceURL,
		userName:            conf.GetStringVar("", "REPORTING_USERNAME"),
		password:            conf.GetStringVar("", "REPORTING_PASSWORD"),
		route:               path,
		instanceID:          conf.GetStringVar("1", "INSTANCE_ID"),
		moduleName:          conf.GetStringVar("", "clientName"),
		compress:            conf.GetReloadableBoolVar(false, "Reporting.httpClient.compression.enabled"),
		compressionLevel:    conf.GetReloadableIntVar(gzip.DefaultCompression, 1, "Reporting.httpClient.compression.level"),
		stats:               stats,
		log:                 log,
		conf:                conf,
	}
}

func (c *Client) Send(ctx context.Context, payload any) error {
	payloadBytes, err := jsonrs.Marshal(payload)
	if err != nil {
		return err
	}
	uncompressedBytes := len(payloadBytes)

	// bodyBytes is what we actually put on the wire: either the raw payload or
	// its gzip-compressed form. It is materialized once here (not streamed)
	// because the request body is replayed on every backoff retry attempt.
	bodyBytes := payloadBytes
	compressed := c.compress.Load()
	if compressed {
		bodyBytes, err = c.gzipCompress(payloadBytes)
		if err != nil {
			return fmt.Errorf("compressing payload: %w", err)
		}
	}

	u, err := c.route.URL(c.reportingServiceURL)
	if err != nil {
		return fmt.Errorf("constructing URL for service endpoint (%q, %q): %w", c.route, c.reportingServiceURL, err)
	}

	o := func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("constructing HTTP request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		if compressed {
			req.Header.Set("Content-Encoding", "gzip")
		}
		req.SetBasicAuth(c.userName, c.password)
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

		// Record total (uncompressed) bytes and the bytes actually sent on the wire
		c.stats.NewTaggedStat(StatRequestTotalBytes, stats.CountType, tags).Count(uncompressedBytes)
		c.stats.NewTaggedStat(StatRequestCompressedBytes, stats.CountType, tags).Count(len(bodyBytes))

		// Record request duration
		c.stats.NewTaggedStat(StatTotalDurationsSeconds, stats.CountType, tags).Count(int(duration.Seconds()))

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading response body: %q: %w", c.route, err)
		}

		if !c.isHTTPRequestSuccessful(resp.StatusCode) {
			err = fmt.Errorf("received unexpected response: %q: statusCode: %d body: %v", c.route, resp.StatusCode, string(respBody))
		}
		return err
	}

	opts := backoffOptsFromConfig(c.conf)
	opts = append(opts, backoff.WithNotify(func(err error, t time.Duration) {
		c.log.Warnn(`Error reporting to service, retrying`, obskit.Error(err))
	}))
	err = backoffvoid.Retry(ctx, o, opts...)
	if err != nil {
		c.log.Errorn(`Error making request to reporting service`, obskit.Error(err))
	}
	return err
}

// gzipCompress gzip-compresses the given bytes using the configured compression
// level. The data is streamed through the gzip.Writer into a buffer so the result
// can be replayed across retries and its size reported.
func (c *Client) gzipCompress(b []byte) ([]byte, error) {
	level := c.compressionLevel.Load()
	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		c.log.Warnn("invalid gzip compression level, using default", logger.NewIntField("level", int64(level)))
		gw, _ = gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	}
	if _, err := gw.Write(b); err != nil {
		_ = gw.Close()
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// getTags returns the common tags for reporting metrics
func (c *Client) getTags() stats.Tags {
	serverURL, _ := url.Parse(c.reportingServiceURL)
	return stats.Tags{
		"module":     c.moduleName,
		"instanceId": c.instanceID,
		"endpoint":   serverURL.Host,
		"path":       string(c.route),
	}
}

func (f *Client) isHTTPRequestSuccessful(status int) bool {
	return status >= 200 && status < 300
}

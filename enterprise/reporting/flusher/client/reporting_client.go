package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const (
	StatReportingHttpReqLatency = "reporting_client_http_request_latency"
	StatReportingHttpReqCount   = "reporting_client_http_request_count"
)

type ReportingClient struct {
	netClient  *http.Client
	url        string
	log        logger.Logger
	tags       stats.Tags
	stats      stats.Stats
	reqLatency stats.Measurement
	reqCount   stats.Counter
}

func NewReportingClient(url string, log logger.Logger, stats stats.Stats, tags stats.Tags) *ReportingClient {
	tr := &http.Transport{}
	nc := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}

	c := ReportingClient{
		netClient: nc,
		url:       url,
		log:       log,
		stats:     stats,
		tags:      tags,
	}
	c.initStats()
	return &c
}

func (c *ReportingClient) initStats() {
	c.reqLatency = c.stats.NewTaggedStat(StatReportingHttpReqLatency, stats.TimerType, c.tags)
	c.reqCount = c.stats.NewTaggedStat(StatReportingHttpReqCount, stats.CountType, c.tags)
}

func (c *ReportingClient) MakePOSTRequest(ctx context.Context, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	o := func() error {
		req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		start := time.Now()
		resp, err := c.netClient.Do(req)
		if err != nil {
			return err
		}
		c.reqLatency.Since(start)
		c.reqCount.Count(1)

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if !c.isHTTPRequestSuccessful(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(o, b, func(err error, t time.Duration) {
		c.log.Errorf(`[ Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		c.log.Errorf(`[ Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

func (c *ReportingClient) isHTTPRequestSuccessful(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

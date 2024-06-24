package flusher

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

type Client struct {
	netClient  *http.Client
	url        string
	log        logger.Logger
	tags       stats.Tags
	stats      stats.Stats
	reqLatency stats.Measurement
	reqCount   stats.Counter
}

func NewClient(url string, log logger.Logger, stats stats.Stats, tags stats.Tags) *Client {
	tr := &http.Transport{}
	nc := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}

	c := Client{
		netClient: nc,
		url:       url,
		log:       log,
		stats:     stats,
		tags:      tags,
	}
	c.initStats()
	return &c
}

func (c *Client) initStats() {
	c.reqLatency = c.stats.NewTaggedStat(StatReportingHttpReqLatency, stats.TimerType, c.tags)
	c.reqCount = c.stats.NewTaggedStat(StatReportingHttpReqCount, stats.CountType, c.tags)
}

func (c *Client) MakePOSTRequestBatch(ctx context.Context, reports []*interface{}) error {
	payload, err := json.Marshal(reports)
	if err != nil {
		panic(err)
	}
	return c.makePOSTRequest(ctx, payload)
}

func (c *Client) MakePOSTRequest(ctx context.Context, report *interface{}) error {
	payload, err := json.Marshal(report)
	if err != nil {
		panic(err)
	}
	return c.makePOSTRequest(ctx, payload)
}

func (c *Client) makePOSTRequest(ctx context.Context, payload []byte) error {
	o := func() error {
		req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(payload))
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
	err := backoff.RetryNotify(o, b, func(err error, t time.Duration) {
		c.log.Errorf(`[ Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		c.log.Errorf(`[ Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

func (c *Client) isHTTPRequestSuccessful(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

package flusher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

var (
	flusherInstances = make(map[string]*Flusher)
	flusherMu        sync.Mutex
)

type Flusher struct {
	log logger.Logger

	db                 db.DB
	maxOpenConnections int

	client *http.Client

	instanceId string
	table      string
	module     string

	sleepInterval config.ValueLoader[time.Duration]
	flushWindow   config.ValueLoader[time.Duration]

	recentExclusionWindow               config.ValueLoader[time.Duration]
	batchSizeFromDB                     config.ValueLoader[int]
	aggressiveFlushEnabled              config.ValueLoader[bool]
	lagThresholdForAggresiveFlushInMins config.ValueLoader[time.Duration]

	reportingURL          string
	minConcurrentRequests config.ValueLoader[int]
	maxConcurrentRequests config.ValueLoader[int]
	batchSizeToReporting  config.ValueLoader[int]

	stats                   stats.Stats
	minReportedAtQueryTimer stats.Measurement
	reportsQueryTimer       stats.Measurement
	reportsCounter          stats.Measurement
	aggReportsTimer         stats.Measurement
	aggReportsCounter       stats.Measurement
	sendReportsTimer        stats.Measurement
	deleteReportsTimer      stats.Measurement
	concurrentRequests      stats.Measurement
	reqLatency              stats.Measurement
	reqCount                stats.Measurement

	aggregator aggregator.Aggregator
	commonTags stats.Tags
}

func NewFlusher(ctx context.Context, db db.DB, log logger.Logger, stats stats.Stats, conf *config.Config, table string, reportingURL string, aggregator aggregator.Aggregator, module string) *Flusher {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	if instance, exists := flusherInstances[table]; exists {
		return instance
	}

	f := createFlusher(ctx, db, log, stats, conf, table, reportingURL, aggregator, module)

	flusherInstances[table] = f

	return f
}

func createFlusher(ctx context.Context, db db.DB, log logger.Logger, stats stats.Stats, conf *config.Config, table string, reportingURL string, aggregator aggregator.Aggregator, module string) *Flusher {
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	sleepInterval := conf.GetReloadableDurationVar(5, time.Second, "Reporting.flusher.sleepInterval")
	flushWindow := conf.GetReloadableDurationVar(60, time.Second, "Reporting.flusher.flushWindow")
	minConcReqs := conf.GetReloadableIntVar(32, 1, "Reporting.flusher.minConcurrentRequests")
	maxConcReqs := conf.GetReloadableIntVar(32, 1, "Reporting.flusher.maxConcurrentRequests")
	recentExclusionWindow := conf.GetReloadableDurationVar(1, time.Minute, "Reporting.flusher.recentExclusionWindowInSeconds")
	batchSizeFromDB := conf.GetReloadableIntVar(1000, 1, "Reporting.flusher.batchSizeFromDB")
	batchSizeToReporting := conf.GetReloadableIntVar(10, 1, "Reporting.flusher.batchSizeToReporting")
	aggressiveFlushEnabled := conf.GetReloadableBoolVar(false, "Reporting.flusher.aggressiveFlushEnabled")
	lagThresholdForAggresiveFlushInMins := conf.GetReloadableDurationVar(5, time.Minute, "Reporting.flusher.lagThresholdForAggresiveFlushInMins")

	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}

	f := Flusher{
		db:                                  db,
		log:                                 log,
		reportingURL:                        reportingURL,
		instanceId:                          conf.GetString("INSTANCE_ID", "1"),
		sleepInterval:                       sleepInterval,
		flushWindow:                         flushWindow,
		minConcurrentRequests:               minConcReqs,
		maxConcurrentRequests:               maxConcReqs,
		stats:                               stats,
		recentExclusionWindow:               recentExclusionWindow,
		batchSizeFromDB:                     batchSizeFromDB,
		table:                               table,
		aggregator:                          aggregator,
		batchSizeToReporting:                batchSizeToReporting,
		maxOpenConnections:                  maxOpenConns,
		aggressiveFlushEnabled:              aggressiveFlushEnabled,
		lagThresholdForAggresiveFlushInMins: lagThresholdForAggresiveFlushInMins,
		client:                              client,
		module:                              module,
	}

	f.initCommonTags()
	f.initStats(f.commonTags)
	return &f
}

func (f *Flusher) CleanUp() error {
	err := f.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (f *Flusher) initCommonTags() {
	f.commonTags = stats.Tags{
		"instance": f.instanceId,
		"table":    f.table,
		"module":   f.module,
	}
}

func (f *Flusher) initStats(tags map[string]string) {
	f.minReportedAtQueryTimer = f.stats.NewTaggedStat("reporting_flusher_get_min_reported_at_query_duration_seconds", stats.TimerType, tags)

	f.reportsQueryTimer = f.stats.NewTaggedStat("reporting_flusher_get_reports_batch_query_duration_seconds", stats.TimerType, tags)
	f.aggReportsTimer = f.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_duration_seconds", stats.TimerType, tags)
	f.reportsCounter = f.stats.NewTaggedStat("reporting_flusher_get_reports_count", stats.HistogramType, tags)
	f.aggReportsCounter = f.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_count", stats.HistogramType, tags)

	f.sendReportsTimer = f.stats.NewTaggedStat("reporting_flusher_send_reports_duration_seconds", stats.TimerType, tags)
	f.deleteReportsTimer = f.stats.NewTaggedStat("reporting_flusher_delete_reports_duration_seconds", stats.TimerType, tags)

	f.concurrentRequests = f.stats.NewTaggedStat("reporting_flusher_concurrent_requests_in_progress", stats.GaugeType, tags)
	f.reqLatency = f.stats.NewTaggedStat("reporting_flusher_http_request_duration_seconds", stats.TimerType, tags)
	f.reqCount = f.stats.NewTaggedStat("reporting_flusher_http_requests_total", stats.CountType, tags)
}

func (f *Flusher) GetLag(ctx context.Context) time.Duration {
	start, err := f.db.GetStart(ctx, f.table)
	if err != nil {
		f.log.Errorn("Error getting start time", obskit.Error(err))
	}

	currentUTC := time.Now().UTC()
	if start.IsZero() {
		return 0
	} else {
		return currentUTC.Sub(start)
	}

}

func (f *Flusher) FlushAggressively(ctx context.Context) bool {
	if !f.aggressiveFlushEnabled.Load() {
		return false
	}
	lag := f.GetLag(ctx)
	reportingLagInMins := lag.Minutes()
	return reportingLagInMins > f.lagThresholdForAggresiveFlushInMins.Load().Minutes()
}

// flush is the main logic for flushing data.
func (f *Flusher) Flush(ctx context.Context) error {
	currentUTC := time.Now().UTC()

	// 1. Get the time range to flush
	s := time.Now()
	start, end, valid, err := f.getRange(ctx, currentUTC)
	if err != nil {
		return err
	}

	if !valid {
		return nil
	}
	f.minReportedAtQueryTimer.Since(s)

	// 2. Aggregate reports. We have different aggregators for in-app and in-db aggregation
	s = time.Now()
	f.aggReportsTimer.Since(s)
	jsonReports, err := f.aggregate(ctx, start, end)
	if err != nil {
		return err
	}
	f.aggReportsCounter.Observe(float64(len(jsonReports)))

	// 3. Flush aggregated reports
	s = time.Now()
	err = f.send(ctx, jsonReports)
	if err != nil {
		return err
	}
	f.sendReportsTimer.Since(s)

	// 4. Delete reports
	s = time.Now()
	if err := f.delete(ctx, start, end); err != nil {
		return err
	}
	f.deleteReportsTimer.Since(s)

	return nil
}

// Since we have hourly/daily/monthly aggregates on Reporting Service, we want the window to be within same hour
// Don't consider most recent data where there are inserts happening
// Always try to consider full window of flush interval or till current hour
func (f *Flusher) getRange(ctx context.Context, currentUTC time.Time) (start, end time.Time, valid bool, err error) {
	start, err = f.db.GetStart(ctx, f.table)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}

	if start.IsZero() {
		return start, end, false, nil
	}

	end = start.Add(f.flushWindow.Load())
	nextHour := start.Truncate(time.Hour).Add(time.Hour)
	endLimit := currentUTC.Add(-f.recentExclusionWindow.Load())

	if end.After(nextHour) {
		end = nextHour
	}
	if end.After(endLimit) {
		end = endLimit
	}

	currentHourStart := currentUTC.Truncate(time.Hour)
	if end.Sub(start) == f.flushWindow.Load() || end == currentHourStart {
		return start, end, true, nil
	}

	return start, end, false, nil
}

func (f *Flusher) aggregate(ctx context.Context, start, end time.Time) ([]json.RawMessage, error) {
	jsonReports, total, unique, err := f.aggregator.Aggregate(ctx, start, end)
	if err != nil {
		return nil, err
	}
	f.reportsCounter.Observe(float64(total))
	f.aggReportsCounter.Observe(float64(unique))

	return jsonReports, nil
}

func (f *Flusher) send(ctx context.Context, aggReports []json.RawMessage) error {
	if f.batchSizeToReporting.Load() > 1 {
		if err := f.sendInBatches(ctx, aggReports); err != nil {
			return err
		}
	} else {
		if err := f.sendIndividually(ctx, aggReports); err != nil {
			return err
		}
	}
	return nil
}

func (f *Flusher) getConcurrency(ctx context.Context) int {
	if f.FlushAggressively(ctx) {
		return f.maxConcurrentRequests.Load()
	}
	return f.minConcurrentRequests.Load()
}

func (f *Flusher) sendInBatches(ctx context.Context, aggReports []json.RawMessage) error {
	batchSize := f.batchSizeToReporting.Load()
	concurrency := f.getConcurrency(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i := 0; i < len(aggReports); i += batchSize {
		end := i + batchSize
		if end > len(aggReports) {
			end = len(aggReports)
		}
		batch := aggReports[i:end]

		g.Go(func() error {
			if err := f.makePOSTRequest(ctx, f.reportingURL, batch); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (f *Flusher) sendIndividually(ctx context.Context, aggReports []json.RawMessage) error {
	concurrency := f.getConcurrency(ctx)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, r := range aggReports {
		r := r // avoid closure capture issue
		g.Go(func() error {
			return f.makePOSTRequest(ctx, f.reportingURL, r)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (f *Flusher) delete(ctx context.Context, start, end time.Time) error {
	if err := f.db.Delete(ctx, f.table, start, end); err != nil {
		return err
	}
	return nil
}

func (f *Flusher) makePOSTRequest(ctx context.Context, url string, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	o := func() error {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		start := time.Now()
		resp, err := f.client.Do(req)
		if err != nil {
			return err
		}
		f.reqLatency.Since(start)
		f.reqCount.Count(1)

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if !f.isHTTPRequestSuccessful(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(o, b, func(err error, t time.Duration) {
		f.log.Warnn(`Error reporting to service, retrying`, obskit.Error(err))
	})
	if err != nil {
		f.log.Errorn(`Error making request to reporting service`, obskit.Error(err))
	}
	return err
}

func (f *Flusher) isHTTPRequestSuccessful(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

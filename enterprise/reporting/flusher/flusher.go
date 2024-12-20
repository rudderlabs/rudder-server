package flusher

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/lib/pq"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type Flusher struct {
	log logger.Logger

	db                 *sql.DB
	maxOpenConnections int

	client     *http.Client
	aggregator aggregator.Aggregator

	instanceId string
	table      string
	module     string

	sleepInterval                       config.ValueLoader[time.Duration]
	flushWindow                         config.ValueLoader[time.Duration]
	recentExclusionWindow               config.ValueLoader[time.Duration]
	batchSizeFromDB                     config.ValueLoader[int]
	aggressiveFlushEnabled              config.ValueLoader[bool]
	lagThresholdForAggresiveFlushInMins config.ValueLoader[time.Duration]
	vacuumThresholdDeletedRows          config.ValueLoader[int]
	vacuumThresholdBytes                config.ValueLoader[int64]
	deletedRows                         int
	lastVacuum                          time.Time
	vacuumFull                          config.ValueLoader[bool]
	vacuumInterval                      config.ValueLoader[time.Duration]

	reportingURL          string
	minConcurrentRequests config.ValueLoader[int]
	maxConcurrentRequests config.ValueLoader[int]
	batchSizeToReporting  config.ValueLoader[int]

	stats                   stats.Stats
	minReportedAtQueryTimer stats.Measurement
	aggReportsTimer         stats.Measurement
	sendReportsTimer        stats.Measurement
	deleteReportsTimer      stats.Measurement
	vacuumReportsTimer      stats.Measurement
	concurrentRequests      stats.Measurement
	reqLatency              stats.Measurement
	reqCount                stats.Measurement
	sentBytes               stats.Measurement
	flushLag                stats.Measurement

	commonTags stats.Tags
}

func NewFlusher(db *sql.DB, log logger.Logger, stats stats.Stats, conf *config.Config, table, reportingURL string, aggregator aggregator.Aggregator, module string) (*Flusher, error) {
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	sleepInterval := conf.GetReloadableDurationVar(5, time.Second, "Reporting.flusher.sleepInterval")
	flushWindow := conf.GetReloadableDurationVar(60, time.Second, "Reporting.flusher.flushWindow")
	recentExclusionWindow := conf.GetReloadableDurationVar(1, time.Minute, "Reporting.flusher.recentExclusionWindowInSeconds")
	minConcReqs := conf.GetReloadableIntVar(32, 1, "Reporting.flusher.minConcurrentRequests")
	maxConcReqs := conf.GetReloadableIntVar(32, 1, "Reporting.flusher.maxConcurrentRequests")
	batchSizeFromDB := conf.GetReloadableIntVar(1000, 1, "Reporting.flusher.batchSizeFromDB")
	batchSizeToReporting := conf.GetReloadableIntVar(10, 1, "Reporting.flusher.batchSizeToReporting")
	aggressiveFlushEnabled := conf.GetReloadableBoolVar(false, "Reporting.flusher.aggressiveFlushEnabled")
	lagThresholdForAggresiveFlushInMins := conf.GetReloadableDurationVar(5, time.Minute, "Reporting.flusher.lagThresholdForAggresiveFlushInMins")
	vacuumThresholdDeletedRows := conf.GetReloadableIntVar(100000, 1, "Reporting.flusher.vacuumThresholdDeletedRows")
	vacuumInterval := conf.GetReloadableDurationVar(15, time.Minute, "Reporting.flusher.vacuumInterval", "Reporting.vacuumInterval")
	vacuumThresholdBytes := conf.GetReloadableInt64Var(10*bytesize.GB, 1, "Reporting.flusher.vacuumThresholdBytes", "Reporting.vacuumThresholdBytes")

	tr := &http.Transport{}
	client := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}

	f := Flusher{
		db:                         db,
		log:                        log,
		reportingURL:               reportingURL,
		instanceId:                 conf.GetString("INSTANCE_ID", "1"),
		sleepInterval:              sleepInterval,
		flushWindow:                flushWindow,
		recentExclusionWindow:      recentExclusionWindow,
		minConcurrentRequests:      minConcReqs,
		maxConcurrentRequests:      maxConcReqs,
		stats:                      stats,
		batchSizeFromDB:            batchSizeFromDB,
		vacuumThresholdDeletedRows: vacuumThresholdDeletedRows,
		vacuumFull:                 conf.GetReloadableBoolVar(false, "Reporting.flusher.vacuumFull", "Reporting.vacuumFull"),
		vacuumInterval:             vacuumInterval,
		vacuumThresholdBytes:       vacuumThresholdBytes,

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
	return &f, nil
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

	f.aggReportsTimer = f.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_duration_seconds", stats.TimerType, tags)

	f.sendReportsTimer = f.stats.NewTaggedStat("reporting_flusher_send_reports_duration_seconds", stats.TimerType, tags)
	f.deleteReportsTimer = f.stats.NewTaggedStat("reporting_flusher_delete_reports_duration_seconds", stats.TimerType, tags)
	f.vacuumReportsTimer = f.stats.NewTaggedStat("reporting_flusher_vacuum_reports_duration_seconds", stats.TimerType, tags)

	f.concurrentRequests = f.stats.NewTaggedStat("reporting_flusher_concurrent_requests_in_progress", stats.GaugeType, tags)
	f.reqLatency = f.stats.NewTaggedStat("reporting_flusher_http_request_duration_seconds", stats.TimerType, tags)
	f.reqCount = f.stats.NewTaggedStat("reporting_flusher_http_requests_total", stats.CountType, tags)
	f.sentBytes = f.stats.NewTaggedStat("reporting_flusher_sent_bytes", stats.HistogramType, tags)

	f.flushLag = f.stats.NewTaggedStat("reporting_flusher_lag_seconds", stats.GaugeType, tags)
}

func (f *Flusher) getStart(ctx context.Context) (time.Time, error) {
	var start sql.NullTime
	query := fmt.Sprintf("SELECT MIN(reported_at) FROM %s", f.table)
	err := f.db.QueryRowContext(ctx, query).Scan(&start)
	if err != nil {
		return time.Time{}, fmt.Errorf("error getting min reported_at %w", err)
	}

	return start.Time, nil
}

func (f *Flusher) getLag(ctx context.Context) (time.Duration, error) {
	start, err := f.getStart(ctx)
	if err != nil {
		return 0, fmt.Errorf("error getting start %w", err)
	}

	currentUTC := time.Now().UTC()
	if start.IsZero() {
		return 0, nil
	} else {
		return currentUTC.Sub(start), nil
	}
}

func (f *Flusher) ShouldFlushAggressively(ctx context.Context) bool {
	if !f.aggressiveFlushEnabled.Load() {
		return false
	}
	lag, err := f.getLag(ctx)
	if err != nil {
		return false
	}
	reportingLagInMins := lag.Minutes()
	return reportingLagInMins > f.lagThresholdForAggresiveFlushInMins.Load().Minutes()
}

// flush is the main logic for flushing data.
func (f *Flusher) Flush(ctx context.Context) error {
	currentUTC := time.Now().UTC()

	// Emit the lag metric
	lag, err := f.getLag(ctx)
	if err != nil {
		return err
	}
	f.flushLag.Gauge(lag.Seconds())

	// 1. Get the time range to flush
	s := time.Now()
	start, end, isFullFlushWindow, err := f.getRange(ctx, currentUTC)
	if err != nil {
		return err
	}

	if !isFullFlushWindow {
		return nil
	}
	f.minReportedAtQueryTimer.Since(s)

	// 2. Aggregate reports. We have different aggregators for in-app and in-db aggregation
	s = time.Now()
	jsonReports, err := f.aggregate(ctx, start, end)
	if err != nil {
		return err
	}
	f.aggReportsTimer.Since(s)

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

	// 5. Vacuum the table
	if err := f.vacuum(ctx); err != nil {
		return err
	}

	return nil
}

// Since we have hourly/daily/monthly aggregates on Reporting Service, we want the window to be within same hour
// Don't consider most recent data where there are inserts happening
// Always try to consider full window of flush interval or till current hour
func (f *Flusher) getRange(ctx context.Context, currentUTC time.Time) (start, end time.Time, isFullFlushWindow bool, err error) {
	start, err = f.getStart(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("error getting start in getRange %w", err)
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

	if end.Sub(start) == f.flushWindow.Load() || end == nextHour {
		return start, end, true, nil
	}

	return start, end, false, nil
}

func (f *Flusher) aggregate(ctx context.Context, start, end time.Time) ([]json.RawMessage, error) {
	jsonReports, err := f.aggregator.Aggregate(ctx, start, end)
	if err != nil {
		return nil, err
	}

	return jsonReports, nil
}

func (f *Flusher) getConcurrency(ctx context.Context) int {
	if f.ShouldFlushAggressively(ctx) {
		return f.maxConcurrentRequests.Load()
	}
	return f.minConcurrentRequests.Load()
}

func (f *Flusher) send(ctx context.Context, aggReports []json.RawMessage) error {
	batchSize := f.batchSizeToReporting.Load()
	concurrency := f.getConcurrency(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	f.concurrentRequests.Gauge(float64(concurrency))

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

func (f *Flusher) delete(ctx context.Context, minReportedAt, maxReportedAt time.Time) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE reported_at >= $1 AND reported_at < $2", f.table)
	res, err := f.db.ExecContext(ctx, query, minReportedAt, maxReportedAt)
	if err == nil {
		rows, _ := res.RowsAffected()
		f.deletedRows += int(rows)
	}
	return err
}

func (f *Flusher) vacuum(ctx context.Context) error {
	var query string
	var full bool
	var performVacuum bool
	if f.deletedRows >= f.vacuumThresholdDeletedRows.Load() {
		performVacuum = true
	} else if time.Since(f.lastVacuum) >= f.vacuumInterval.Load() {
		var sizeEstimate int64
		if err := f.db.QueryRowContext(
			ctx, `SELECT pg_table_size(oid) from pg_class where relname = $1`, f.table,
		).Scan(&sizeEstimate); err != nil {
			return fmt.Errorf("error getting table size %w", err)
		}
		if sizeEstimate >= f.vacuumThresholdBytes.Load() {
			performVacuum = true
		}
	}
	if !performVacuum {
		return nil
	}
	vacuumStart := time.Now()
	defer f.vacuumReportsTimer.Since(vacuumStart)
	if f.vacuumFull.Load() {
		full = true
		query = fmt.Sprintf("vacuum full analyze %s", pq.QuoteIdentifier(f.table))
	} else {
		query = fmt.Sprintf("vacuum analyze %s", pq.QuoteIdentifier(f.table))
	}
	if _, err := f.db.ExecContext(ctx, query); err != nil {
		f.log.Errorn(
			"error vacuuming",
			logger.NewStringField("table", f.table),
			obskit.Error(err),
			logger.NewBoolField("full", full),
		)
		return fmt.Errorf("error vacuuming table %w", err)
	}
	f.lastVacuum = time.Now()
	f.deletedRows = 0
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
		f.sentBytes.Observe(float64(len(payloadBytes)))

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error response body from reporting %w", err)
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

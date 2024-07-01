package flusher

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/client"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/handler"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

var (
	flusherInstances = make(map[string]*Flusher)
	flusherMu        sync.Mutex
)

type Flusher struct {
	ctx    context.Context
	cancel context.CancelFunc
	g      *errgroup.Group
	log    logger.Logger

	db                 db.DB
	maxOpenConnections int

	instanceId string

	table  string
	labels []string

	sleepInterval config.ValueLoader[time.Duration]
	flushWindow   config.ValueLoader[time.Duration]

	inAppAggregationEnabled             bool
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
	flushTimer              stats.Measurement
	reportsCounter          stats.Measurement
	aggReportsTimer         stats.Measurement
	aggReportsCounter       stats.Measurement
	reportingLag            stats.Measurement
	sendReportsTimer        stats.Measurement
	deleteReportsTimer      stats.Measurement
	concurrentRequests      stats.Measurement

	client     client.Client
	handler    handler.Handler
	commonTags stats.Tags

	startOnce sync.Once
	started   atomic.Bool
}

func NewFlusher(ctx context.Context, db db.DB, log logger.Logger, stats stats.Stats, conf *config.Config, table string, labels []string, reportingURL string, inAppAggregationEnabled bool, handler handler.Handler) *Flusher {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	if instance, exists := flusherInstances[table]; exists {
		return instance
	}

	f := createFlusher(ctx, db, log, stats, conf, table, labels, reportingURL, inAppAggregationEnabled, handler)

	flusherInstances[table] = f

	return f
}

func createFlusher(ctx context.Context, db db.DB, log logger.Logger, stats stats.Stats, conf *config.Config, table string, labels []string, reportingURL string, inAppAggregationEnabled bool, handler handler.Handler) *Flusher {
	maxOpenConns := conf.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	sleepInterval := conf.GetReloadableDurationVar(60, time.Second, "Reporting.flusher.sleepInterval")
	flushWindow := conf.GetReloadableDurationVar(60, time.Second, "Reporting.flusher.flushWindow")
	minConcReqs := conf.GetReloadableIntVar(32, 1, "Reporting.flusher.minConcurrentRequests")
	maxConcReqs := conf.GetReloadableIntVar(32, 1, "Reporting.flusher.maxConcurrentRequests")
	recentExclusionWindow := conf.GetReloadableDurationVar(1, time.Minute, "Reporting.flusher.recentExclusionWindowInSeconds")
	batchSizeFromDB := conf.GetReloadableIntVar(1000, 1, "Reporting.flusher.batchSizeFromDB")
	batchSizeToReporting := conf.GetReloadableIntVar(10, 1, "Reporting.flusher.batchSizeToReporting")
	aggressiveFlushEnabled := conf.GetReloadableBoolVar(false, "Reporting.flusher.aggressiveFlushEnabled")
	lagThresholdForAggresiveFlushInMins := conf.GetReloadableDurationVar(5, time.Minute, "Reporting.flusher.lagThresholdForAggresiveFlushInMins")
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	f := Flusher{
		ctx:                                 ctx,
		cancel:                              cancel,
		g:                                   g,
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
		labels:                              labels,
		batchSizeFromDB:                     batchSizeFromDB,
		table:                               table,
		handler:                             handler,
		inAppAggregationEnabled:             inAppAggregationEnabled,
		batchSizeToReporting:                batchSizeToReporting,
		maxOpenConnections:                  maxOpenConns,
		aggressiveFlushEnabled:              aggressiveFlushEnabled,
		lagThresholdForAggresiveFlushInMins: lagThresholdForAggresiveFlushInMins,
	}

	f.initCommonTags()
	f.client = client.NewReportingClient(reportingURL, log, stats, f.commonTags)
	return &f
}

func (f *Flusher) Run() {
	f.startOnce.Do(func() {
		f.initStats(f.commonTags)

		f.g.Go(func() error {
			return f.startLagCapture(f.ctx)
		})

		f.g.Go(func() error {
			return f.startFlushing(f.ctx)
		})

		f.started.Store(true)

		if err := f.g.Wait(); err != nil {
			f.log.Errorn("Error in flusher", obskit.Error(err))

			// TODO: Should we panic here ?
			if !errors.Is(err, context.Canceled) {
				panic(err)
			}
		}
	})
}

func (f *Flusher) Stop() {
	f.cancel()
	_ = f.g.Wait()
	err := f.db.Close()
	if err != nil {
		f.log.Errorn("Error closing DB", obskit.Error(err))
	}
	f.started.Store(false)
}

func (f *Flusher) initCommonTags() {
	f.commonTags = stats.Tags{
		"instanceId":       f.instanceId,
		"tableName":        f.table,
		"inAppAggregation": strconv.FormatBool(f.inAppAggregationEnabled),
	}
}

func (f *Flusher) startLagCapture(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			lag := f.getLag()
			f.reportingLag.Gauge(lag.Seconds())
		}
	}
}

func (f *Flusher) initStats(tags map[string]string) {
	f.flushTimer = f.stats.NewTaggedStat("reporting_flusher_flush_duration_seconds", stats.TimerType, tags)

	f.minReportedAtQueryTimer = f.stats.NewTaggedStat("reporting_flusher_get_min_reported_at_query_duration_seconds", stats.TimerType, tags)

	f.reportsQueryTimer = f.stats.NewTaggedStat("reporting_flusher_get_reports_batch_query_duration_seconds", stats.TimerType, tags)
	f.aggReportsTimer = f.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_duration_seconds", stats.TimerType, tags)
	f.reportsCounter = f.stats.NewTaggedStat("reporting_flusher_get_reports_count", stats.HistogramType, tags)
	f.aggReportsCounter = f.stats.NewTaggedStat("reporting_flusher_get_aggregated_reports_count", stats.HistogramType, tags)

	f.sendReportsTimer = f.stats.NewTaggedStat("reporting_flusher_send_reports_duration_seconds", stats.TimerType, tags)
	f.deleteReportsTimer = f.stats.NewTaggedStat("reporting_flusher_delete_reports_duration_seconds", stats.TimerType, tags)

	f.concurrentRequests = f.stats.NewTaggedStat("reporting_flusher_concurrent_requests_in_progress", stats.GaugeType, tags)
	f.reportingLag = f.stats.NewTaggedStat("reporting_flusher_lag_seconds", stats.GaugeType, tags)
}

func (f *Flusher) startFlushing(ctx context.Context) error {
	ticker := time.NewTicker(f.sleepInterval.Load())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			s := time.Now().UTC()
			if err := f.flush(ctx); err != nil {
				return err
			}
			f.flushTimer.Since(s)

			if !f.flushAggressively() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	}
}

func (f *Flusher) getLag() time.Duration {
	start, err := f.db.GetStart(f.ctx, f.table)
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

func (f *Flusher) flushAggressively() bool {
	if !f.aggressiveFlushEnabled.Load() {
		return false
	}
	lag := f.getLag()
	reportingLagInMins := lag.Minutes()
	return reportingLagInMins > f.lagThresholdForAggresiveFlushInMins.Load().Minutes()
}

// flush is the main logic for flushing data.
func (f *Flusher) flush(ctx context.Context) error {
	currentTime := time.Now().UTC()

	// 1. Get the time range to flush
	s := time.Now().UTC()
	start, end, valid, err := f.getRange(ctx, currentTime, f.flushWindow.Load(), f.recentExclusionWindow.Load())
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	f.minReportedAtQueryTimer.Since(s)

	// 2. Aggregate reports. Get reports in batches and aggregate in app if inAppAggregationEnabled or aggregate in DB
	s = time.Now().UTC()
	f.aggReportsTimer.Since(s)
	aggReports, err := f.aggregate(ctx, start, end, f.inAppAggregationEnabled, f.batchSizeFromDB.Load())
	if err != nil {
		return err
	}
	f.aggReportsCounter.Observe(float64(len(aggReports)))

	// 3. Flush aggregated reports
	s = time.Now().UTC()
	err = f.send(ctx, aggReports, f.batchSizeToReporting.Load(), f.getConcurrency())
	if err != nil {
		return err
	}
	f.sendReportsTimer.Since(s)

	// 4. Delete reports
	s = time.Now().UTC()
	if err := f.delete(ctx, start, end); err != nil {
		return err
	}
	f.deleteReportsTimer.Since(s)

	return nil
}

func (f *Flusher) getRange(ctx context.Context, currentTime time.Time, flushWindow, recentExclusionWindow time.Duration) (start, end time.Time, valid bool, err error) {
	start, err = f.db.GetStart(ctx, f.table)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}

	if start.IsZero() {
		return start, end, false, nil
	}

	end = f.calcEnd(start, currentTime, flushWindow, recentExclusionWindow)

	currentHourStart := currentTime.Truncate(time.Hour)
	if end.Sub(start) == flushWindow || end == currentHourStart {
		return start, end, true, nil
	}

	return start, end, false, nil
}

// Since we have hourly/daily/monthly aggregates on Reporting Service, we want the window to be within same hour
// Don't consider most recent data where there are inserts happening
func (f *Flusher) calcEnd(start, currentTime time.Time, flushWindow, recentExclusionWindow time.Duration) time.Time {
	end := start.Add(flushWindow)
	nextHour := start.Truncate(time.Hour).Add(time.Hour)
	endLimit := currentTime.Add(-recentExclusionWindow)

	if end.After(nextHour) {
		end = nextHour
	}
	if end.After(endLimit) {
		end = endLimit
	}
	return end
}

func (f *Flusher) aggregate(ctx context.Context, start, end time.Time, inAppAggregationEnabled bool, batchSize int) ([]*report.DecodedReport, error) {
	if inAppAggregationEnabled {
		aggReports, err := f.aggregateInApp(ctx, start, end, batchSize)
		if err != nil {
			return nil, err
		}
		return aggReports, nil
	} else {
		panic("aggregation in DB is not implemented")
	}
}

func (f *Flusher) aggregateInApp(ctx context.Context, start, end time.Time, batchSize int) ([]*report.DecodedReport, error) {
	var offset int
	aggReportsMap := make(map[string]report.DecodedReport)
	reportsCount := 0

	for {
		s := time.Now().UTC()
		reports, err := f.db.FetchBatch(ctx, f.table, start, end, batchSize, offset)
		if err != nil {
			return nil, err
		}
		if len(reports) == 0 {
			break
		}

		f.reportsQueryTimer.Since(s)
		reportsCount = reportsCount + len(reports)

		s = time.Now().UTC()
		err = f.aggregateBatch(reports, aggReportsMap)
		if err != nil {
			return nil, err
		}
		f.aggReportsTimer.Since(s)

		if len(reports) < batchSize {
			break
		}

		offset += batchSize
	}

	f.reportsCounter.Observe(float64(reportsCount))

	aggReports := convertToSlice(aggReportsMap)
	return aggReports, nil
}

func (f *Flusher) aggregateBatch(reports []report.RawReport, aggReportsMap map[string]report.DecodedReport) error {
	for _, r := range reports {
		dr, err := f.handler.Decode(r)
		if err != nil {
			return err
		}

		k := f.getAggKey(r)

		if agg, exists := aggReportsMap[k]; exists {
			if err := f.handler.Aggregate(agg, dr); err != nil {
				return err
			}
		} else {
			aggReportsMap[k] = dr
		}
	}
	return nil
}

func (f *Flusher) getAggKey(r map[string]interface{}) string {
	var k string
	for _, l := range f.labels {
		if v, exists := r[l]; exists {
			k += v.(string) + "_"
		}
	}
	return k
}

func convertToSlice(r map[string]report.DecodedReport) []*report.DecodedReport {
	s := make([]*report.DecodedReport, 0, len(r))
	for _, v := range r {
		s = append(s, &v)
	}
	return s
}

func (f *Flusher) send(ctx context.Context, aggReports []*report.DecodedReport, batchSize, concurrency int) error {
	if batchSize > 1 {
		if err := f.sendInBatches(ctx, aggReports, batchSize, concurrency); err != nil {
			return err
		}
	} else {
		if err := f.sendIndividually(ctx, aggReports, concurrency); err != nil {
			return err
		}
	}
	return nil
}

func (f *Flusher) getConcurrency() int {
	if f.flushAggressively() {
		return f.maxConcurrentRequests.Load()
	}
	return f.minConcurrentRequests.Load()
}

func (f *Flusher) sendInBatches(ctx context.Context, aggReports []*report.DecodedReport, batchSize, concurrency int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i := 0; i < len(aggReports); i += batchSize {
		end := i + batchSize
		if end > len(aggReports) {
			end = len(aggReports)
		}
		batch := aggReports[i:end]

		g.Go(func() error {
			if err := f.client.MakePOSTRequest(ctx, batch); err != nil {
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

func (f *Flusher) sendIndividually(ctx context.Context, aggReports []*report.DecodedReport, concurrency int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for _, r := range aggReports {
		r := r // avoid closure capture issue
		g.Go(func() error {
			return f.client.MakePOSTRequest(ctx, r)
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

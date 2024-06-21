package flusher

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

type Flusher struct {
	ctx    context.Context
	cancel context.CancelFunc
	g      *errgroup.Group
	log    logger.Logger
	db     db.Database

	instanceId string

	table  string
	labels []string
	values []string

	mainLoopSleepInterval config.ValueLoader[time.Duration]
	sleepInterval         config.ValueLoader[time.Duration]
	aggWindowMins         config.ValueLoader[time.Duration]
	batchSizeFromDB       config.ValueLoader[int]
	reportingURL          string
	maxConcurrentRequests config.ValueLoader[int]

	stats                  stats.Stats
	minReportedAtQueryTime stats.Measurement
	reportsQueryTime       stats.Measurement
	reqLatency             stats.Measurement
	mainLoopTimer          stats.Measurement
	reportsTimer           stats.Measurement
	reportsCount           stats.Measurement
	aggReportsTimer        stats.Measurement
	aggReportsCount        stats.Measurement
	reportingLag           stats.Measurement
	flushReportsTimer      stats.Measurement
	deleteReportsTimer     stats.Measurement

	lastReportedAt atomic.Time
	client         *Client
	handler        Handler
	commonTags     stats.Tags
}

func NewFlusher(ctx context.Context, db db.Database, log logger.Logger, stats stats.Stats, table string, labels []string, values []string, reportingServiceURL string, handler Handler) *Flusher {

	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := config.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcReqs := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	aggWindowMins := config.GetReloadableDurationVar(5, time.Minute, "Reporting.aggregationWindowInMinutes")
	batchSizeFromDB := config.GetReloadableIntVar(1000, 1, "Reporting.batchSizeFromDB")
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	flusher := Flusher{
		ctx:                   ctx,
		cancel:                cancel,
		g:                     g,
		log:                   log,
		reportingURL:          reportingServiceURL,
		instanceId:            config.GetString("INSTANCE_ID", "1"),
		sleepInterval:         sleepInterval,
		mainLoopSleepInterval: mainLoopSleepInterval,
		maxConcurrentRequests: maxConcReqs,
		stats:                 stats,
		aggWindowMins:         aggWindowMins,
		labels:                labels,
		values:                values,
		batchSizeFromDB:       batchSizeFromDB,
		table:                 table,
		db:                    db,
		handler:               handler,
	}

	flusher.initCommonTags()
	flusher.client = NewClient(reportingServiceURL, log, stats, flusher.commonTags)
	return &flusher
}

func (f *Flusher) Start(ctx context.Context) {
	f.initStats(f.commonTags)

	f.lastReportedAt.Store(time.Now())

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return f.emitLagMetricLoop(ctx)
	})

	g.Go(func() error {
		return f.mainLoop(ctx)
	})

	if err := g.Wait(); err != nil {
		f.log.Errorw("Error running flusher", "error", err)
	}
}

func (f *Flusher) initCommonTags() {
	f.commonTags = stats.Tags{
		"instanceId": f.instanceId,
		"tableName":  f.table,
	}
}

func (f *Flusher) emitLagMetricLoop(ctx context.Context) error {
	for {
		lag := time.Since(f.lastReportedAt.Load())
		f.reportingLag.Gauge(lag.Seconds())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Minute):
		}
	}
}

func (f *Flusher) Stop() {
	f.cancel()
	_ = f.g.Wait()
}

func (f *Flusher) initStats(tags map[string]string) {
	f.mainLoopTimer = f.stats.NewTaggedStat(StatReportingMainLoopTime, stats.TimerType, tags)

	f.minReportedAtQueryTime = f.stats.NewTaggedStat(StatReportingGetMinReportedAtQueryTime, stats.TimerType, tags)

	f.reportsTimer = f.stats.NewTaggedStat(StatReportingGetReportsTime, stats.TimerType, tags)
	f.reportsCount = f.stats.NewTaggedStat(StatReportingGetReportsCount, stats.HistogramType, tags)

	f.reportsQueryTime = f.stats.NewTaggedStat(StatReportingGetReportsQueryTime, stats.TimerType, tags)
	f.aggReportsTimer = f.stats.NewTaggedStat(StatReportingGetAggregatedReportsTime, stats.TimerType, tags)
	f.aggReportsCount = f.stats.NewTaggedStat(StatReportingGetAggregatedReportsCount, stats.HistogramType, tags)

	f.flushReportsTimer = f.stats.NewTaggedStat(StatReportingFlushReportsTime, stats.TimerType, tags)
	f.deleteReportsTimer = f.stats.NewTaggedStat(StatReportingDeleteReportsTime, stats.TimerType, tags)

	f.reqLatency = f.stats.NewTaggedStat(StatReportingHttpReqLatency, stats.TimerType, tags)
	f.reportingLag = f.stats.NewTaggedStat(StatReportingMetricsLagInSeconds, stats.GaugeType, tags)

}

func (f *Flusher) mainLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(f.mainLoopSleepInterval.Load()): // TODO: sleep less if there's lag
			start := time.Now()
			f.mainLoopOnce(ctx)
			f.mainLoopTimer.Since(start)
		}
	}
}

// mainLoopOnce is the main logic for flushing data.
func (f *Flusher) mainLoopOnce(ctx context.Context) error {
	// 1. Get the time range to flush
	start := time.Now()
	min, max := f.getRange(ctx)
	if min.IsZero() || max.IsZero() {
		return nil
	}
	f.minReportedAtQueryTime.Since(start)

	// 2. Get reports in batches and aggregate in app
	start = time.Now()
	aggReports, err := f.batchAndAggregateInApp(ctx, min, max)
	if err != nil {
		return err
	}
	f.aggReportsTimer.Since(start)
	f.aggReportsCount.Observe(float64(len(aggReports)))

	// 3. Flush aggregated reports
	start = time.Now()
	if err := f.flushAggReports(ctx, aggReports); err != nil {
		return err
	}
	f.flushReportsTimer.Since(start)

	// 4. Delete reports
	start = time.Now()
	if err := f.deleteReports(ctx, min, max); err != nil {
		return err
	}
	f.deleteReportsTimer.Since(start)

	f.lastReportedAt.Store(max)
	return nil
}

func (f *Flusher) getRange(ctx context.Context) (min, max time.Time) {
	var err error
	min, err = f.db.GetMinReportedAt(ctx, f.table)
	if err != nil {
		f.log.Errorw("Error getting min reported_at", "error", err)
		return time.Time{}, time.Time{}
	}

	max = f.calcMax(min)
	return min, max
}

// Since we have hourly/daily/monthly aggregates on Reporting Service, we want the window to be within same hour
func (f *Flusher) calcMax(min time.Time) time.Time {
	max := min.Add(f.aggWindowMins.Load())
	if max.After(time.Now().Truncate(time.Hour).Add(time.Hour)) {
		max = time.Now().Truncate(time.Hour).Add(time.Hour)
	}
	return max
}

func (f *Flusher) batchAndAggregateInApp(ctx context.Context, minReportedAt, maxReportedAt time.Time) (map[string]interface{}, error) {
	var offset int
	aggReports := make(map[string]interface{})

	for {
		reports, err := f.db.FetchReports(ctx, f.table, minReportedAt, maxReportedAt, f.batchSizeFromDB.Load(), offset)
		if err != nil {
			f.log.Errorw("Error fetching reports", "error", err)
			return nil, err
		}

		for _, r := range reports {
			dr, err := f.handler.Decode(r)
			if err != nil {
				f.log.Errorw("Error deserializing report", "error", err)
				return nil, err
			}

			key := f.getAggKey(r)

			if agg, exists := aggReports[key]; exists {
				if err := f.handler.Aggregate(agg, dr); err != nil {
					f.log.Errorw("Error adding report to aggregate", "error", err)
					return nil, err
				}
			} else {
				aggReports[key] = dr
			}
		}

		if len(reports) < f.batchSizeFromDB.Load() {
			break
		}

		offset += f.batchSizeFromDB.Load()
	}

	return aggReports, nil
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

func (f *Flusher) flushAggReports(ctx context.Context, aggReports map[string]interface{}) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, r := range aggReports {
		r := r // avoid closure capture issue
		g.Go(func() error {
			return f.client.MakePOSTRequest(ctx, &r)
		})
	}

	if err := g.Wait(); err != nil {
		f.log.Errorw("Error sending data to Reporting Service", "error", err)
		return err
	}

	return nil
}

func (f *Flusher) deleteReports(ctx context.Context, min, max time.Time) error {
	if err := f.db.DeleteReports(ctx, f.table, min, max); err != nil {
		return err
	}
	return nil
}

package flusher

import (
	"context"
	"errors"
	"strconv"
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

	mainLoopSleepInterval   config.ValueLoader[time.Duration]
	aggWindowMins           config.ValueLoader[time.Duration]
	batchSizeFromDB         config.ValueLoader[int]
	reportingURL            string
	maxConcurrentRequests   config.ValueLoader[int]
	inAppAggregationEnabled bool
	batchSizeToReporting    config.ValueLoader[int]

	stats                   stats.Stats
	minReportedAtQueryTimer stats.Measurement
	reportsQueryTimer       stats.Measurement
	reqLatencyTimer         stats.Measurement
	mainLoopTimer           stats.Measurement
	reportsTimer            stats.Measurement
	reportsCounter          stats.Measurement
	aggReportsTimer         stats.Measurement
	aggReportsCounter       stats.Measurement
	reportingLag            stats.Measurement
	sendReportsTimer        stats.Measurement
	deleteReportsTimer      stats.Measurement

	lastReportedAt atomic.Time
	client         *Client
	handler        Handler
	commonTags     stats.Tags
}

func NewFlusher(ctx context.Context, db db.Database, log logger.Logger, stats stats.Stats, table string, labels []string, values []string, reportingURL string, inAppAggregationEnabled bool, handler Handler) *Flusher {

	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	maxConcReqs := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	aggWindowMins := config.GetReloadableDurationVar(5, time.Minute, "Reporting.aggregationWindowInMinutes")
	batchSizeFromDB := config.GetReloadableIntVar(1000, 1, "Reporting.batchSizeFromDB")
	batchSizeToReporting := config.GetReloadableIntVar(10, 1, "Reporting.batchSizeToReporting")
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	f := Flusher{
		ctx:                     ctx,
		cancel:                  cancel,
		g:                       g,
		log:                     log,
		reportingURL:            reportingURL,
		instanceId:              config.GetString("INSTANCE_ID", "1"),
		mainLoopSleepInterval:   mainLoopSleepInterval,
		maxConcurrentRequests:   maxConcReqs,
		stats:                   stats,
		aggWindowMins:           aggWindowMins,
		labels:                  labels,
		values:                  values,
		batchSizeFromDB:         batchSizeFromDB,
		table:                   table,
		db:                      db,
		handler:                 handler,
		inAppAggregationEnabled: inAppAggregationEnabled,
		batchSizeToReporting:    batchSizeToReporting,
	}

	f.initCommonTags()
	f.client = NewClient(reportingURL, log, stats, f.commonTags)
	return &f
}

func (f *Flusher) Start() {
	f.initStats(f.commonTags)

	f.lastReportedAt.Store(time.Now())

	g, ctx := errgroup.WithContext(f.ctx)

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
		"instanceId":       f.instanceId,
		"tableName":        f.table,
		"inAppAggregation": strconv.FormatBool(f.inAppAggregationEnabled),
	}
}

func (f *Flusher) emitLagMetricLoop(ctx context.Context) error {
	for {
		lag := time.Since(f.lastReportedAt.Load())
		f.reportingLag.Gauge(lag.Seconds())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Minute): // TODO: Avoid time.Sleep especially for such durations of time, can cause issues with graceful shutdown and testing.

		}
	}
}

func (f *Flusher) Stop() {
	f.cancel()
	_ = f.g.Wait()
}

func (f *Flusher) initStats(tags map[string]string) {
	f.mainLoopTimer = f.stats.NewTaggedStat(StatReportingMainLoopTime, stats.TimerType, tags)

	f.minReportedAtQueryTimer = f.stats.NewTaggedStat(StatReportingGetMinReportedAtQueryTime, stats.TimerType, tags)

	f.reportsTimer = f.stats.NewTaggedStat(StatReportingGetReportsTime, stats.TimerType, tags)
	f.reportsCounter = f.stats.NewTaggedStat(StatReportingGetReportsCount, stats.HistogramType, tags)

	f.reportsQueryTimer = f.stats.NewTaggedStat(StatReportingGetReportsBatchQueryTime, stats.TimerType, tags)
	f.aggReportsTimer = f.stats.NewTaggedStat(StatReportingGetAggregatedReportsTime, stats.TimerType, tags)
	f.aggReportsCounter = f.stats.NewTaggedStat(StatReportingGetAggregatedReportsCount, stats.HistogramType, tags)

	f.sendReportsTimer = f.stats.NewTaggedStat(StatReportingSendReportsTime, stats.TimerType, tags)
	f.deleteReportsTimer = f.stats.NewTaggedStat(StatReportingDeleteReportsTime, stats.TimerType, tags)

	f.reqLatencyTimer = f.stats.NewTaggedStat(StatReportingHttpReqLatency, stats.TimerType, tags)
	f.reportingLag = f.stats.NewTaggedStat(StatReportingMetricsLagInSeconds, stats.GaugeType, tags)

}

func (f *Flusher) mainLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
	s := time.Now()
	start, end, err := f.getRange(ctx)
	if err != nil {
		return err
	}
	f.minReportedAtQueryTimer.Since(s)

	// 2. Aggregate reports. Get reports in batches and aggregate in app if inAppAggregationEnabled or aggregate in DB
	s = time.Now()
	f.aggReportsTimer.Since(s)
	aggReports, err := f.aggregate(ctx, start, end)
	if err != nil {
		return err
	}
	f.aggReportsCounter.Observe(float64(len(aggReports)))

	// 3. Flush aggregated reports
	s = time.Now()
	f.send(ctx, aggReports)
	f.sendReportsTimer.Since(s)

	// 4. Delete reports
	s = time.Now()
	if err := f.delete(ctx, start, end); err != nil {
		return err
	}
	f.deleteReportsTimer.Since(s)

	f.lastReportedAt.Store(end)
	return nil
}

func (f *Flusher) getRange(ctx context.Context) (start, end time.Time, error error) {
	start, err := f.db.GetStart(ctx, f.table)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	end = f.calcEnd(start)
	return start, end, nil
}

// Since we have hourly/daily/monthly aggregates on Reporting Service, we want the window to be within same hour
func (f *Flusher) calcEnd(start time.Time) time.Time {
	end := start.Add(f.aggWindowMins.Load())
	if end.After(time.Now().Truncate(time.Hour).Add(time.Hour)) {
		end = time.Now().Truncate(time.Hour).Add(time.Hour)
	}
	return end
}

func (f *Flusher) aggregate(ctx context.Context, start, end time.Time) ([]*interface{}, error) {
	if f.inAppAggregationEnabled {
		aggReports, err := f.aggregateInApp(ctx, start, end)
		if err != nil {
			return nil, err
		}
		return aggReports, nil
	} else {
		return nil, errors.New("aggregation in DB is not implemented")
	}
}

func (f *Flusher) aggregateInApp(ctx context.Context, start, end time.Time) ([]*interface{}, error) {
	var offset int
	aggMap := make(map[string]interface{})
	reportsCount := 0

	for {
		s := time.Now()
		reports, err := f.db.FetchBatch(ctx, f.table, start, end, f.batchSizeFromDB.Load(), offset)
		if err != nil {
			f.log.Errorw("Error fetching reports", "error", err)
			return nil, err
		}
		f.reportsQueryTimer.Since(s)
		reportsCount = reportsCount + len(reports)

		s = time.Now()
		for _, r := range reports {
			dr, err := f.handler.Decode(r)
			if err != nil {
				f.log.Errorw("Error deserializing report", "error", err)
				return nil, err
			}

			k := f.getAggKey(r)

			if agg, exists := aggMap[k]; exists {
				if err := f.handler.Aggregate(agg, dr); err != nil {
					f.log.Errorw("Error adding report to aggregate", "error", err)
					return nil, err
				}
			} else {
				aggMap[k] = dr
			}
		}

		if len(reports) > 0 {
			f.aggReportsTimer.Since(s)
		}

		if len(reports) < f.batchSizeFromDB.Load() {
			break
		}

		offset += f.batchSizeFromDB.Load()
	}

	f.reportsCounter.Observe(float64(reportsCount))

	aggReports := convertToSlice(aggMap)
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

func convertToSlice(r map[string]interface{}) []*interface{} {
	s := make([]*interface{}, 0, len(r))
	for _, v := range r {
		s = append(s, &v)
	}
	return s
}

func (f *Flusher) send(ctx context.Context, aggReports []*interface{}) error {
	if f.batchSizeToReporting.Load() > 1 {
		if err := f.sendInBatches(ctx, aggReports, f.batchSizeFromDB.Load()); err != nil {
			return err
		}
	} else {
		if err := f.sendIndividually(ctx, aggReports); err != nil {
			return err
		}
	}
	return nil
}

func (f *Flusher) sendInBatches(ctx context.Context, aggReports []*interface{}, batchSize int) error {
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < len(aggReports); i += batchSize {
		end := i + batchSize
		if end > len(aggReports) {
			end = len(aggReports)
		}
		batch := aggReports[i:end]

		g.Go(func() error {
			if err := f.client.MakePOSTRequestBatch(ctx, batch); err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		f.log.Errorw("Error sending data to Reporting Service", "error", err)
		return err
	}

	return nil
}

func (f *Flusher) sendIndividually(ctx context.Context, aggReports []*interface{}) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, r := range aggReports {
		r := r // avoid closure capture issue
		g.Go(func() error {
			return f.client.MakePOSTRequest(ctx, r)
		})
	}

	if err := g.Wait(); err != nil {
		f.log.Errorw("Error sending data to Reporting Service", "error", err)
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

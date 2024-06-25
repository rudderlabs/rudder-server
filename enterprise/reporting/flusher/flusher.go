package flusher

import (
	"context"
	"errors"
	"strconv"
	"sync"
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

	db                 db.Database
	maxOpenConnections int

	instanceId string

	table  string
	labels []string
	values []string

	mainLoopSleepInterval config.ValueLoader[time.Duration]

	inAppAggregationEnabled             bool
	aggWindowMins                       config.ValueLoader[time.Duration]
	batchSizeFromDB                     config.ValueLoader[int]
	lagThresholdForAggresiveFlushInMins config.ValueLoader[time.Duration]

	reportingURL          string
	minConcurrentRequests config.ValueLoader[int]
	maxConcurrentRequests config.ValueLoader[int]
	batchSizeToReporting  config.ValueLoader[int]

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
	concurrentRequests      stats.Measurement

	lastReportedAt atomic.Time
	client         *Client
	handler        Handler
	commonTags     stats.Tags

	startOnce sync.Once
}

func NewFlusher(ctx context.Context, db db.Database, log logger.Logger, stats stats.Stats, table string, labels []string, values []string, reportingURL string, inAppAggregationEnabled bool, handler Handler) *Flusher {

	maxOpenConns := config.GetIntVar(4, 1, "Reporting.maxOpenConnections")
	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	minConcReqs := config.GetReloadableIntVar(32, 1, "Reporting.minConcurrentRequests")
	maxConcReqs := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	aggWindowMins := config.GetReloadableDurationVar(5, time.Minute, "Reporting.aggregationWindowInMinutes")
	batchSizeFromDB := config.GetReloadableIntVar(1000, 1, "Reporting.batchSizeFromDB")
	batchSizeToReporting := config.GetReloadableIntVar(10, 1, "Reporting.batchSizeToReporting")
	lagThresholdForAggresiveFlushInMins := config.GetReloadableDurationVar(5, time.Minute, "Reporting.lagThresholdForAggresiveFlushInMins")
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	f := Flusher{
		ctx:                                 ctx,
		cancel:                              cancel,
		g:                                   g,
		db:                                  db,
		log:                                 log,
		reportingURL:                        reportingURL,
		instanceId:                          config.GetString("INSTANCE_ID", "1"),
		mainLoopSleepInterval:               mainLoopSleepInterval,
		minConcurrentRequests:               minConcReqs,
		maxConcurrentRequests:               maxConcReqs,
		stats:                               stats,
		aggWindowMins:                       aggWindowMins,
		labels:                              labels,
		values:                              values,
		batchSizeFromDB:                     batchSizeFromDB,
		table:                               table,
		handler:                             handler,
		inAppAggregationEnabled:             inAppAggregationEnabled,
		batchSizeToReporting:                batchSizeToReporting,
		maxOpenConnections:                  maxOpenConns,
		lagThresholdForAggresiveFlushInMins: lagThresholdForAggresiveFlushInMins,
	}

	f.initCommonTags()
	f.client = NewClient(reportingURL, log, stats, f.commonTags)
	return &f
}

func (f *Flusher) Start() {
	f.startOnce.Do(func() {

		err := f.db.InitDB()
		if err != nil {
			panic(err)
		}

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
			f.log.Errorw("Error in flusher", "error", err)

			// TODO: Should we panic here ?
			if !errors.Is(err, context.Canceled) {
				panic(err)
			}
		}
	})
}

func (f *Flusher) initCommonTags() {
	f.commonTags = stats.Tags{
		"instanceId":       f.instanceId,
		"tableName":        f.table,
		"inAppAggregation": strconv.FormatBool(f.inAppAggregationEnabled),
	}
}

func (f *Flusher) emitLagMetricLoop(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			lag := time.Since(f.lastReportedAt.Load())
			f.reportingLag.Gauge(lag.Seconds())
		}
	}
}

func (f *Flusher) Stop() {
	f.cancel()
	_ = f.g.Wait()
	f.db.CloseDB()
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
	f.concurrentRequests = f.stats.NewTaggedStat(StatReportingHttpReqCount, stats.GaugeType, tags)
	f.reportingLag = f.stats.NewTaggedStat(StatReportingMetricsLagInSeconds, stats.GaugeType, tags)

}

func (f *Flusher) mainLoop(ctx context.Context) error {
	ticker := time.NewTicker(f.mainLoopSleepInterval.Load())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			start := time.Now()
			if err := f.mainLoopOnce(ctx); err != nil {
				return err
			}
			f.mainLoopTimer.Since(start)
			if !f.shouldIncreaseFlushing(f.lastReportedAt.Load()) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	}
}

func (f *Flusher) shouldIncreaseFlushing(lastReportedAt time.Time) bool {
	reportingLagInMins := time.Since(lastReportedAt).Minutes()
	if reportingLagInMins > f.lagThresholdForAggresiveFlushInMins.Load().Minutes() {
		return false
	}
	return true
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
			return nil, err
		}
		f.reportsQueryTimer.Since(s)
		reportsCount = reportsCount + len(reports)

		s = time.Now()
		for _, r := range reports {
			dr, err := f.handler.Decode(r)
			if err != nil {
				return nil, err
			}

			k := f.getAggKey(r)

			if agg, exists := aggMap[k]; exists {
				if err := f.handler.Aggregate(agg, dr); err != nil {
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
	concurrency := f.getConcurrency(f.lastReportedAt.Load())
	if f.batchSizeToReporting.Load() > 1 {
		if err := f.sendInBatches(ctx, aggReports, f.batchSizeFromDB.Load(), concurrency); err != nil {
			return err
		}
	} else {
		if err := f.sendIndividually(ctx, aggReports, concurrency); err != nil {
			return err
		}
	}
	return nil
}

func (f *Flusher) getConcurrency(lastReportedAt time.Time) int {
	if f.shouldIncreaseFlushing(f.lastReportedAt.Load()) {
		return f.maxConcurrentRequests.Load()
	}
	return f.minConcurrentRequests.Load()
}

func (f *Flusher) sendInBatches(ctx context.Context, aggReports []*interface{}, batchSize int, concurrency int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

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
		return err
	}

	return nil
}

func (f *Flusher) sendIndividually(ctx context.Context, aggReports []*interface{}, concurrency int) error {
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

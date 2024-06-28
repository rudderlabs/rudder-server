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
	flushInterval config.ValueLoader[time.Duration]

	inAppAggregationEnabled             bool
	aggWindowMins                       config.ValueLoader[time.Duration]
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

	lastReportedAt atomic.Time
	client         client.Client
	handler        handler.Handler
	commonTags     stats.Tags

	startOnce sync.Once
	started   atomic.Bool
}

func NewFlusher(ctx context.Context, db db.DB, log logger.Logger, stats stats.Stats, table string, labels []string, reportingURL string, inAppAggregationEnabled bool, handler handler.Handler) *Flusher {
	flusherMu.Lock()
	defer flusherMu.Unlock()

	if instance, exists := flusherInstances[table]; exists {
		return instance
	}

	f := createFlusher(ctx, db, log, stats, table, labels, reportingURL, inAppAggregationEnabled, handler)

	flusherInstances[table] = f

	return f
}

func createFlusher(ctx context.Context, db db.DB, log logger.Logger, stats stats.Stats, table string, labels []string, reportingURL string, inAppAggregationEnabled bool, handler handler.Handler) *Flusher {
	maxOpenConns := config.GetIntVar(4, 1, "Reporting.flusher.maxOpenConnections")
	sleepInterval := config.GetReloadableDurationVar(60, time.Second, "Reporting.flusher.sleepInterval")
	flushInterval := config.GetReloadableDurationVar(60, time.Second, "Reporting.flusher.flushInterval")
	minConcReqs := config.GetReloadableIntVar(32, 1, "Reporting.flusher.minConcurrentRequests")
	maxConcReqs := config.GetReloadableIntVar(32, 1, "Reporting.flusher.maxConcurrentRequests")
	aggWindowMins := config.GetReloadableDurationVar(5, time.Minute, "Reporting.flusher.aggregationWindowInMinutes")
	recentExclusionWindow := config.GetReloadableDurationVar(1, time.Minute, "Reporting.flusher.recentExclusionWindowInSeconds")
	batchSizeFromDB := config.GetReloadableIntVar(1000, 1, "Reporting.flusher.batchSizeFromDB")
	batchSizeToReporting := config.GetReloadableIntVar(10, 1, "Reporting.flusher.batchSizeToReporting")
	aggressiveFlushEnabled := config.GetReloadableBoolVar(false, "Reporting.flusher.aggressiveFlushEnabled")
	lagThresholdForAggresiveFlushInMins := config.GetReloadableDurationVar(5, time.Minute, "Reporting.flusher.lagThresholdForAggresiveFlushInMins")
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
		sleepInterval:                       sleepInterval,
		flushInterval:                       flushInterval,
		minConcurrentRequests:               minConcReqs,
		maxConcurrentRequests:               maxConcReqs,
		stats:                               stats,
		aggWindowMins:                       aggWindowMins,
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

func (f *Flusher) Start() {
	f.startOnce.Do(func() {
		err := f.db.InitDB()
		if err != nil {
			panic(err)
		}

		f.initStats(f.commonTags)

		f.lastReportedAt.Store(time.Now().UTC())

		g, ctx := errgroup.WithContext(f.ctx)

		g.Go(func() error {
			return f.startLagCapture(ctx)
		})

		g.Go(func() error {
			return f.startFlushing(ctx)
		})

		f.started.Store(true)

		if err := g.Wait(); err != nil {
			f.log.Errorw("Error in flusher", "error", err)

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
	err := f.db.CloseDB()
	if err != nil {
		f.log.Errorw("Error closing DB", "error", err)
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
			lag := time.Since(f.lastReportedAt.Load())
			f.reportingLag.Gauge(lag.Seconds())
		}
	}
}

func (f *Flusher) initStats(tags map[string]string) {
	f.flushTimer = f.stats.NewTaggedStat(StatFlusherFlushTime, stats.TimerType, tags)

	f.minReportedAtQueryTimer = f.stats.NewTaggedStat(StatFlusherGetMinReportedAtQueryTime, stats.TimerType, tags)

	f.reportsCounter = f.stats.NewTaggedStat(StatFlusherGetReportsCount, stats.HistogramType, tags)

	f.reportsQueryTimer = f.stats.NewTaggedStat(StatFlusherGetReportsBatchQueryTime, stats.TimerType, tags)
	f.aggReportsTimer = f.stats.NewTaggedStat(StatFlusherGetAggregatedReportsTime, stats.TimerType, tags)
	f.aggReportsCounter = f.stats.NewTaggedStat(StatFluherGetAggregatedReportsCount, stats.HistogramType, tags)

	f.sendReportsTimer = f.stats.NewTaggedStat(StatFlusherSendReportsTime, stats.TimerType, tags)
	f.deleteReportsTimer = f.stats.NewTaggedStat(StatFlusherDeleteReportsTime, stats.TimerType, tags)

	f.concurrentRequests = f.stats.NewTaggedStat(StatFlusherConcurrentRequests, stats.GaugeType, tags)
	f.reportingLag = f.stats.NewTaggedStat(StatFlusherLagInSeconds, stats.GaugeType, tags)
}

func (f *Flusher) startFlushing(ctx context.Context) error {
	ticker := time.NewTicker(f.sleepInterval.Load())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			shouldFlush, err := f.shouldFlush()
			if err != nil {
				return err
			}

			if shouldFlush {
				s := time.Now().UTC()
				if err := f.flush(ctx); err != nil {
					return err
				}
				f.flushTimer.Since(s)
			}

			if !f.flushAggressively(f.lastReportedAt.Load(), f.aggressiveFlushEnabled.Load()) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	}
}

func (f *Flusher) shouldFlush() (bool, error) {
	currentTime := time.Now().UTC()
	start, err := f.db.GetStart(f.ctx, f.table)
	if err != nil {
		return false, err
	}
	if start.IsZero() || start.After(currentTime.Add(-f.flushInterval.Load())) {
		return false, nil
	}
	return true, nil
}

func (f *Flusher) flushAggressively(lastReportedAt time.Time, aggresiveFlushEnabled bool) bool {
	if !aggresiveFlushEnabled {
		return false
	}
	reportingLagInMins := time.Since(lastReportedAt).Minutes()
	return reportingLagInMins <= f.lagThresholdForAggresiveFlushInMins.Load().Minutes()
}

// flush is the main logic for flushing data.
func (f *Flusher) flush(ctx context.Context) error {
	// 1. Get the time range to flush
	s := time.Now().UTC()a	start, end, err := f.getRange(ctx, f.aggWindowMins.Load(), f.recentExclusionWindow.Load())
	if err != nil {
		return err
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
	err = f.send(ctx, aggReports, f.batchSizeToReporting.Load(), f.getConcurrency(f.lastReportedAt.Load()))
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

	f.lastReportedAt.Store(end)
	return nil
}

func (f *Flusher) getRange(ctx context.Context, aggWindowMins, recentExclusionWindow time.Duration) (start, end time.Time, error error) {
	start, err := f.db.GetStart(ctx, f.table)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	end = f.calcEnd(start, aggWindowMins, recentExclusionWindow)
	return start, end, nil
}

// Since we have hourly/daily/monthly aggregates on Reporting Service, we want the window to be within same hour
// Don't consider most recent data where there are inserts happening
func (f *Flusher) calcEnd(start time.Time, aggWindowMins, recentExclusionWindow time.Duration) time.Time {
	end := start.Add(aggWindowMins)
	currentTime := time.Now().UTC()
	nextHour := currentTime.Truncate(time.Hour).Add(time.Hour)
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

func (f *Flusher) getConcurrency(lastReportedAt time.Time) int {
	if f.flushAggressively(lastReportedAt, f.aggressiveFlushEnabled.Load()) {
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

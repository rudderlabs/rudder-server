package reporting

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/event_sampler"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

const ReportsTable = "reports"

const (
	StatReportingMainLoopTime              = "reporting_client_main_loop_time"
	StatReportingGetReportsTime            = "reporting_client_get_reports_time"
	StatReportingGetReportsCount           = "reporting_client_get_reports_count"
	StatReportingGetAggregatedReportsTime  = "reporting_client_get_aggregated_reports_time"
	StatReportingGetAggregatedReportsCount = "reporting_client_get_aggregated_reports_count"
	StatReportingGetMinReportedAtQueryTime = "reporting_client_get_min_reported_at_query_time"
	StatReportingGetReportsQueryTime       = "reporting_client_get_reports_query_time"
	StatReportingVacuumDuration            = "reporting_vacuum_duration"
)

type DefaultReporter struct {
	ctx              context.Context
	cancel           context.CancelFunc
	g                *errgroup.Group
	configSubscriber *configSubscriber
	syncersMu        sync.RWMutex
	syncers          map[string]*types.SyncSource
	log              logger.Logger
	namespace        string

	instanceID                           string
	whActionsOnly                        bool
	sleepInterval                        config.ValueLoader[time.Duration]
	mainLoopSleepInterval                config.ValueLoader[time.Duration]
	dbQueryTimeout                       *config.Reloadable[time.Duration]
	sourcesWithEventNameTrackingDisabled []string
	maxOpenConnections                   int
	maxConcurrentRequests                config.ValueLoader[int]
	vacuumFull                           config.ValueLoader[bool]

	getMinReportedAtQueryTime stats.Measurement
	getReportsQueryTime       stats.Measurement
	stats                     stats.Stats
	maxReportsCountInARequest config.ValueLoader[int]

	eventSamplingEnabled  config.ValueLoader[bool]
	eventSamplingDuration config.ValueLoader[time.Duration]
	eventSampler          event_sampler.EventSampler

	eventNamePrefixLength config.ValueLoader[int]
	eventNameSuffixLength config.ValueLoader[int]
	commonClient          *client.Client
}

func NewDefaultReporter(ctx context.Context, conf *config.Config, log logger.Logger, configSubscriber *configSubscriber, stats stats.Stats) *DefaultReporter {
	var dbQueryTimeout *config.Reloadable[time.Duration]
	var eventSampler event_sampler.EventSampler

	sourcesWithEventNameTrackingDisabled := config.GetStringSlice("Reporting.sourcesWithEventNameTrackingDisabled", []string{})

	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := config.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := config.GetIntVar(32, 1, "Reporting.maxOpenConnections")
	dbQueryTimeout = config.GetReloadableDurationVar(60, time.Second, "Reporting.dbQueryTimeout")
	maxReportsCountInARequest := conf.GetReloadableIntVar(10, 1, "Reporting.maxReportsCountInARequest")
	eventSamplingEnabled := conf.GetReloadableBoolVar(false, "Reporting.eventSampling.enabled")
	eventSamplingDuration := conf.GetReloadableDurationVar(60, time.Minute, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("badger", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(100000, 1, "Reporting.eventSampling.cardinality")
	eventNamePrefixLength := conf.GetReloadableIntVar(40, 1, "Reporting.eventNameTrimming.prefixLength")
	eventNameSuffixLength := conf.GetReloadableIntVar(10, 1, "Reporting.eventNameTrimming.suffixLength")
	// only send reports for wh actions sources if whActionsOnly is configured
	whActionsOnly := config.GetBool("REPORTING_WH_ACTIONS_ONLY", false)
	if whActionsOnly {
		log.Infon("REPORTING_WH_ACTIONS_ONLY enabled.only sending reports relevant to wh actions.")
	}

	if eventSamplingEnabled.Load() {
		var err error
		eventSampler, err = event_sampler.NewEventSampler(ctx, eventSamplingDuration, eventSamplerType, eventSamplingCardinality, event_sampler.MetricsReporting, conf, log, stats)
		if err != nil {
			panic(err)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)
	return &DefaultReporter{
		ctx:                                  ctx,
		cancel:                               cancel,
		g:                                    g,
		log:                                  log,
		configSubscriber:                     configSubscriber,
		syncers:                              make(map[string]*types.SyncSource),
		namespace:                            config.GetKubeNamespace(),
		instanceID:                           config.GetString("INSTANCE_ID", "1"),
		whActionsOnly:                        whActionsOnly,
		sleepInterval:                        sleepInterval,
		mainLoopSleepInterval:                mainLoopSleepInterval,
		vacuumFull:                           config.GetReloadableBoolVar(false, "Reporting.vacuumFull"),
		sourcesWithEventNameTrackingDisabled: sourcesWithEventNameTrackingDisabled,
		maxOpenConnections:                   maxOpenConnections,
		maxConcurrentRequests:                maxConcurrentRequests,
		dbQueryTimeout:                       dbQueryTimeout,
		maxReportsCountInARequest:            maxReportsCountInARequest,
		stats:                                stats,
		eventSamplingEnabled:                 eventSamplingEnabled,
		eventSamplingDuration:                eventSamplingDuration,
		eventSampler:                         eventSampler,
		eventNamePrefixLength:                eventNamePrefixLength,
		eventNameSuffixLength:                eventNameSuffixLength,
		commonClient:                         client.New(client.RouteMetrics, conf, log, stats),
	}
}

func (r *DefaultReporter) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	if c.Label == "" {
		c.Label = types.CoreReportingLabel
	}

	r.syncersMu.Lock()
	defer r.syncersMu.Unlock()
	if _, ok := r.syncers[c.ConnInfo]; ok {
		return func() {} // returning a no-op syncer since another go routine has already started syncing
	}

	dbHandle, err := sql.Open("postgres", c.ConnInfo)
	if err != nil {
		panic(err)
	}
	dbHandle.SetMaxOpenConns(r.maxOpenConnections)

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "reports_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate("reports")
	if err != nil {
		panic(fmt.Errorf("could not run reports migrations: %w", err))
	}

	m = &migrator.Migrator{
		Handle:          dbHandle,
		MigrationsTable: "reports_runalways_migrations",
		RunAlways:       true,
	}
	templateData := map[string]interface{}{
		"config": config.Default,
	}
	if err := m.MigrateFromTemplates("reports_always", templateData); err != nil {
		panic(fmt.Errorf("could not run reports_always migrations: %w", err))
	}
	r.syncers[c.ConnInfo] = &types.SyncSource{SyncerConfig: c, DbHandle: dbHandle}

	if !config.GetBool("Reporting.syncer.enabled", true) {
		return func() {}
	}
	if config.GetBool("Reporting.syncer.vacuumAtStartup", false) {
		if _, err := dbHandle.ExecContext(context.Background(), `vacuum full analyze reports;`); err != nil {
			r.log.Errorn(`[ Reporting ]: Error full vacuuming reports table`, obskit.Error(err))
			panic(err)
		}
	}
	return func() {
		r.g.Go(func() error {
			r.mainLoop(r.ctx, c)
			return nil
		})
	}
}

func (r *DefaultReporter) GetSyncer(syncerKey string) *types.SyncSource {
	r.syncersMu.RLock()
	defer r.syncersMu.RUnlock()
	return r.syncers[syncerKey]
}

func (r *DefaultReporter) getDBHandle(syncerKey string) (*sql.DB, error) {
	syncer := r.GetSyncer(syncerKey)
	if syncer != nil {
		return syncer.DbHandle, nil
	}

	return nil, fmt.Errorf("DBHandle not found for syncer key: %s", syncerKey)
}

func (r *DefaultReporter) getReports(currentMs, aggregationIntervalMin int64, syncerKey string) (reports []*types.ReportByStatus, reportedAt int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT min(reported_at) FROM %s WHERE reported_at < $1`, ReportsTable)
	var queryMin sql.NullInt64
	dbHandle, err := r.getDBHandle(syncerKey)
	if err != nil {
		panic(err)
	}

	queryStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), r.dbQueryTimeout.Load())
	defer cancel()
	err = dbHandle.QueryRowContext(ctx, sqlStatement, currentMs).Scan(&queryMin)

	if err != nil && err != sql.ErrNoRows && ctx.Err() == nil {
		panic(err)
	}
	if ctx.Err() != nil {
		return nil, 0, fmt.Errorf("reporting query timeout")
	}

	r.getMinReportedAtQueryTime.Since(queryStart)
	if !queryMin.Valid {
		return nil, 0, nil
	}

	bucketStart, bucketEnd := GetAggregationBucketMinute(queryMin.Int64, aggregationIntervalMin)
	// we don't want to flush partial buckets, so we wait for the current bucket to be complete
	if bucketEnd > currentMs {
		return nil, 0, nil
	}

	groupByColumns := "workspace_id, namespace, instance_id, source_definition_id, source_category, source_id, destination_definition_id, destination_id, source_task_run_id, source_job_id, source_job_run_id, transformation_id, transformation_version_id, tracking_plan_id, tracking_plan_version, in_pu, pu, status, terminal_state, initial_state, status_code, event_name, event_type, error_type"
	sqlStatement = fmt.Sprintf(`
    SELECT
        %s, MAX(reported_at),
        COALESCE(
            (ARRAY_AGG(sample_response ORDER BY id DESC) FILTER (WHERE (sample_event != '{}' AND sample_event IS NOT NULL) OR (sample_response IS NOT NULL AND sample_response != '')))[1],
            ''
        ) AS sample_response,
        COALESCE(
            (ARRAY_AGG(sample_event ORDER BY id DESC) FILTER (WHERE (sample_event != '{}' AND sample_event IS NOT NULL) OR (sample_response IS NOT NULL AND sample_response != '')))[1],
            '{}'
        ) AS sample_event,
        SUM(count),
        SUM(violation_count)
    FROM
        %s
    WHERE
        reported_at >= $1 and reported_at < $2
		GROUP BY
        %s`, groupByColumns, ReportsTable, groupByColumns)
	var rows *sql.Rows
	queryStart = time.Now()
	rows, err = dbHandle.Query(sqlStatement, bucketStart, bucketEnd)
	if err != nil {
		panic(err)
	}

	r.getReportsQueryTime.Since(queryStart)
	defer func() { _ = rows.Close() }()

	var metricReports []*types.ReportByStatus
	for rows.Next() {
		metricReport := types.ReportByStatus{StatusDetail: &types.StatusDetail{}}
		var sampleEvent string
		err = rows.Scan(
			&metricReport.WorkspaceID, &metricReport.Namespace, &metricReport.InstanceID,
			&metricReport.SourceDefinitionID,
			&metricReport.SourceCategory,
			&metricReport.SourceID,
			&metricReport.DestinationDefinitionID,
			&metricReport.DestinationID,
			&metricReport.SourceTaskRunID,
			&metricReport.SourceJobID,
			&metricReport.SourceJobRunID,
			&metricReport.TransformationID,
			&metricReport.TransformationVersionID,
			&metricReport.TrackingPlanID,
			&metricReport.TrackingPlanVersion,
			&metricReport.InPU, &metricReport.PU,
			&metricReport.StatusDetail.Status,
			&metricReport.TerminalPU, &metricReport.InitialPU,
			&metricReport.StatusDetail.StatusCode,
			&metricReport.StatusDetail.EventName, &metricReport.StatusDetail.EventType,
			&metricReport.StatusDetail.ErrorType,
			&metricReport.ReportedAt,
			&metricReport.StatusDetail.SampleResponse, &sampleEvent,
			&metricReport.StatusDetail.Count, &metricReport.StatusDetail.ViolationCount,
		)
		if err != nil {
			panic(err)
		}
		metricReport.StatusDetail.SampleEvent = []byte(sampleEvent)
		metricReports = append(metricReports, &metricReport)
	}
	if err := rows.Err(); err != nil {
		// Handle rows error
		panic(err)
	}

	return metricReports, queryMin.Int64, err
}

func (r *DefaultReporter) getAggregatedReports(reports []*types.ReportByStatus) []*types.Metric {
	metricsByGroup := map[string]*types.Metric{}
	maxReportsCountInARequest := r.maxReportsCountInARequest.Load()
	sampleEventBucket, _ := GetAggregationBucketMinute(reports[0].ReportedAt, int64(r.eventSamplingDuration.Load().Minutes()))
	var values []*types.Metric

	reportIdentifier := func(report *types.ReportByStatus) string {
		groupingIdentifiers := []string{
			report.WorkspaceID, report.Namespace, report.InstanceID,
			report.SourceID,
			report.DestinationID,
			report.SourceTaskRunID,
			report.SourceJobID,
			report.SourceJobRunID,
			report.TransformationID,
			report.TransformationVersionID,
			report.TrackingPlanID,
			strconv.Itoa(report.TrackingPlanVersion),
			report.InPU, report.PU,
			strconv.FormatBool(report.TerminalPU), strconv.FormatBool(report.InitialPU),
			strconv.FormatInt(report.ReportedAt, 10),
		}
		return strings.Join(groupingIdentifiers, `::`)
	}

	for _, report := range reports {
		identifier := reportIdentifier(report)
		if _, ok := metricsByGroup[identifier]; !ok || len(metricsByGroup[identifier].StatusDetails) >= maxReportsCountInARequest {
			metricsByGroup[identifier] = &types.Metric{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: report.WorkspaceID,
					Namespace:   report.Namespace,
					InstanceID:  report.InstanceID,
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceDefinitionID:      report.SourceDefinitionID,
					SourceCategory:          report.SourceCategory,
					SourceID:                report.SourceID,
					DestinationDefinitionID: report.DestinationDefinitionID,
					DestinationID:           report.DestinationID,
					SourceTaskRunID:         report.SourceTaskRunID,
					SourceJobID:             report.SourceJobID,
					SourceJobRunID:          report.SourceJobRunID,
					TransformationID:        report.TransformationID,
					TransformationVersionID: report.TransformationVersionID,
					TrackingPlanID:          report.TrackingPlanID,
					TrackingPlanVersion:     report.TrackingPlanVersion,
				},
				PUDetails: types.PUDetails{
					InPU:       report.InPU,
					PU:         report.PU,
					TerminalPU: report.TerminalPU,
					InitialPU:  report.InitialPU,
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        report.ReportedAt * 60 * 1000, // send reportedAt in milliseconds
					SampleEventBucket: sampleEventBucket * 60 * 1000,
				},
			}
			values = append(values, metricsByGroup[identifier])
		}

		metricsByGroup[identifier].StatusDetails = append(metricsByGroup[identifier].StatusDetails, &types.StatusDetail{
			Status:         report.StatusDetail.Status,
			StatusCode:     report.StatusDetail.StatusCode,
			Count:          report.StatusDetail.Count,
			ViolationCount: report.StatusDetail.ViolationCount,
			SampleResponse: report.StatusDetail.SampleResponse,
			SampleEvent:    report.StatusDetail.SampleEvent,
			EventName:      report.StatusDetail.EventName,
			EventType:      report.StatusDetail.EventType,
			ErrorType:      report.StatusDetail.ErrorType,
		})
	}

	return values
}

func (r *DefaultReporter) emitLagMetric(ctx context.Context, c types.SyncerConfig, lastReportedAtTime *atomic.Time) error {
	// for monitoring reports pileups
	reportingLag := r.stats.NewTaggedStat(
		"reporting_metrics_lag_seconds", stats.GaugeType, stats.Tags{"client": c.Label},
	)
	for {
		lag := time.Since(lastReportedAtTime.Load())
		reportingLag.Gauge(lag.Seconds())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Minute):
		}
	}
}

func (r *DefaultReporter) mainLoop(ctx context.Context, c types.SyncerConfig) {
	r.configSubscriber.Wait()

	tags := r.getTags(c.Label)
	mainLoopTimer := r.stats.NewTaggedStat(StatReportingMainLoopTime, stats.TimerType, tags)
	getReportsTimer := r.stats.NewTaggedStat(StatReportingGetReportsTime, stats.TimerType, tags)
	getReportsCount := r.stats.NewTaggedStat(StatReportingGetReportsCount, stats.HistogramType, tags)
	getAggregatedReportsTimer := r.stats.NewTaggedStat(StatReportingGetAggregatedReportsTime, stats.TimerType, tags)
	getAggregatedReportsCount := r.stats.NewTaggedStat(StatReportingGetAggregatedReportsCount, stats.HistogramType, tags)

	r.getMinReportedAtQueryTime = r.stats.NewTaggedStat(StatReportingGetMinReportedAtQueryTime, stats.TimerType, tags)
	r.getReportsQueryTime = r.stats.NewTaggedStat(StatReportingGetReportsQueryTime, stats.TimerType, tags)

	var lastReportedAtTime atomic.Time
	lastReportedAtTime.Store(time.Now())

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.emitLagMetric(ctx, c, &lastReportedAtTime)
	})

	g.Go(func() error {
		var (
			deletedRows                int
			vacuumDeletedRowsThreshold = config.GetReloadableIntVar(100000, 1, "Reporting.vacuumThresholdDeletedRows")
			lastVacuum                 time.Time
			vacuumInterval             = config.GetReloadableDurationVar(15, time.Minute, "Reporting.vacuumInterval")
			vacuumThresholdBytes       = config.GetReloadableInt64Var(10*bytesize.GB, 1, "Reporting.vacuumThresholdBytes")
			aggregationInterval        = config.GetReloadableDurationVar(1, time.Minute, "Reporting.aggregationIntervalMinutes") // Values should be a factor of 60 or else we will panic, for example 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60
		)
		for {
			if ctx.Err() != nil {
				r.log.Infon("stopping mainLoop for syncer",
					logger.NewStringField("label", c.Label),
					obskit.Error(ctx.Err()))
				return ctx.Err()
			}
			requestChan := make(chan struct{}, r.maxConcurrentRequests.Load())
			loopStart := time.Now()
			currentMin := time.Now().UTC().Unix() / 60

			getReportsStart := time.Now()
			aggregationIntervalMin := int64(aggregationInterval.Load().Minutes())
			reports, reportedAt, err := r.getReports(currentMin, aggregationIntervalMin, c.ConnInfo)
			if err != nil {
				r.log.Errorn("getting reports", obskit.Error(err))
				select {
				case <-ctx.Done():
					r.log.Infon("stopping mainLoop for syncer",
						logger.NewStringField("label", c.Label),
						obskit.Error(ctx.Err()))
					return ctx.Err()
				case <-time.After(r.mainLoopSleepInterval.Load()):
				}
				continue
			}

			getReportsTimer.Since(getReportsStart)
			getReportsCount.Observe(float64(len(reports)))
			if len(reports) == 0 {
				lastReportedAtTime.Store(loopStart)
				select {
				case <-ctx.Done():
					r.log.Infon("stopping mainLoop for syncer",
						logger.NewStringField("label", c.Label),
						obskit.Error(ctx.Err()))
					return ctx.Err()
				case <-time.After(r.sleepInterval.Load()):
				}
				continue
			}

			lastReportedAtTime.Store(time.Unix(reportedAt*60, 0))
			getAggregatedReportsStart := time.Now()
			metrics := r.getAggregatedReports(reports)
			getAggregatedReportsTimer.Since(getAggregatedReportsStart)
			getAggregatedReportsCount.Observe(float64(len(metrics)))

			errGroup, errCtx := errgroup.WithContext(ctx)
			for _, metric := range metrics {
				if r.whActionsOnly && metric.SourceCategory != "warehouse" {
					// if whActionsOnly is true, we only send reports for wh actions sources
					// we silently drop all other reports
					continue
				}
				metricToSend := metric
				requestChan <- struct{}{}
				if errCtx.Err() != nil {
					// if any of errGroup's goroutines fail - don't send anymore requests for this batch
					break
				}
				errGroup.Go(func() error {
					err := r.commonClient.Send(errCtx, metricToSend)
					<-requestChan
					return err
				})
			}

			err = errGroup.Wait()
			if err != nil {
				r.log.Errorn(`[ Reporting ]: Error sending metrics to service`, obskit.Error(err))
			} else {
				dbHandle, err := r.getDBHandle(c.ConnInfo)
				if err != nil {
					return err
				}
				// Use the same aggregationIntervalMin value that was used to query the reports in getReports()
				bucketStart, bucketEnd := GetAggregationBucketMinute(reportedAt, aggregationIntervalMin)
				_, err = dbHandle.Exec(`DELETE FROM `+ReportsTable+` WHERE reported_at >= $1 and reported_at < $2`, bucketStart, bucketEnd)
				if err != nil {
					r.log.Errorn(`[ Reporting ]: Error deleting local reports from table`,
						logger.NewStringField("table", ReportsTable),
						obskit.Error(err))
				} else {
					deletedRows += len(reports)
				}

				// vacuum the table
				if deletedRows >= vacuumDeletedRowsThreshold.Load() {
					if err := r.vacuum(ctx, dbHandle, tags); err == nil {
						deletedRows = 0
						lastVacuum = time.Now()
					}
				} else if time.Since(lastVacuum) > vacuumInterval.Load() {
					var sizeEstimate int64
					if err := dbHandle.QueryRowContext(
						ctx,
						fmt.Sprintf(`SELECT pg_table_size(oid) from pg_class where relname='%s';`, ReportsTable),
					).Scan(&sizeEstimate); err != nil {
						r.log.Errorn(
							`[ Reporting ]: Error getting table size estimate`,
							obskit.Error(err),
						)
					}
					if sizeEstimate >= vacuumThresholdBytes.Load() {
						if err := r.vacuum(ctx, dbHandle, tags); err == nil {
							deletedRows = 0
							lastVacuum = time.Now()
						}
					}
				}
			}

			mainLoopTimer.Since(loopStart)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.mainLoopSleepInterval.Load()):
			}
		}
	})

	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}

func (r *DefaultReporter) vacuum(ctx context.Context, db *sql.DB, tags stats.Tags) error {
	defer r.stats.NewTaggedStat(StatReportingVacuumDuration, stats.TimerType, tags).RecordDuration()()
	var (
		query string
		full  bool
	)
	if r.vacuumFull.Load() {
		full = true
		query = `vacuum full analyze reports;`
	} else {
		query = `vacuum analyze reports;`
	}
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		r.log.Errorn(
			`[ Reporting ]: Error vacuuming reports table`,
			obskit.Error(err),
			logger.NewBoolField("full", full),
		)
	}
	return nil
}

func (r *DefaultReporter) Report(ctx context.Context, metrics []*types.PUReportedMetric, txn *Tx) error {
	if len(metrics) == 0 {
		return nil
	}

	prefixLength := r.eventNamePrefixLength.Load()
	suffixLength := r.eventNameSuffixLength.Load()
	maxLength := prefixLength + suffixLength
	if prefixLength <= 0 || suffixLength <= 0 {
		err := errors.New("invalid event name trimming configuration prefixLength and suffixLength must be > 0")
		r.log.Errorn(`[ Reporting ]: Invalid event name trimming configuration`, obskit.Error(err),
			logger.NewIntField("prefixLength", int64(prefixLength)),
			logger.NewIntField("suffixLength", int64(suffixLength)))
		return err
	}

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(ReportsTable,
		"workspace_id", "namespace", "instance_id",
		"source_definition_id",
		"source_category",
		"source_id",
		"destination_definition_id",
		"destination_id",
		"source_task_run_id",
		"source_job_id",
		"source_job_run_id",
		"transformation_id",
		"transformation_version_id",
		"tracking_plan_id",
		"tracking_plan_version",
		"in_pu", "pu",
		"reported_at",
		"status",
		"count", "violation_count",
		"terminal_state", "initial_state",
		"status_code",
		"sample_response", "sample_event",
		"event_name", "event_type",
		"error_type",
	))
	if err != nil {
		return fmt.Errorf("preparing statement: %v", err)
	}
	defer func() { _ = stmt.Close() }()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := r.configSubscriber.WorkspaceIDFromSource(metric.SourceID)
		metric := *metric

		if metric.SourceCategory == "warehouse" || slices.Contains(r.sourcesWithEventNameTrackingDisabled, metric.SourceID) {
			metric.StatusDetail.EventName = metric.StatusDetail.EventType
		}

		if r.configSubscriber.IsPIIReportingDisabled(workspaceID) {
			metric = transformMetricForPII(metric, getPIIColumnsToExclude())
		}

		sampleEvent, sampleResponse, err := getSampleWithEventSampling(metric, reportedAt, r.eventSampler, r.eventSamplingEnabled.Load(), int64(r.eventSamplingDuration.Load().Minutes()))
		if err != nil {
			return err
		}

		runeEventName := []rune(metric.StatusDetail.EventName)
		if len(runeEventName) > maxLength {
			metric.StatusDetail.EventName = fmt.Sprintf("%s...%s", string(runeEventName[:prefixLength]), string(runeEventName[len(runeEventName)-suffixLength:]))
		}

		_, err = stmt.Exec(
			workspaceID, r.namespace, r.instanceID,
			metric.SourceDefinitionID,
			metric.SourceCategory,
			metric.SourceID,
			metric.DestinationDefinitionID,
			metric.DestinationID,
			metric.SourceTaskRunID,
			metric.SourceJobID,
			metric.SourceJobRunID,
			metric.TransformationID,
			metric.TransformationVersionID,
			metric.TrackingPlanID,
			metric.TrackingPlanVersion,
			metric.InPU, metric.PU,
			reportedAt,
			metric.StatusDetail.Status,
			metric.StatusDetail.Count, metric.StatusDetail.ViolationCount,
			metric.TerminalPU, metric.InitialPU,
			metric.StatusDetail.StatusCode,
			sampleResponse, getStringifiedSampleEvent(sampleEvent),
			metric.StatusDetail.EventName, metric.StatusDetail.EventType,
			metric.StatusDetail.ErrorType,
		)
		if err != nil {
			return fmt.Errorf("executing statement: %v", err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("executing final statement: %v", err)
	}

	return nil
}

func (r *DefaultReporter) getTags(label string) stats.Tags {
	return stats.Tags{
		"workspaceId": r.configSubscriber.WorkspaceID(),
		"instanceId":  r.instanceID,
		"clientName":  label,
	}
}

func (r *DefaultReporter) Stop() {
	r.cancel()
	_ = r.g.Wait()

	if r.eventSampler != nil {
		r.eventSampler.Close()
	}
}

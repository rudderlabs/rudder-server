package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
	StatReportingHttpReqLatency            = "reporting_client_http_request_latency"
	StatReportingHttpReq                   = "reporting_client_http_request"
	StatReportingGetMinReportedAtQueryTime = "reporting_client_get_min_reported_at_query_time"
	StatReportingGetReportsQueryTime       = "reporting_client_get_reports_query_time"
)

type DefaultReporter struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	g                   *errgroup.Group
	configSubscriber    *configSubscriber
	syncersMu           sync.RWMutex
	syncers             map[string]*types.SyncSource
	log                 logger.Logger
	reportingServiceURL string
	namespace           string

	instanceID                           string
	whActionsOnly                        bool
	region                               string
	sleepInterval                        misc.ValueLoader[time.Duration]
	mainLoopSleepInterval                misc.ValueLoader[time.Duration]
	dbQueryTimeout                       *config.Reloadable[time.Duration]
	sourcesWithEventNameTrackingDisabled []string
	maxOpenConnections                   int
	maxConcurrentRequests                misc.ValueLoader[int]

	getMinReportedAtQueryTime stats.Measurement
	getReportsQueryTime       stats.Measurement
	requestLatency            stats.Measurement
	stats                     stats.Stats
}

func NewDefaultReporter(ctx context.Context, log logger.Logger, configSubscriber *configSubscriber, stats stats.Stats) *DefaultReporter {
	var dbQueryTimeout *config.Reloadable[time.Duration]

	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")
	sourcesWithEventNameTrackingDisabled := config.GetStringSlice("Reporting.sourcesWithEventNameTrackingDisabled", []string{})

	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := config.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := config.GetIntVar(32, 1, "Reporting.maxOpenConnections")
	dbQueryTimeout = config.GetReloadableDurationVar(60, time.Second, "Reporting.dbQueryTimeout")
	// only send reports for wh actions sources if whActionsOnly is configured
	whActionsOnly := config.GetBool("REPORTING_WH_ACTIONS_ONLY", false)
	if whActionsOnly {
		log.Info("REPORTING_WH_ACTIONS_ONLY enabled.only sending reports relevant to wh actions.")
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
		reportingServiceURL:                  reportingServiceURL,
		namespace:                            config.GetKubeNamespace(),
		instanceID:                           config.GetString("INSTANCE_ID", "1"),
		whActionsOnly:                        whActionsOnly,
		sleepInterval:                        sleepInterval,
		mainLoopSleepInterval:                mainLoopSleepInterval,
		region:                               config.GetString("region", ""),
		sourcesWithEventNameTrackingDisabled: sourcesWithEventNameTrackingDisabled,
		maxOpenConnections:                   maxOpenConnections,
		maxConcurrentRequests:                maxConcurrentRequests,
		dbQueryTimeout:                       dbQueryTimeout,
		stats:                                stats,
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

func (r *DefaultReporter) getReportsV0(currentMs int64, syncerKey string) (reports []*types.ReportByStatus, reportedAt int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT reported_at FROM %s WHERE reported_at < %d ORDER BY reported_at ASC LIMIT 1`, ReportsTable, currentMs)
	var queryMin sql.NullInt64
	dbHandle, err := r.getDBHandle(syncerKey)
	if err != nil {
		panic(err)
	}

	queryStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), r.dbQueryTimeout.Load())
	defer cancel()
	err = dbHandle.QueryRowContext(ctx, sqlStatement).Scan(&queryMin)

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

	sqlStatement = fmt.Sprintf(`SELECT workspace_id, namespace, instance_id, source_definition_id, source_category, source_id, destination_definition_id, destination_id, source_task_run_id, source_job_id, source_job_run_id, transformation_id, transformation_version_id, tracking_plan_id, tracking_plan_version, in_pu, pu, reported_at, status, count, violation_count, terminal_state, initial_state, status_code, sample_response, sample_event, event_name, event_type, error_type FROM %s WHERE reported_at = %d order by id`, ReportsTable, queryMin.Int64)
	var rows *sql.Rows
	queryStart = time.Now()
	rows, err = dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}

	r.getReportsQueryTime.Since(queryStart)
	defer func() { _ = rows.Close() }()

	var metricReports []*types.ReportByStatus
	for rows.Next() {
		metricReport := types.ReportByStatus{StatusDetail: &types.StatusDetail{}}
		err = rows.Scan(
			&metricReport.InstanceDetails.WorkspaceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID,
			&metricReport.ConnectionDetails.SourceDefinitionId,
			&metricReport.ConnectionDetails.SourceCategory,
			&metricReport.ConnectionDetails.SourceID,
			&metricReport.ConnectionDetails.DestinationDefinitionId,
			&metricReport.ConnectionDetails.DestinationID,
			&metricReport.ConnectionDetails.SourceTaskRunID,
			&metricReport.ConnectionDetails.SourceJobID,
			&metricReport.ConnectionDetails.SourceJobRunID,
			&metricReport.ConnectionDetails.TransformationID,
			&metricReport.ConnectionDetails.TransformationVersionID,
			&metricReport.ConnectionDetails.TrackingPlanID,
			&metricReport.ConnectionDetails.TrackingPlanVersion,
			&metricReport.PUDetails.InPU, &metricReport.PUDetails.PU,
			&metricReport.ReportedAt,
			&metricReport.StatusDetail.Status,
			&metricReport.StatusDetail.Count, &metricReport.StatusDetail.ViolationCount,
			&metricReport.PUDetails.TerminalPU, &metricReport.PUDetails.InitialPU,
			&metricReport.StatusDetail.StatusCode,
			&metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent,
			&metricReport.StatusDetail.EventName, &metricReport.StatusDetail.EventType,
			&metricReport.StatusDetail.ErrorType,
		)
		if err != nil {
			panic(err)
		}
		metricReports = append(metricReports, &metricReport)
	}
	if err := rows.Err(); err != nil {
		// Handle rows error
		panic(err)
	}

	return metricReports, queryMin.Int64, err
}

func (*DefaultReporter) getAggregatedReportsV0(reports []*types.ReportByStatus) []*types.Metric {
	metricsByGroup := map[string]*types.Metric{}

	reportIdentifier := func(report *types.ReportByStatus) string {
		groupingIdentifiers := []string{
			report.InstanceDetails.WorkspaceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID,
			report.ConnectionDetails.SourceID,
			report.ConnectionDetails.DestinationID,
			report.ConnectionDetails.SourceTaskRunID,
			report.ConnectionDetails.SourceJobID,
			report.ConnectionDetails.SourceJobRunID,
			report.ConnectionDetails.TransformationID,
			report.ConnectionDetails.TransformationVersionID,
			report.ConnectionDetails.TrackingPlanID,
			strconv.Itoa(report.ConnectionDetails.TrackingPlanVersion),
			report.PUDetails.InPU, report.PUDetails.PU,
			report.StatusDetail.Status,
			strconv.Itoa(report.StatusDetail.StatusCode),
			report.StatusDetail.EventName, report.StatusDetail.EventType,
		}
		return strings.Join(groupingIdentifiers, `::`)
	}

	for _, report := range reports {
		identifier := reportIdentifier(report)
		if _, ok := metricsByGroup[identifier]; !ok {
			metricsByGroup[identifier] = &types.Metric{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: report.WorkspaceID,
					Namespace:   report.Namespace,
					InstanceID:  report.InstanceID,
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceDefinitionId:      report.SourceDefinitionId,
					SourceCategory:          report.SourceCategory,
					SourceID:                report.SourceID,
					DestinationDefinitionId: report.DestinationDefinitionId,
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
					ReportedAt: report.ReportedAt * 60 * 1000, // send reportedAt in milliseconds
				},
			}
		}
		statusDetailInterface, found := lo.Find(metricsByGroup[identifier].StatusDetails, func(i *types.StatusDetail) bool {
			return i.Status == report.StatusDetail.Status && i.StatusCode == report.StatusDetail.StatusCode && i.ErrorType == report.StatusDetail.ErrorType
		})
		if !found {
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
			continue
		}
		statusDetail := statusDetailInterface
		statusDetail.Count += report.StatusDetail.Count
		statusDetail.ViolationCount += report.StatusDetail.ViolationCount
		statusDetail.SampleResponse = report.StatusDetail.SampleResponse
		statusDetail.SampleEvent = report.StatusDetail.SampleEvent
	}

	var values []*types.Metric
	for _, val := range metricsByGroup {
		values = append(values, val)
	}
	return values
}

func (r *DefaultReporter) getReports(currentMs int64, syncerKey string) (reports []*types.ReportByStatus, reportedAt int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT min(reported_at) FROM %s WHERE reported_at < %d`, ReportsTable, currentMs)
	var queryMin sql.NullInt64
	dbHandle, err := r.getDBHandle(syncerKey)
	if err != nil {
		panic(err)
	}

	queryStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), r.dbQueryTimeout.Load())
	defer cancel()
	err = dbHandle.QueryRowContext(ctx, sqlStatement).Scan(&queryMin)

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

	groupByColumns := "workspace_id, namespace, instance_id, source_definition_id, source_category, source_id, destination_definition_id, destination_id, source_task_run_id, source_job_id, source_job_run_id, transformation_id, transformation_version_id, tracking_plan_id, tracking_plan_version, in_pu, pu, reported_at, status, terminal_state, initial_state, status_code, event_name, event_type, error_type"
	sqlStatement = fmt.Sprintf(`SELECT %s, (ARRAY_AGG(sample_response order by id desc))[1], (ARRAY_AGG(sample_event order by id desc))[1], SUM(count), SUM(violation_count) FROM %s WHERE reported_at = %d GROUP BY %s`, groupByColumns, ReportsTable, queryMin.Int64, groupByColumns)
	var rows *sql.Rows
	queryStart = time.Now()
	rows, err = dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}

	r.getReportsQueryTime.Since(queryStart)
	defer func() { _ = rows.Close() }()

	var metricReports []*types.ReportByStatus
	for rows.Next() {
		metricReport := types.ReportByStatus{StatusDetail: &types.StatusDetail{}}
		err = rows.Scan(
			&metricReport.InstanceDetails.WorkspaceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID,
			&metricReport.ConnectionDetails.SourceDefinitionId,
			&metricReport.ConnectionDetails.SourceCategory,
			&metricReport.ConnectionDetails.SourceID,
			&metricReport.ConnectionDetails.DestinationDefinitionId,
			&metricReport.ConnectionDetails.DestinationID,
			&metricReport.ConnectionDetails.SourceTaskRunID,
			&metricReport.ConnectionDetails.SourceJobID,
			&metricReport.ConnectionDetails.SourceJobRunID,
			&metricReport.ConnectionDetails.TransformationID,
			&metricReport.ConnectionDetails.TransformationVersionID,
			&metricReport.ConnectionDetails.TrackingPlanID,
			&metricReport.ConnectionDetails.TrackingPlanVersion,
			&metricReport.PUDetails.InPU, &metricReport.PUDetails.PU,
			&metricReport.ReportedAt,
			&metricReport.StatusDetail.Status,
			&metricReport.PUDetails.TerminalPU, &metricReport.PUDetails.InitialPU,
			&metricReport.StatusDetail.StatusCode,
			&metricReport.StatusDetail.EventName, &metricReport.StatusDetail.EventType,
			&metricReport.StatusDetail.ErrorType,
			&metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent,
			&metricReport.StatusDetail.Count, &metricReport.StatusDetail.ViolationCount,
		)
		if err != nil {
			panic(err)
		}
		metricReports = append(metricReports, &metricReport)
	}
	if err := rows.Err(); err != nil {
		// Handle rows error
		panic(err)
	}

	return metricReports, queryMin.Int64, err
}

func (*DefaultReporter) getAggregatedReports(reports []*types.ReportByStatus) []*types.Metric {
	metricsByGroup := map[string]*types.Metric{}
	var values []*types.Metric

	reportIdentifier := func(report *types.ReportByStatus) string {
		groupingIdentifiers := []string{
			report.InstanceDetails.WorkspaceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID,
			report.ConnectionDetails.SourceID,
			report.ConnectionDetails.DestinationID,
			report.ConnectionDetails.SourceTaskRunID,
			report.ConnectionDetails.SourceJobID,
			report.ConnectionDetails.SourceJobRunID,
			report.ConnectionDetails.TransformationID,
			report.ConnectionDetails.TransformationVersionID,
			report.ConnectionDetails.TrackingPlanID,
			strconv.Itoa(report.ConnectionDetails.TrackingPlanVersion),
			report.PUDetails.InPU, report.PUDetails.PU,
			report.StatusDetail.Status,
			strconv.Itoa(report.StatusDetail.StatusCode),
			report.StatusDetail.EventName, report.StatusDetail.EventType,
		}
		return strings.Join(groupingIdentifiers, `::`)
	}

	for _, report := range reports {
		identifier := reportIdentifier(report)
		if _, ok := metricsByGroup[identifier]; !ok {
			metricsByGroup[identifier] = &types.Metric{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: report.WorkspaceID,
					Namespace:   report.Namespace,
					InstanceID:  report.InstanceID,
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceDefinitionId:      report.SourceDefinitionId,
					SourceCategory:          report.SourceCategory,
					SourceID:                report.SourceID,
					DestinationDefinitionId: report.DestinationDefinitionId,
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
					ReportedAt: report.ReportedAt * 60 * 1000, // send reportedAt in milliseconds
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
			return nil
		case <-time.After(2 * time.Minute):
		}
	}
}

func (r *DefaultReporter) mainLoop(ctx context.Context, c types.SyncerConfig) {
	r.configSubscriber.Wait()

	// tr := &http.Transport{}
	// netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	tags := r.getTags(c.Label)
	mainLoopTimer := r.stats.NewTaggedStat(StatReportingMainLoopTime, stats.TimerType, tags)
	getReportsTimer := r.stats.NewTaggedStat(StatReportingGetReportsTime, stats.TimerType, tags)
	getReportsCount := r.stats.NewTaggedStat(StatReportingGetReportsCount, stats.HistogramType, tags)
	getAggregatedReportsTimer := r.stats.NewTaggedStat(StatReportingGetAggregatedReportsTime, stats.TimerType, tags)
	getAggregatedReportsCount := r.stats.NewTaggedStat(StatReportingGetAggregatedReportsCount, stats.HistogramType, tags)

	r.getMinReportedAtQueryTime = r.stats.NewTaggedStat(StatReportingGetMinReportedAtQueryTime, stats.TimerType, tags)
	r.getReportsQueryTime = r.stats.NewTaggedStat(StatReportingGetReportsQueryTime, stats.TimerType, tags)
	r.requestLatency = r.stats.NewTaggedStat(StatReportingHttpReqLatency, stats.TimerType, tags)

	var lastReportedAtTime atomic.Time
	lastReportedAtTime.Store(time.Now())

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.emitLagMetric(ctx, c, &lastReportedAtTime)
	})

	g.Go(func() error {
		for {
			if ctx.Err() != nil {
				r.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
				return ctx.Err()
			}
			// requestChan := make(chan struct{}, r.maxConcurrentRequests.Load())
			loopStart := time.Now()
			currentMs := time.Now().UTC().Unix() / 60

			getReportsStart := time.Now()
			reports, reportedAt, err := r.getReports(currentMs, c.ConnInfo)
			fmt.Println("timing for getReports ", time.Since(getReportsStart).Seconds()*1000)

			getReportsStartV0 := time.Now()
			reportsV0, _, _ := r.getReportsV0(currentMs, c.ConnInfo)
			fmt.Println("timing for getReportsV0 ", time.Since(getReportsStartV0).Seconds()*1000)

			getReportsTimer.Since(getReportsStart)
			getReportsCount.Observe(float64(len(reports)))
			if len(reports) == 0 {
				if err == nil {
					lastReportedAtTime.Store(loopStart)
				} else {
					r.log.Errorw("getting reports", "error", err)
				}
				select {
				case <-ctx.Done():
					r.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
					return nil
				case <-time.After(r.sleepInterval.Load()):
				}
				continue
			}

			lastReportedAtTime.Store(time.Unix(reportedAt*60, 0))
			getAggregatedReportsStart := time.Now()
			metrics := r.getAggregatedReports(reports)
			fmt.Println("timing for getAggregatedReports ", time.Since(getAggregatedReportsStart).Seconds()*1000)
			getAggregatedReportsTimer.Since(getAggregatedReportsStart)
			getAggregatedReportsCount.Observe(float64(len(metrics)))

			getAggregatedReportsStartV0 := time.Now()
			metricsV0 := r.getAggregatedReportsV0(reportsV0)
			fmt.Println("timing for getAggregatedReportsV0 ", time.Since(getAggregatedReportsStartV0).Seconds()*1000)

			// compare metrics and metricsV0 values
			if len(metrics) != len(metricsV0) {
				panic("mismatch in metrics and metricsV0 length. metrics")
			}

			seen := make(map[*types.Metric]bool)

			for i := 0; i < len(metrics); i++ {
				sort.Slice(metrics[i].StatusDetails, func(x int, y int) bool {
					return metrics[i].StatusDetails[x].ErrorType < metrics[i].StatusDetails[y].ErrorType
				})

				sort.Slice(metricsV0[i].StatusDetails, func(x int, y int) bool {
					return metricsV0[i].StatusDetails[x].ErrorType < metricsV0[i].StatusDetails[y].ErrorType
				})
			}

			for _, cur := range metrics {
				jsonCur, _ := json.Marshal(cur)
				match, found := lo.Find(metricsV0, func(m *types.Metric) bool {
					jsonMatch, _ := json.Marshal(m)
					return string(jsonCur) == string(jsonMatch)
				})

				if !found {
					fmt.Println(string(jsonCur))
					panic("mismatch in metrics and metricsV0")
				}

				if seen[match] {
					panic("duplicate metrics in metricsV0")
				}

				if len(cur.StatusDetails) != len(match.StatusDetails) {
					panic("mismatch in status details length")
				}

				seen[match] = true
			}

			// errGroup, errCtx := errgroup.WithContext(ctx)
			// for _, metric := range metrics {
			// 	if r.whActionsOnly && metric.SourceCategory != "warehouse" {
			// 		// if whActionsOnly is true, we only send reports for wh actions sources
			// 		// we silently drop all other reports
			// 		continue
			// 	}
			// 	metricToSend := metric
			// 	requestChan <- struct{}{}
			// 	if errCtx.Err() != nil {
			// 		// if any of errGroup's goroutines fail - don't send anymore requests for this batch
			// 		break
			// 	}
			// 	errGroup.Go(func() error {
			// 		err := r.sendMetric(errCtx, netClient, c.Label, metricToSend)
			// 		<-requestChan
			// 		return err
			// 	})
			// }

			// err = errGroup.Wait()
			// if err == nil {
			// 	dbHandle, err := r.getDBHandle(c.ConnInfo)
			// 	if err != nil {
			// 		panic(err)
			// 	}
			// 	_, err = dbHandle.Exec(`DELETE FROM `+ReportsTable+` WHERE reported_at = $1`, reportedAt)
			// 	if err != nil {
			// 		r.log.Errorf(`[ Reporting ]: Error deleting local reports from %s: %v`, ReportsTable, err)
			// 	}
			// }

			mainLoopTimer.Since(loopStart)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.mainLoopSleepInterval.Load()):
			}
		}
	})

	_ = g.Wait()
}

func isMetricPosted(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

func getPIIColumnsToExclude() []string {
	piiColumnsToExclude := strings.Split(config.GetString("REPORTING_PII_COLUMNS_TO_EXCLUDE", "sample_event,sample_response"), ",")
	for i := range piiColumnsToExclude {
		piiColumnsToExclude[i] = strings.Trim(piiColumnsToExclude[i], " ")
	}
	return piiColumnsToExclude
}

func transformMetricForPII(metric types.PUReportedMetric, piiColumns []string) types.PUReportedMetric {
	for _, col := range piiColumns {
		switch col {
		case "sample_event":
			metric.StatusDetail.SampleEvent = []byte(`{}`)
		case "sample_response":
			metric.StatusDetail.SampleResponse = ""
		case "event_name":
			metric.StatusDetail.EventName = ""
		case "event_type":
			metric.StatusDetail.EventType = ""
		}
	}

	return metric
}

func (r *DefaultReporter) Report(ctx context.Context, metrics []*types.PUReportedMetric, txn *Tx) error {
	if len(metrics) == 0 {
		return nil
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

	eventNameMaxLength := config.GetInt("Reporting.eventNameMaxLength", 0)
	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := r.configSubscriber.WorkspaceIDFromSource(metric.ConnectionDetails.SourceID)
		metric := *metric

		if slices.Contains(r.sourcesWithEventNameTrackingDisabled, metric.ConnectionDetails.SourceID) {
			metric.StatusDetail.EventName = metric.StatusDetail.EventType
		}

		if r.configSubscriber.IsPIIReportingDisabled(workspaceID) {
			metric = transformMetricForPII(metric, getPIIColumnsToExclude())
		}

		if eventNameMaxLength > 0 && len(metric.StatusDetail.EventName) > eventNameMaxLength {
			metric.StatusDetail.EventName = types.MaxLengthExceeded
		}

		_, err = stmt.Exec(
			workspaceID, r.namespace, r.instanceID,
			metric.ConnectionDetails.SourceDefinitionId,
			metric.ConnectionDetails.SourceCategory,
			metric.ConnectionDetails.SourceID,
			metric.ConnectionDetails.DestinationDefinitionId,
			metric.ConnectionDetails.DestinationID,
			metric.ConnectionDetails.SourceTaskRunID,
			metric.ConnectionDetails.SourceJobID,
			metric.ConnectionDetails.SourceJobRunID,
			metric.ConnectionDetails.TransformationID,
			metric.ConnectionDetails.TransformationVersionID,
			metric.ConnectionDetails.TrackingPlanID,
			metric.ConnectionDetails.TrackingPlanVersion,
			metric.PUDetails.InPU, metric.PUDetails.PU,
			reportedAt,
			metric.StatusDetail.Status,
			metric.StatusDetail.Count, metric.StatusDetail.ViolationCount,
			metric.PUDetails.TerminalPU, metric.PUDetails.InitialPU,
			metric.StatusDetail.StatusCode,
			metric.StatusDetail.SampleResponse, string(metric.StatusDetail.SampleEvent),
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
}

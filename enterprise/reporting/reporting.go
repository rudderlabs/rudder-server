package reporting

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/cenkalti/backoff/v4"
	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/httputil"
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
	StatReportingVacuumDuration            = "reporting_vacuum_duration"
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
	sleepInterval                        config.ValueLoader[time.Duration]
	mainLoopSleepInterval                config.ValueLoader[time.Duration]
	dbQueryTimeout                       *config.Reloadable[time.Duration]
	sourcesWithEventNameTrackingDisabled []string
	maxOpenConnections                   int
	maxConcurrentRequests                config.ValueLoader[int]
	vacuumFull                           config.ValueLoader[bool]

	getMinReportedAtQueryTime stats.Measurement
	getReportsQueryTime       stats.Measurement
	requestLatency            stats.Measurement
	stats                     stats.Stats
	maxReportsCountInARequest config.ValueLoader[int]

	eventSamplingEnabled config.ValueLoader[bool]
	eventSampler         *EventSampler
}

func NewDefaultReporter(ctx context.Context, conf *config.Config, log logger.Logger, configSubscriber *configSubscriber, stats stats.Stats) *DefaultReporter {
	var dbQueryTimeout *config.Reloadable[time.Duration]
	var eventSampler *EventSampler

	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")
	sourcesWithEventNameTrackingDisabled := config.GetStringSlice("Reporting.sourcesWithEventNameTrackingDisabled", []string{})

	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := config.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := config.GetIntVar(32, 1, "Reporting.maxOpenConnections")
	dbQueryTimeout = config.GetReloadableDurationVar(60, time.Second, "Reporting.dbQueryTimeout")
	maxReportsCountInARequest := conf.GetReloadableIntVar(10, 1, "Reporting.maxReportsCountInARequest")
	eventSamplingEnabled := conf.GetReloadableBoolVar(false, "Reporting.eventSamplingEnabled")
	eventSamplingDuration := conf.GetReloadableDurationVar(60, time.Minute, "Reporting.eventSamplingDurationInMinutes")
	// only send reports for wh actions sources if whActionsOnly is configured
	whActionsOnly := config.GetBool("REPORTING_WH_ACTIONS_ONLY", false)
	if whActionsOnly {
		log.Info("REPORTING_WH_ACTIONS_ONLY enabled.only sending reports relevant to wh actions.")
	}

	if eventSamplingEnabled.Load() {
		var err error
		eventSampler, err = NewEventSampler("/reporting-badger", eventSamplingDuration, conf, log)
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
		reportingServiceURL:                  reportingServiceURL,
		namespace:                            config.GetKubeNamespace(),
		instanceID:                           config.GetString("INSTANCE_ID", "1"),
		whActionsOnly:                        whActionsOnly,
		sleepInterval:                        sleepInterval,
		mainLoopSleepInterval:                mainLoopSleepInterval,
		vacuumFull:                           config.GetReloadableBoolVar(false, "Reporting.vacuumFull"),
		region:                               config.GetString("region", ""),
		sourcesWithEventNameTrackingDisabled: sourcesWithEventNameTrackingDisabled,
		maxOpenConnections:                   maxOpenConnections,
		maxConcurrentRequests:                maxConcurrentRequests,
		dbQueryTimeout:                       dbQueryTimeout,
		maxReportsCountInARequest:            maxReportsCountInARequest,
		stats:                                stats,
		eventSamplingEnabled:                 eventSamplingEnabled,
		eventSampler:                         eventSampler,
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
	if _, err := dbHandle.ExecContext(context.Background(), `vacuum full analyze reports;`); err != nil {
		r.log.Errorn(`[ Reporting ]: Error full vacuuming reports table`, logger.NewErrorField(err))
		panic(err)
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

func (r *DefaultReporter) getReports(currentMs int64, syncerKey string) (reports []*types.ReportByStatus, reportedAt int64, err error) {
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

	groupByColumns := "workspace_id, namespace, instance_id, source_definition_id, source_category, source_id, destination_definition_id, destination_id, source_task_run_id, source_job_id, source_job_run_id, transformation_id, transformation_version_id, tracking_plan_id, tracking_plan_version, in_pu, pu, reported_at, status, terminal_state, initial_state, status_code, event_name, event_type, error_type"
	sqlStatement = fmt.Sprintf(`SELECT %s, (ARRAY_AGG(sample_response order by id))[1], (ARRAY_AGG(sample_event order by id))[1], SUM(count), SUM(violation_count) FROM %s WHERE reported_at = $1 GROUP BY %s`, groupByColumns, ReportsTable, groupByColumns)
	var rows *sql.Rows
	queryStart = time.Now()
	rows, err = dbHandle.Query(sqlStatement, queryMin.Int64)
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

func (r *DefaultReporter) getAggregatedReports(reports []*types.ReportByStatus) []*types.Metric {
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

		if len(metricsByGroup[identifier].StatusDetails) >= r.maxReportsCountInARequest.Load() {
			delete(metricsByGroup, identifier)
		}
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

	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
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
		var (
			deletedRows                int
			vacuumDeletedRowsThreshold = config.GetReloadableIntVar(100000, 1, "Reporting.vacuumThresholdDeletedRows")
			lastVacuum                 time.Time
			vacuumInterval             = config.GetReloadableDurationVar(15, time.Minute, "Reporting.vacuumInterval")
			vacuumThresholdBytes       = config.GetReloadableInt64Var(10*bytesize.GB, 1, "Reporting.vacuumThresholdBytes")
		)
		for {
			if ctx.Err() != nil {
				r.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
				return ctx.Err()
			}
			requestChan := make(chan struct{}, r.maxConcurrentRequests.Load())
			loopStart := time.Now()
			currentMin := time.Now().UTC().Unix() / 60

			getReportsStart := time.Now()
			reports, reportedAt, err := r.getReports(currentMin, c.ConnInfo)
			if err != nil {
				r.log.Errorw("getting reports", "error", err)
				select {
				case <-ctx.Done():
					r.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
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
					r.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
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
					err := r.sendMetric(errCtx, netClient, c.Label, metricToSend)
					<-requestChan
					return err
				})
			}

			err = errGroup.Wait()
			if err != nil {
				r.log.Errorf(`[ Reporting ]: Error sending metrics to service: %v`, err)
			} else {
				dbHandle, err := r.getDBHandle(c.ConnInfo)
				if err != nil {
					return err
				}
				_, err = dbHandle.Exec(`DELETE FROM `+ReportsTable+` WHERE reported_at = $1`, reportedAt)
				if err != nil {
					r.log.Errorf(`[ Reporting ]: Error deleting local reports from %s: %v`, ReportsTable, err)
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
							logger.NewErrorField(err),
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

func (r *DefaultReporter) sendMetric(ctx context.Context, netClient *http.Client, label string, metric *types.Metric) error {
	payload, err := json.Marshal(metric)
	if err != nil {
		panic(err)
	}
	operation := func() error {
		uri := fmt.Sprintf("%s/metrics?version=v1", r.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		if r.region != "" {
			q := req.URL.Query()
			q.Add("region", r.region)
			req.URL.RawQuery = q.Encode()
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		httpRequestStart := time.Now()
		resp, err := netClient.Do(req)
		if err != nil {
			r.log.Error(err.Error())
			return err
		}

		r.requestLatency.Since(httpRequestStart)
		httpStatTags := r.getTags(label)
		httpStatTags["status"] = strconv.Itoa(resp.StatusCode)
		r.stats.NewTaggedStat(StatReportingHttpReq, stats.CountType, httpStatTags).Count(1)

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			r.log.Error(err.Error())
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		r.log.Errorf(`[ Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		r.log.Errorf(`[ Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

func isMetricPosted(status int) bool {
	return status >= 200 && status < 300
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

func (r *DefaultReporter) transformMetricWithEventSampling(metric types.PUReportedMetric) types.PUReportedMetric {
	if r.eventSampler == nil {
		return metric
	}

	isValidSampleEvent := metric.StatusDetail.SampleEvent != nil && string(metric.StatusDetail.SampleEvent) != "{}"

	if isValidSampleEvent {
		hash := NewLabelSet(metric).generateHash()
		found, err := r.eventSampler.Get([]byte(hash))
		if err != nil {
			panic(err)
		}

		if found {
			metric.StatusDetail.SampleEvent = json.RawMessage(`{}`)
			metric.StatusDetail.SampleResponse = ""
		} else {
			err := r.eventSampler.Put([]byte(hash))
			if err != nil {
				panic(err)
			}
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

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := r.configSubscriber.WorkspaceIDFromSource(metric.ConnectionDetails.SourceID)
		metric := *metric

		if metric.ConnectionDetails.SourceCategory == "warehouse" || slices.Contains(r.sourcesWithEventNameTrackingDisabled, metric.ConnectionDetails.SourceID) {
			metric.StatusDetail.EventName = metric.StatusDetail.EventType
		}

		if r.configSubscriber.IsPIIReportingDisabled(workspaceID) {
			metric = transformMetricForPII(metric, getPIIColumnsToExclude())
		}

		if r.eventSamplingEnabled.Load() {
			metric = r.transformMetricWithEventSampling(metric)
		}

		runeEventName := []rune(metric.StatusDetail.EventName)
		if len(runeEventName) > 50 {
			metric.StatusDetail.EventName = fmt.Sprintf("%s...%s", string(runeEventName[:40]), string(runeEventName[len(runeEventName)-10:]))
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

	if r.eventSampler != nil {
		r.eventSampler.Close()
	}
}

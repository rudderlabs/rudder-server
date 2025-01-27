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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lib/pq"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/event_sampler"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	ErrorDetailReportsTable = "error_detail_reports"
	groupKeyDelimitter      = "$::$"
)

var ErrorDetailReportsColumns = []string{
	"workspace_id",
	"namespace",
	"instance_id",
	"source_definition_id",
	"source_id",
	"destination_definition_id",
	"destination_id",
	"dest_type",
	"pu", // reportedBy
	"reported_at",
	"count",
	"status_code",
	"event_type",
	"error_code",
	"error_message",
	"sample_response",
	"sample_event",
	"event_name",
}

type ErrorDetailReporter struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	g                   *errgroup.Group
	configSubscriber    *configSubscriber
	reportingServiceURL string
	syncersMu           sync.RWMutex
	syncers             map[string]*types.SyncSource
	log                 logger.Logger
	namespace           string

	instanceID            string
	region                string
	sleepInterval         config.ValueLoader[time.Duration]
	mainLoopSleepInterval config.ValueLoader[time.Duration]
	maxConcurrentRequests config.ValueLoader[int]
	maxOpenConnections    int
	vacuumFull            config.ValueLoader[bool]

	httpClient *http.Client

	errorDetailExtractor *ExtractorHandle

	minReportedAtQueryTime      stats.Measurement
	errorDetailReportsQueryTime stats.Measurement
	edReportingRequestLatency   stats.Measurement
	eventSamplingEnabled        config.ValueLoader[bool]
	eventSamplingDuration       config.ValueLoader[time.Duration]
	eventSampler                event_sampler.EventSampler

	stats  stats.Stats
	config *config.Config
}

func NewErrorDetailReporter(
	ctx context.Context,
	configSubscriber *configSubscriber,
	stats stats.Stats,
	conf *config.Config,
) *ErrorDetailReporter {
	tr := &http.Transport{}
	reportingServiceURL := conf.GetString("REPORTING_URL", "https://reporting.dev.rudderlabs.com")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")

	netClient := &http.Client{Transport: tr, Timeout: conf.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	mainLoopSleepInterval := conf.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := conf.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := conf.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := conf.GetIntVar(16, 1, "Reporting.errorReporting.maxOpenConnections")
	eventSamplingEnabled := conf.GetReloadableBoolVar(false, "Reporting.errorReporting.eventSampling.enabled")
	eventSamplingDuration := conf.GetReloadableDurationVar(60, time.Minute, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("badger", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(100000, 1, "Reporting.eventSampling.cardinality")

	log := logger.NewLogger().Child("enterprise").Child("error-detail-reporting")
	extractor := NewErrorDetailExtractor(log)
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	var eventSampler event_sampler.EventSampler

	if eventSamplingEnabled.Load() {
		var err error
		eventSampler, err = event_sampler.NewEventSampler(ctx, eventSamplingDuration, eventSamplerType, eventSamplingCardinality, event_sampler.ErrorsReporting, conf, log, stats)
		if err != nil {
			panic(err)
		}
	}

	return &ErrorDetailReporter{
		ctx:                   ctx,
		cancel:                cancel,
		g:                     g,
		reportingServiceURL:   reportingServiceURL,
		log:                   log,
		sleepInterval:         sleepInterval,
		mainLoopSleepInterval: mainLoopSleepInterval,
		maxConcurrentRequests: maxConcurrentRequests,
		vacuumFull:            conf.GetReloadableBoolVar(true, "Reporting.errorReporting.vacuumFull", "Reporting.vacuumFull"),
		httpClient:            netClient,

		eventSamplingEnabled:  eventSamplingEnabled,
		eventSamplingDuration: eventSamplingDuration,
		eventSampler:          eventSampler,

		namespace:  config.GetKubeNamespace(),
		instanceID: conf.GetString("INSTANCE_ID", "1"),
		region:     conf.GetString("region", ""),

		configSubscriber:     configSubscriber,
		syncers:              make(map[string]*types.SyncSource),
		errorDetailExtractor: extractor,
		maxOpenConnections:   maxOpenConnections,
		stats:                stats,
		config:               conf,
	}
}

func (edr *ErrorDetailReporter) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	if c.Label == "" {
		c.Label = types.CoreReportingLabel
	}

	edr.syncersMu.Lock()
	defer edr.syncersMu.Unlock()
	if _, ok := edr.syncers[c.ConnInfo]; ok {
		return func() {} // returning a no-op syncer since another go routine has already started syncing
	}
	dbHandle, err := edr.migrate(c)
	if err != nil {
		panic(fmt.Errorf("failed during migration: %v", err))
	}
	edr.syncers[c.ConnInfo] = &types.SyncSource{SyncerConfig: c, DbHandle: dbHandle}

	if !edr.config.GetBool("Reporting.errorReporting.syncer.enabled", true) {
		return func() {}
	}
	if edr.config.GetBool("Reporting.errorReporting.vacuumAtStartup", false) {
		if _, err := dbHandle.ExecContext(
			context.Background(),
			fmt.Sprintf("vacuum full analyze %s", pq.QuoteIdentifier(ErrorDetailReportsTable)),
		); err != nil {
			edr.log.Errorn("error full vacuuming", logger.NewStringField("table", ErrorDetailReportsTable), obskit.Error(err))
			panic(err)
		}
	}

	return func() {
		edr.g.Go(func() error {
			edr.mainLoop(edr.ctx, c)
			return nil
		})
	}
}

func (edr *ErrorDetailReporter) emitLagMetric(ctx context.Context, c types.SyncerConfig, lastReportedAtTime *atomic.Time) error {
	// for monitoring reports pileups
	reportingLag := edr.stats.NewTaggedStat(
		"error_detail_reports_metrics_lag_seconds", stats.GaugeType, stats.Tags{"client": c.Label},
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

func (edr *ErrorDetailReporter) GetSyncer(syncerKey string) *types.SyncSource {
	edr.syncersMu.RLock()
	defer edr.syncersMu.RUnlock()
	return edr.syncers[syncerKey]
}

func shouldReport(metric types.PUReportedMetric) bool {
	switch {
	case metric.StatusDetail.StatusCode >= http.StatusBadRequest, metric.StatusDetail.StatusCode == types.FilterEventCode, metric.StatusDetail.StatusCode == types.SuppressEventCode:
		return true
	default:
		return false
	}
}

func (edr *ErrorDetailReporter) Report(ctx context.Context, metrics []*types.PUReportedMetric, txn *Tx) error {
	edr.log.Debug("[ErrorDetailReport] Report method called\n")
	if len(metrics) == 0 {
		return nil
	}

	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(ErrorDetailReportsTable, ErrorDetailReportsColumns...))
	if err != nil {
		edr.log.Errorf("Failed during statement preparation: %v", err)
		return fmt.Errorf("preparing statement: %v", err)
	}
	defer func() { _ = stmt.Close() }()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		metric := *metric
		if !shouldReport(metric) {
			continue
		}

		workspaceID := edr.configSubscriber.WorkspaceIDFromSource(metric.ConnectionDetails.SourceID)
		if edr.IsPIIReportingDisabled(workspaceID) {
			edr.log.Debugn("PII setting is disabled for workspaceId:", obskit.WorkspaceID(workspaceID))
			return nil
		}
		destinationDetail := edr.configSubscriber.GetDestDetail(metric.ConnectionDetails.DestinationID)
		edr.log.Debugn("DestinationId & DestDetail details", obskit.DestinationID(metric.ConnectionDetails.DestinationID), logger.NewField("destinationDetail", destinationDetail))

		// extract error-message & error-code
		metric.StatusDetail.ErrorDetails = edr.extractErrorDetails(metric.StatusDetail.SampleResponse, metric.StatusDetail.StatTags)

		edr.stats.NewTaggedStat("error_detail_reporting_failures", stats.CountType, stats.Tags{
			"errorCode":     metric.StatusDetail.ErrorDetails.Code,
			"workspaceId":   workspaceID,
			"destType":      destinationDetail.destType,
			"sourceId":      metric.ConnectionDetails.SourceID,
			"destinationId": metric.ConnectionDetails.DestinationID,
		}).Count(int(metric.StatusDetail.Count))

		sampleEvent, sampleResponse, err := getSampleWithEventSampling(metric, reportedAt, edr.eventSampler, edr.eventSamplingEnabled.Load(), int64(edr.eventSamplingDuration.Load().Minutes()))
		if err != nil {
			return err
		}

		_, err = stmt.Exec(
			workspaceID,
			edr.namespace,
			edr.instanceID,
			metric.ConnectionDetails.SourceDefinitionID,
			metric.ConnectionDetails.SourceID,
			destinationDetail.destinationDefinitionID,
			metric.ConnectionDetails.DestinationID,
			destinationDetail.destType,
			metric.PUDetails.PU,
			reportedAt,
			metric.StatusDetail.Count,
			metric.StatusDetail.StatusCode,
			metric.StatusDetail.EventType,
			metric.StatusDetail.ErrorDetails.Code,
			metric.StatusDetail.ErrorDetails.Message,
			sampleResponse,
			string(sampleEvent),
			metric.StatusDetail.EventName,
		)
		if err != nil {
			edr.log.Errorf("Failed during statement execution(each metric): %v", err)
			return fmt.Errorf("executing statement: %v", err)
		}
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		edr.log.Errorf("Failed during statement execution: %v", err)
		return fmt.Errorf("executing final statement: %v", err)
	}

	return nil
}

func (ed *ErrorDetailReporter) IsPIIReportingDisabled(workspaceID string) bool {
	return ed.configSubscriber.IsPIIReportingDisabled(workspaceID)
}

func (edr *ErrorDetailReporter) migrate(c types.SyncerConfig) (*sql.DB, error) {
	dbHandle, err := sql.Open("postgres", c.ConnInfo)
	if err != nil {
		return nil, err
	}
	dbHandle.SetMaxOpenConns(edr.maxOpenConnections)
	err = edr.stats.RegisterCollector(collectors.NewDatabaseSQLStats("error_detail_reporting", dbHandle))
	if err != nil {
		edr.log.Errorn("error registering database sql stats", obskit.Error(err))
	}

	m := &migrator.Migrator{
		Handle:          dbHandle,
		MigrationsTable: fmt.Sprintf("%v_migrations", ErrorDetailReportsTable),
		// TODO: shall we use separate env ?
		ShouldForceSetLowerVersion: edr.config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate(ErrorDetailReportsTable)
	if err != nil {
		return nil, fmt.Errorf("could not run %v migrations: %w", ErrorDetailReportsTable, err)
	}
	return dbHandle, nil
}

func (edr *ErrorDetailReporter) extractErrorDetails(sampleResponse string, statTags map[string]string) types.ErrorDetails {
	errMsg := edr.errorDetailExtractor.GetErrorMessage(sampleResponse)
	cleanedErrMsg := edr.errorDetailExtractor.CleanUpErrorMessage(errMsg)
	errorCode := edr.errorDetailExtractor.GetErrorCode(cleanedErrMsg, statTags)
	return types.ErrorDetails{
		Message: cleanedErrMsg,
		Code:    errorCode,
	}
}

func (edr *ErrorDetailReporter) getDBHandle(syncerKey string) (*sql.DB, error) {
	syncer := edr.GetSyncer(syncerKey)
	if syncer != nil {
		return syncer.DbHandle, nil
	}
	return nil, fmt.Errorf("DBHandle not found for syncer name: %s", syncerKey)
}

func (edr *ErrorDetailReporter) getTags(label string) stats.Tags {
	return stats.Tags{
		"workspaceID": edr.configSubscriber.WorkspaceID(),
		"clientName":  label,
		"instanceId":  edr.instanceID,
	}
}

// Sending metrics to Reporting service --- STARTS
func (edr *ErrorDetailReporter) mainLoop(ctx context.Context, c types.SyncerConfig) {
	edr.configSubscriber.Wait()

	tags := edr.getTags(c.Label)

	mainLoopTimer := edr.stats.NewTaggedStat("error_detail_reports_main_loop_time", stats.TimerType, tags)
	getReportsTimer := edr.stats.NewTaggedStat("error_detail_reports_get_reports_time", stats.TimerType, tags)
	aggregateTimer := edr.stats.NewTaggedStat("error_detail_reports_aggregate_time", stats.TimerType, tags)
	getReportsSize := edr.stats.NewTaggedStat("error_detail_reports_size", stats.HistogramType, tags)
	getAggregatedReportsSize := edr.stats.NewTaggedStat("error_detail_reports_aggregated_size", stats.HistogramType, tags)

	errorDetailReportsDeleteQueryTimer := edr.stats.NewTaggedStat("error_detail_reports_delete_query_time", stats.TimerType, tags)

	edr.minReportedAtQueryTime = edr.stats.NewTaggedStat("error_detail_reports_min_reported_at_query_time", stats.TimerType, tags)
	edr.errorDetailReportsQueryTime = edr.stats.NewTaggedStat("error_detail_reports_query_time", stats.TimerType, tags)
	edr.edReportingRequestLatency = edr.stats.NewTaggedStat("error_detail_reporting_request_latency", stats.TimerType, tags)

	var lastReportedAtTime atomic.Time
	lastReportedAtTime.Store(time.Now())

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return edr.emitLagMetric(ctx, c, &lastReportedAtTime)
	})

	g.Go(func() error {
		// In infinite loop
		// Get Reports
		// Aggregate
		// Send in a separate go-routine
		// Delete in a separate go-routine
		var (
			deletedRows               int
			vacuumDeletedRowThreshold = edr.config.GetReloadableIntVar(
				100000, 1,
				"Reporting.errorReporting.vacuumThresholdDeletedRows",
				"Reporting.vacuumThresholdDeletedRows",
			)
			lastVacuum     time.Time
			vacuumInterval = edr.config.GetReloadableDurationVar(
				15,
				time.Minute,
				"Reporting.errorReporting.vacuumInterval",
				"Reporting.vacuumInterval",
			)
			vacuumThresholdBytes = config.GetReloadableInt64Var(
				10*bytesize.GB, 1,
				"Reporting.errorReporting.vacuumThresholdBytes",
				"Reporting.vacuumThresholdBytes",
			)
		)
		for {
			if ctx.Err() != nil {
				edr.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
				return ctx.Err()
			}
			requestChan := make(chan struct{}, edr.maxConcurrentRequests.Load())
			loopStart := time.Now()
			currentMs := time.Now().UTC().Unix() / 60

			getReportsStart := time.Now()
			reports, reportedAt := edr.getReports(ctx, currentMs, c.ConnInfo)
			if ctx.Err() != nil {
				edr.log.Errorw("getting reports", "error", ctx.Err())
				select {
				case <-ctx.Done():
					edr.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
					return ctx.Err()
				case <-time.After(edr.mainLoopSleepInterval.Load()):
				}
				continue
			}
			getReportsTimer.Since(getReportsStart)
			getReportsSize.Observe(float64(len(reports)))

			if len(reports) == 0 {
				lastReportedAtTime.Store(loopStart)
				select {
				case <-ctx.Done():
					edr.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
					return ctx.Err()
				case <-time.After(edr.sleepInterval.Load()):
				}
				continue
			}
			lastReportedAtTime.Store(time.Unix(reportedAt*60, 0))

			aggregationStart := time.Now()
			metrics := edr.aggregate(reports)
			aggregateTimer.Since(aggregationStart)
			getAggregatedReportsSize.Observe(float64(len(metrics)))

			errGroup, errCtx := errgroup.WithContext(ctx)
			for _, metric := range metrics {
				metricToSend := metric
				requestChan <- struct{}{}
				if errCtx.Err() != nil {
					// if any of errGroup's goroutines fail - don't send anymore requests for this batch
					break
				}
				errGroup.Go(func() error {
					err := edr.sendMetric(errCtx, c.Label, metricToSend)
					if err != nil {
						edr.log.Error("Error while sending to Reporting service:", err)
					}
					<-requestChan
					return err
				})
			}

			err := errGroup.Wait()
			if err == nil {
				// sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_at = %d`, ErrorDetailReportsTable, reportedAt)
				dbHandle, err := edr.getDBHandle(c.ConnInfo)
				if err != nil {
					edr.log.Errorf("error reports deletion getDbhandle failed: %v", err)
					continue
				}
				deleteReportsStart := time.Now()
				_, err = dbHandle.ExecContext(ctx, `DELETE FROM `+ErrorDetailReportsTable+` WHERE reported_at = $1`, reportedAt)
				errorDetailReportsDeleteQueryTimer.Since(deleteReportsStart)
				if err != nil {
					edr.log.Errorf("[ Error Detail Reporting ]: Error deleting local reports from %s: %v", ErrorDetailReportsTable, err)
				} else {
					deletedRows += len(reports)
				}
				// vacuum error_reports_details table
				if deletedRows >= vacuumDeletedRowThreshold.Load() {
					if err := edr.vacuum(ctx, dbHandle, tags); err == nil {
						deletedRows = 0
						lastVacuum = time.Now()
					}
				} else if time.Since(lastVacuum) >= vacuumInterval.Load() {
					var sizeEstimate int64
					if err := dbHandle.QueryRowContext(
						ctx,
						`SELECT pg_table_size(oid) from pg_class where relname = $1`, ErrorDetailReportsTable,
					).Scan(&sizeEstimate); err != nil {
						edr.log.Errorn(
							fmt.Sprintf(`Error getting %s table size estimate`, ErrorDetailReportsTable),
							logger.NewErrorField(err),
						)
					}
					if sizeEstimate >= vacuumThresholdBytes.Load() {
						if err := edr.vacuum(ctx, dbHandle, tags); err == nil {
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
			case <-time.After(edr.mainLoopSleepInterval.Load()):
			}
		}
	})
	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}

func (edr *ErrorDetailReporter) vacuum(ctx context.Context, dbHandle *sql.DB, tags stats.Tags) error {
	defer edr.stats.NewTaggedStat(StatReportingVacuumDuration, stats.TimerType, tags).RecordDuration()()
	var query string
	var full bool
	if edr.vacuumFull.Load() {
		query = fmt.Sprintf("vacuum full analyze %s", pq.QuoteIdentifier(ErrorDetailReportsTable))
		full = true
	} else {
		query = fmt.Sprintf("vacuum analyze %s", pq.QuoteIdentifier(ErrorDetailReportsTable))
	}
	_, err := dbHandle.ExecContext(ctx, query)
	if err != nil {
		edr.log.Errorn(
			"error vacuuming",
			logger.NewStringField("table", ErrorDetailReportsTable),
			obskit.Error(err),
			logger.NewBoolField("full", full),
		)
		return err
	}

	return nil
}

func (edr *ErrorDetailReporter) getReports(ctx context.Context, currentMs int64, syncerKey string) ([]*types.EDReportsDB, int64) {
	var queryMin sql.NullInt64
	dbHandle, err := edr.getDBHandle(syncerKey)
	if err != nil {
		edr.log.Errorf("Failed while getting DbHandle: %v", err)
		return []*types.EDReportsDB{}, queryMin.Int64
	}

	queryStart := time.Now()
	err = dbHandle.QueryRowContext(ctx, "SELECT reported_at FROM "+ErrorDetailReportsTable+" WHERE reported_at < $1 ORDER BY reported_at ASC LIMIT 1", currentMs).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		edr.log.Errorf("Failed while getting reported_at: %v", err)
		return []*types.EDReportsDB{}, queryMin.Int64
	}
	edr.minReportedAtQueryTime.Since(queryStart)
	if !queryMin.Valid {
		return nil, 0
	}
	edSelColumns := strings.Join([]string{
		"workspace_id",
		"namespace",
		"instance_id",
		"source_definition_id",
		"source_id",
		"destination_definition_id",
		"destination_id",
		"pu",
		"reported_at",
		"count",
		"status_code",
		"event_type",
		"error_code",
		"error_message",
		"dest_type",
		"sample_response",
		"sample_event",
		"event_name",
	}, ", ")
	var rows *sql.Rows
	queryStart = time.Now()
	rows, err = dbHandle.Query(`SELECT `+edSelColumns+` FROM `+ErrorDetailReportsTable+` WHERE reported_at = $1`, queryMin.Int64)
	if err != nil {
		edr.log.Errorf("Failed while getting reports(reported_at=%v): %v", queryMin.Int64, err)
		return []*types.EDReportsDB{}, queryMin.Int64
	}

	edr.errorDetailReportsQueryTime.Since(queryStart)
	defer func() { _ = rows.Close() }()
	var metrics []*types.EDReportsDB
	for rows.Next() {
		/*
			"workspace_id",
			"namespace",
			"instance_id",
			"source_definition_id",
			"source_id",
			"destination_definition_id",
			"destination_id",
			"pu",
			"reported_at",
			"count",
			"status_code",
			"event_type",
			"error_code",
			"error_message",
			"dest_type",
			"sample_response",
			"sample_event",
			"event_name",
		*/
		dbEdMetric := &types.EDReportsDB{
			EDErrorDetails:    types.EDErrorDetails{},
			EDInstanceDetails: types.EDInstanceDetails{},
		}
		err = rows.Scan(
			&dbEdMetric.EDInstanceDetails.WorkspaceID,
			&dbEdMetric.EDInstanceDetails.Namespace,
			&dbEdMetric.EDInstanceDetails.InstanceID,
			&dbEdMetric.EDConnectionDetails.SourceDefinitionId,
			&dbEdMetric.EDConnectionDetails.SourceID,
			&dbEdMetric.EDConnectionDetails.DestinationDefinitionId,
			&dbEdMetric.EDConnectionDetails.DestinationID,
			&dbEdMetric.PU,
			&dbEdMetric.ReportMetadata.ReportedAt,
			&dbEdMetric.Count,
			&dbEdMetric.EDErrorDetails.EDErrorDetailsKey.StatusCode,
			&dbEdMetric.EDErrorDetails.EDErrorDetailsKey.EventType,
			&dbEdMetric.EDErrorDetails.EDErrorDetailsKey.ErrorCode,
			&dbEdMetric.EDErrorDetails.EDErrorDetailsKey.ErrorMessage,
			&dbEdMetric.EDConnectionDetails.DestType,
			&dbEdMetric.EDErrorDetails.SampleResponse,
			&dbEdMetric.EDErrorDetails.SampleEvent,
			&dbEdMetric.EDErrorDetails.EDErrorDetailsKey.EventName,
		)
		if err != nil {
			edr.log.Errorf("Failed while scanning rows(reported_at=%v): %v", queryMin.Int64, err)
			return []*types.EDReportsDB{}, queryMin.Int64
		}
		metrics = append(metrics, dbEdMetric)
	}
	if rows.Err() != nil {
		edr.log.Errorf("Rows error while querying: %v", rows.Err())
		return []*types.EDReportsDB{}, queryMin.Int64
	}
	return metrics, queryMin.Int64
}

func (edr *ErrorDetailReporter) aggregate(reports []*types.EDReportsDB) []*types.EDMetric {
	groupedReports := lo.GroupBy(reports, func(report *types.EDReportsDB) string {
		keys := []string{
			report.WorkspaceID,
			report.Namespace,
			report.SourceDefinitionId,
			report.SourceID,
			report.DestinationID,
			report.DestinationDefinitionId,
			report.PU,
			fmt.Sprint(report.ReportedAt),
			report.DestType,
		}
		return strings.Join(keys, groupKeyDelimitter)
	})
	var edrortingMetrics []*types.EDMetric
	groupKeys := lo.Keys(groupedReports)
	sort.Strings(groupKeys)

	for _, key := range groupKeys {
		reports := groupedReports[key]
		firstReport := reports[0]
		edrSchema := types.EDMetric{
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: firstReport.WorkspaceID,
				Namespace:   firstReport.Namespace,
				InstanceID:  firstReport.InstanceID,
			},
			EDConnectionDetails: types.EDConnectionDetails{
				DestinationID:           firstReport.DestinationID,
				DestinationDefinitionId: firstReport.DestinationDefinitionId,
				SourceID:                firstReport.SourceID,
				SourceDefinitionId:      firstReport.SourceDefinitionId,
				DestType:                firstReport.DestType,
			},
			PU: firstReport.PU,
			ReportMetadata: types.ReportMetadata{
				ReportedAt: firstReport.ReportedAt * 60 * 1000,
			},
		}
		messageMap := make(map[string]int)
		reportsCountMap := make(map[types.EDErrorDetailsKey]*types.EDReportMapValue)
		for index, rep := range reports {
			messageMap[rep.EDErrorDetails.ErrorMessage] = index
			errDet := rep.EDErrorDetails.EDErrorDetailsKey
			reportMapValue, ok := reportsCountMap[errDet]
			if !ok {
				reportsCountMap[errDet] = &types.EDReportMapValue{
					SampleResponse: rep.SampleResponse,
					SampleEvent:    rep.SampleEvent,
					Count:          rep.Count,
				}
				continue
			}
			reportMapValue.Count += rep.Count
		}

		reportGrpKeys := lo.Keys(reportsCountMap)
		sort.SliceStable(reportGrpKeys, func(i, j int) bool {
			irep := reportGrpKeys[i]
			jrep := reportGrpKeys[j]

			return (irep.StatusCode < jrep.StatusCode ||
				irep.ErrorMessage < jrep.ErrorMessage ||
				irep.EventType < jrep.EventType)
		})
		errs := make([]types.EDErrorDetails, len(reportGrpKeys))
		for i, repKey := range reportGrpKeys {
			repValue := reportsCountMap[repKey]
			errs[i] = types.EDErrorDetails{
				EDErrorDetailsKey: repKey,
				SampleResponse:    repValue.SampleResponse,
				SampleEvent:       repValue.SampleEvent,
				ErrorCount:        repValue.Count,
			}
		}
		edrSchema.Errors = errs
		edrortingMetrics = append(edrortingMetrics, &edrSchema)
	}
	return edrortingMetrics
}

func (edr *ErrorDetailReporter) sendMetric(ctx context.Context, label string, metric *types.EDMetric) error {
	payload, err := json.Marshal(metric)
	if err != nil {
		return fmt.Errorf("marshal failure: %w", err)
	}
	operation := func() error {
		uri := fmt.Sprintf("%s/recordErrors", edr.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		if edr.region != "" {
			q := req.URL.Query()
			q.Add("region", edr.region)
			req.URL.RawQuery = q.Encode()
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		httpRequestStart := time.Now()
		resp, err := edr.httpClient.Do(req)
		if err != nil {
			edr.log.Errorf("Sending request failed: %v", err)
			return err
		}

		edr.edReportingRequestLatency.Since(httpRequestStart)
		httpStatTags := edr.getTags(label)
		httpStatTags["status"] = strconv.Itoa(resp.StatusCode)
		edr.stats.NewTaggedStat("error_detail_reporting_http_request", stats.CountType, httpStatTags).Increment()

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		edr.log.Debugf("[ErrorDetailReporting]Response from ReportingAPI: %v\n", string(respBody))
		if err != nil {
			edr.log.Errorf("Reading response failed: %w", err)
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode: %d error: %v`, resp.StatusCode, string(respBody))
			edr.log.Error(err.Error())
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		edr.log.Errorf(`[ Error Detail Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		edr.log.Errorf(`[ Error Detail Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

func (edr *ErrorDetailReporter) Stop() {
	edr.cancel()
	_ = edr.g.Wait()
	if edr.eventSampler != nil {
		edr.eventSampler.Close()
	}
}

package reporting

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
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

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
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

// ErrorReportingStats manages all stats for error reporting
type ErrorReportingStats struct {
	// Basic stats
	ReportTime                   stats.Measurement
	ReportingLag                 stats.Measurement
	ErrorDetailReportingFailures stats.Measurement
	HttpRequest                  stats.Measurement
	VacuumDuration               stats.Measurement

	// Error normalizer stats
	NormalizerCleanupTime stats.Measurement
}

// NewErrorReportingStats creates a new stats manager
func NewErrorReportingStats(statsInstance stats.Stats) *ErrorReportingStats {
	return &ErrorReportingStats{
		ReportTime:                   statsInstance.NewStat("error_detail_reporter_report_time", stats.TimerType),
		ReportingLag:                 statsInstance.NewStat("error_detail_reports_metrics_lag_seconds", stats.GaugeType),
		ErrorDetailReportingFailures: statsInstance.NewStat("error_detail_reporting_failures", stats.CountType),
		HttpRequest:                  statsInstance.NewStat("error_detail_reporting_http_request", stats.CountType),
		VacuumDuration:               statsInstance.NewStat("reporting_vacuum_duration", stats.TimerType),
		NormalizerCleanupTime:        statsInstance.NewStat("error_detail_reporter_normalizer_cleanup_time", stats.TimerType),
	}
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

	// DEPRECATED: Remove this after migration to commonClient, use edr.commonClient.Send instead.
	httpClient *http.Client

	errorDetailExtractor *ExtractorHandle
	errorNormalizer      ErrorNormalizer

	// Stats management
	statsManager *ErrorReportingStats

	// Tagged stats (created dynamically with tags)
	minReportedAtQueryTime      stats.Measurement
	errorDetailReportsQueryTime stats.Measurement
	edReportingRequestLatency   stats.Measurement
	eventSamplingEnabled        config.ValueLoader[bool]
	eventSamplingDuration       config.ValueLoader[time.Duration]
	eventSampler                event_sampler.EventSampler
	groupingThreshold           config.ValueLoader[float64]

	stats  stats.Stats
	config *config.Config

	useCommonClient config.ValueLoader[bool]
	commonClient    *client.Client
}

func NewErrorDetailReporter(
	ctx context.Context,
	configSubscriber *configSubscriber,
	statsInstance stats.Stats,
	conf *config.Config,
) *ErrorDetailReporter {
	// DEPRECATED: Remove this after migration to commonClient, use edr.commonClient.Send instead.
	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr, Timeout: conf.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	reportingServiceURL := conf.GetString("REPORTING_URL", "https://reporting.dev.rudderlabs.com")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")
	useCommonClient := conf.GetReloadableBoolVar(false, "Reporting.useCommonClient")

	mainLoopSleepInterval := conf.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := conf.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := conf.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := conf.GetIntVar(16, 1, "Reporting.errorReporting.maxOpenConnections")
	eventSamplingEnabled := conf.GetReloadableBoolVar(false, "Reporting.errorReporting.eventSampling.enabled")
	eventSamplingDuration := conf.GetReloadableDurationVar(60, time.Minute, "Reporting.eventSampling.durationInMinutes")
	eventSamplerType := conf.GetReloadableStringVar("badger", "Reporting.eventSampling.type")
	eventSamplingCardinality := conf.GetReloadableIntVar(100000, 1, "Reporting.eventSampling.cardinality")

	log := logger.NewLogger().Child("enterprise").Child("error-detail-reporting")
	extractor := NewErrorDetailExtractor(log, conf)
	statsManager := NewErrorReportingStats(statsInstance)
	errorNormalizer := NewErrorNormalizer(log, conf, statsInstance, statsManager)
	groupingThreshold := conf.GetReloadableFloat64Var(0.75, "Reporting.errorReporting.grouping.similarityThreshold")
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	var eventSampler event_sampler.EventSampler

	if eventSamplingEnabled.Load() {
		var err error
		eventSampler, err = event_sampler.NewEventSampler(ctx, eventSamplingDuration, eventSamplerType, eventSamplingCardinality, event_sampler.ErrorsReporting, conf, log, statsInstance)
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
		groupingThreshold:     groupingThreshold,

		namespace: config.GetKubeNamespace(),

		// Initialize stats manager
		statsManager: statsManager,
		instanceID:   conf.GetString("INSTANCE_ID", "1"),
		region:       conf.GetString("region", ""),

		configSubscriber:     configSubscriber,
		syncers:              make(map[string]*types.SyncSource),
		errorDetailExtractor: extractor,
		errorNormalizer:      errorNormalizer,
		maxOpenConnections:   maxOpenConnections,
		stats:                statsInstance,
		config:               conf,

		useCommonClient: useCommonClient,
		commonClient:    client.New(client.RouteRecordErrors, conf, log, statsInstance),
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

func (edr *ErrorDetailReporter) emitLagMetric(ctx context.Context, lastReportedAtTime *atomic.Time) error {
	// for monitoring reports pileups
	for {
		lag := time.Since(lastReportedAtTime.Load())
		edr.statsManager.ReportingLag.Gauge(lag.Seconds())
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
	start := time.Now()
	defer func() {
		edr.statsManager.ReportTime.Since(start)
	}()

	// Extract error details and filter metrics that should be reported
	reportableMetrics := edr.extractErrorDetailsAndFilterMetrics(metrics)

	// Early exit if no metrics to report
	if len(reportableMetrics) == 0 {
		return nil
	}

	// Group errors by connection details
	connectionGroups := edr.groupByConnection(reportableMetrics)

	// Normalize errors after grouping (modifies connectionGroups in place)
	connectionGroups = edr.normalizeErrors(ctx, connectionGroups)

	// Merge metrics by error messages within the same connection group
	mergedMetricGroups := edr.mergeMetricGroupsByErrorMessage(connectionGroups)

	// Write grouped errors to database
	return edr.writeGroupedErrors(ctx, mergedMetricGroups, txn)
}

func (edr *ErrorDetailReporter) extractErrorDetailsAndFilterMetrics(metrics []*types.PUReportedMetric) []*types.EDReportsDB {
	var reportableMetrics []*types.EDReportsDB

	for _, metric := range metrics {
		if !shouldReport(*metric) {
			continue
		}

		workspaceID := edr.configSubscriber.WorkspaceIDFromSource(metric.SourceID)
		if edr.IsPIIReportingDisabled(workspaceID) {
			continue
		}

		// EXTRACT ERROR DETAILS - This is where error messages are created from SampleResponse
		destinationDetail := edr.configSubscriber.GetDestDetail(metric.DestinationID)
		errorDetails := edr.extractErrorDetails(metric.StatusDetail.SampleResponse, metric.StatusDetail.StatTags, destinationDetail.destType)

		if errorDetails.Message == "" {
			continue
		}

		// Convert to EDReportsDB instead of deep copying
		params := types.ErrorMetricParams{
			WorkspaceID:             workspaceID,
			Namespace:               edr.namespace,
			InstanceID:              edr.instanceID,
			DestType:                destinationDetail.destType,
			DestinationDefinitionID: destinationDetail.destinationDefinitionID,
			ErrorDetails:            errorDetails,
		}
		errorMetric := types.PUReportedMetricToEDReportsDB(metric, params)
		reportableMetrics = append(reportableMetrics, errorMetric)
	}

	return reportableMetrics
}

func (edr *ErrorDetailReporter) normalizeErrors(ctx context.Context, connectionGroups map[types.ErrorDetailGroupKey][]*types.EDReportsDB) map[types.ErrorDetailGroupKey][]*types.EDReportsDB {
	for groupKey, groupMetrics := range connectionGroups {
		// Update all metrics in the group with the normalized error message
		for _, metric := range groupMetrics {
			metric.ErrorMessage = edr.errorNormalizer.NormalizeError(ctx, groupKey, metric.ErrorMessage)
		}
	}
	return connectionGroups
}

func (edr *ErrorDetailReporter) writeGroupedErrors(ctx context.Context, groups map[types.ErrorDetailGroupKey][]*types.EDReportsDB, txn *Tx) error {
	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(ErrorDetailReportsTable, ErrorDetailReportsColumns...))
	if err != nil {
		return fmt.Errorf("preparing statement: %v", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, groupMetrics := range groups {

		// Calculate total count for the group
		var totalCount int64
		// Record stats
		for _, metric := range groupMetrics {
			totalCount += metric.Count

			sampleEvent, sampleResponse, err := getSampleWithEventSamplingForEDReportsDB(*metric, metric.ReportedAt, edr.eventSampler, edr.eventSamplingEnabled.Load(), int64(edr.eventSamplingDuration.Load().Minutes()))
			if err != nil {
				return fmt.Errorf("event sampling error: %v", err)
			}

			_, err = stmt.Exec(
				metric.WorkspaceID,
				metric.Namespace,
				metric.InstanceID,
				metric.SourceDefinitionId,
				metric.SourceID,
				metric.DestinationDefinitionId,
				metric.DestinationID,
				metric.DestType,
				metric.PU,
				metric.ReportedAt,
				metric.Count,
				metric.StatusCode,
				metric.EventType,
				metric.ErrorCode,
				metric.ErrorMessage,
				sampleResponse,
				getStringifiedSampleEvent(sampleEvent),
				metric.EventName,
			)
			if err != nil {
				return fmt.Errorf("executing statement: %v", err)
			}
		}
		edr.statsManager.ErrorDetailReportingFailures.Count(int(totalCount))
	}

	_, err = stmt.ExecContext(ctx)
	return err
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

func (edr *ErrorDetailReporter) extractErrorDetails(sampleResponse string, statTags map[string]string, destType string) types.ErrorDetails {
	errMsg := edr.errorDetailExtractor.GetErrorMessage(sampleResponse)
	cleanedErrMsg := edr.errorDetailExtractor.CleanUpErrorMessage(errMsg)
	errorCode := edr.errorDetailExtractor.GetErrorCode(cleanedErrMsg, statTags, destType)
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
		return edr.errorNormalizer.StartCleanup(edr.ctx)
	})

	g.Go(func() error {
		return edr.emitLagMetric(ctx, &lastReportedAtTime)
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
				edr.log.Infon("stopping mainLoop for syncer", logger.NewStringField("label", c.Label), obskit.Error(ctx.Err()))
				return ctx.Err()
			}
			requestChan := make(chan struct{}, edr.maxConcurrentRequests.Load())
			loopStart := time.Now()
			currentMs := time.Now().UTC().Unix() / 60

			getReportsStart := time.Now()
			reports, reportedAt := edr.getReports(ctx, currentMs, c.ConnInfo)
			if ctx.Err() != nil {
				edr.log.Errorn("getting reports", obskit.Error(ctx.Err()))
				select {
				case <-ctx.Done():
					edr.log.Infon("stopping mainLoop for syncer", logger.NewStringField("label", c.Label), obskit.Error(ctx.Err()))
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
					edr.log.Infon("stopping mainLoop for syncer", logger.NewStringField("label", c.Label), obskit.Error(ctx.Err()))
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
						edr.log.Errorn("Error while sending to Reporting service", obskit.Error(err))
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
					edr.log.Errorn("error reports deletion getDbhandle failed", obskit.Error(err))
					continue
				}
				deleteReportsStart := time.Now()
				_, err = dbHandle.ExecContext(ctx, `DELETE FROM `+ErrorDetailReportsTable+` WHERE reported_at = $1`, reportedAt)
				errorDetailReportsDeleteQueryTimer.Since(deleteReportsStart)
				if err != nil {
					edr.log.Errorn("[ Error Detail Reporting ]: Error deleting local reports from table", logger.NewStringField("table", ErrorDetailReportsTable), obskit.Error(err))
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
							"Error getting table size estimate",
							logger.NewStringField("table", ErrorDetailReportsTable),
							obskit.Error(err),
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

func (edr *ErrorDetailReporter) vacuum(ctx context.Context, dbHandle *sql.DB, _ stats.Tags) error {
	defer edr.statsManager.VacuumDuration.RecordDuration()()
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
		edr.log.Errorn("Failed while getting DbHandle", obskit.Error(err))
		return []*types.EDReportsDB{}, queryMin.Int64
	}

	queryStart := time.Now()
	err = dbHandle.QueryRowContext(ctx, "SELECT reported_at FROM "+ErrorDetailReportsTable+" WHERE reported_at < $1 ORDER BY reported_at ASC LIMIT 1", currentMs).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		edr.log.Errorn("Failed while getting reported_at", obskit.Error(err))
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
		edr.log.Errorn("Failed while getting reports", logger.NewIntField("reported_at", queryMin.Int64), obskit.Error(err))
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
		var sampleEvent string
		err = rows.Scan(
			&dbEdMetric.WorkspaceID,
			&dbEdMetric.Namespace,
			&dbEdMetric.InstanceID,
			&dbEdMetric.SourceDefinitionId,
			&dbEdMetric.SourceID,
			&dbEdMetric.DestinationDefinitionId,
			&dbEdMetric.DestinationID,
			&dbEdMetric.PU,
			&dbEdMetric.ReportedAt,
			&dbEdMetric.Count,
			&dbEdMetric.StatusCode,
			&dbEdMetric.EventType,
			&dbEdMetric.ErrorCode,
			&dbEdMetric.ErrorMessage,
			&dbEdMetric.DestType,
			&dbEdMetric.SampleResponse,
			&sampleEvent,
			&dbEdMetric.EventName,
		)
		if err != nil {
			edr.log.Errorn("Failed while scanning rows", logger.NewIntField("reported_at", queryMin.Int64), obskit.Error(err))
			return []*types.EDReportsDB{}, queryMin.Int64
		}
		dbEdMetric.SampleEvent = []byte(sampleEvent)
		metrics = append(metrics, dbEdMetric)
	}
	if rows.Err() != nil {
		edr.log.Errorn("Rows error while querying", obskit.Error(rows.Err()))
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
			messageMap[rep.ErrorMessage] = index
			errDet := rep.EDErrorDetailsKey
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

// DEPRECATED: Remove this after migration to commonClient, use edr.commonClient.Send instead.
func (edr *ErrorDetailReporter) sendMetric(ctx context.Context, _ string, metric *types.EDMetric) error {
	if edr.useCommonClient.Load() {
		return edr.commonClient.Send(ctx, metric)
	}

	payload, err := jsonrs.Marshal(metric)
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
			edr.log.Errorn("Sending request failed", obskit.Error(err))
			return err
		}

		edr.edReportingRequestLatency.Since(httpRequestStart)
		edr.statsManager.HttpRequest.Increment()

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		edr.log.Debugn("[ErrorDetailReporting]Response from ReportingAPI", logger.NewStringField("response", string(respBody)))
		if err != nil {
			edr.log.Errorn("Reading response failed", obskit.Error(err))
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode: %d error: %v`, resp.StatusCode, string(respBody))
			edr.log.Errorn("received response",
				logger.NewIntField("statusCode", int64(resp.StatusCode)),
				obskit.Error(errors.New(string(respBody))))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		edr.log.Errorn("[ Error Detail Reporting ]: Error reporting to service", obskit.Error(err))
	})
	if err != nil {
		edr.log.Errorn("[ Error Detail Reporting ]: Error making request to reporting service", obskit.Error(err))
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

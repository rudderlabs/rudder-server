package reporting

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
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
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
}

type destDetail struct {
	DestinationDefinitionID string
	DestType                string // destination definition name
}

type ErrorDetailReporter struct {
	ctx                 context.Context
	onceInit            sync.Once
	init                chan struct{}
	reportingServiceURL string
	syncersMu           sync.RWMutex
	syncers             map[string]*types.SyncSource
	log                 logger.Logger
	namespace           string

	instanceID            string
	region                string
	sleepInterval         misc.ValueLoader[time.Duration]
	mainLoopSleepInterval misc.ValueLoader[time.Duration]
	maxConcurrentRequests misc.ValueLoader[int]
	maxOpenConnections    int

	httpClient *http.Client

	backendConfigMu           sync.RWMutex // protects the following
	workspaceID               string
	workspaceIDForSourceIDMap map[string]string
	destinationIDMap          map[string]destDetail

	errorDetailExtractor *ExtractorHandle

	minReportedAtQueryTime      stats.Measurement
	errorDetailReportsQueryTime stats.Measurement
	edReportingRequestLatency   stats.Measurement
}

type errorDetails struct {
	ErrorCode    string
	ErrorMessage string
}

func NewErrorDetailReporter(ctx context.Context) *ErrorDetailReporter {
	tr := &http.Transport{}
	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.dev.rudderlabs.com")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")

	netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := config.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := config.GetIntVar(16, 1, "Reporting.errorReporting.maxOpenConnections")

	log := logger.NewLogger().Child("enterprise").Child("error-detail-reporting")
	extractor := NewErrorDetailExtractor(log)

	return &ErrorDetailReporter{
		ctx:                   ctx,
		reportingServiceURL:   reportingServiceURL,
		log:                   log,
		sleepInterval:         sleepInterval,
		mainLoopSleepInterval: mainLoopSleepInterval,
		maxConcurrentRequests: maxConcurrentRequests,
		httpClient:            netClient,

		namespace:  config.GetKubeNamespace(),
		instanceID: config.GetString("INSTANCE_ID", "1"),
		region:     config.GetString("region", ""),

		init:                      make(chan struct{}),
		workspaceIDForSourceIDMap: make(map[string]string),
		destinationIDMap:          make(map[string]destDetail),
		syncers:                   make(map[string]*types.SyncSource),
		errorDetailExtractor:      extractor,
		maxOpenConnections:        maxOpenConnections,
	}
}

func (edr *ErrorDetailReporter) backendConfigSubscriber(beConfigHandle backendconfig.BackendConfig) {
	edr.log.Info("[Error Detail Reporting] Setting up reporting handler")
	defer edr.onceInit.Do(func() {
		close(edr.init)
	})
	if edr.ctx.Err() != nil {
		return
	}
	ch := beConfigHandle.Subscribe(edr.ctx, backendconfig.TopicBackendConfig)

	for c := range ch {
		conf := c.Data.(map[string]backendconfig.ConfigT)
		newWorkspaceIDForSourceIDMap := make(map[string]string)
		newDestinationIDMap := make(map[string]destDetail)
		newPIIReportingSettings := make(map[string]bool)
		var newWorkspaceID string

		for workspaceID, wConfig := range conf {
			newWorkspaceID = workspaceID
			for _, source := range wConfig.Sources {
				newWorkspaceIDForSourceIDMap[source.ID] = workspaceID
				// Reduce to destination detail based on destination-id
				newDestinationIDMap = lo.Reduce(source.Destinations, func(agg map[string]destDetail, destination backendconfig.DestinationT, _ int) map[string]destDetail {
					agg[destination.ID] = destDetail{
						DestinationDefinitionID: destination.DestinationDefinition.ID,
						DestType:                destination.DestinationDefinition.Name,
					}
					return agg
				}, newDestinationIDMap)
			}
			newPIIReportingSettings[workspaceID] = wConfig.Settings.DataRetention.DisableReportingPII
		}
		if len(conf) > 1 {
			newWorkspaceID = ""
		}
		edr.backendConfigMu.Lock()
		edr.workspaceID = newWorkspaceID
		edr.workspaceIDForSourceIDMap = newWorkspaceIDForSourceIDMap
		edr.destinationIDMap = newDestinationIDMap
		edr.backendConfigMu.Unlock()
		edr.onceInit.Do(func() {
			close(edr.init)
		})
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

	return func() {
		edr.mainLoop(edr.ctx, c)
	}
}

func (edr *ErrorDetailReporter) GetSyncer(syncerKey string) *types.SyncSource {
	edr.syncersMu.RLock()
	defer edr.syncersMu.RUnlock()
	return edr.syncers[syncerKey]
}

func (edr *ErrorDetailReporter) Report(metrics []*types.PUReportedMetric, txn *sql.Tx) {
	edr.log.Debug("[ErrorDetailReport] Report method called\n")
	if len(metrics) == 0 {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(ErrorDetailReportsTable, ErrorDetailReportsColumns...))
	if err != nil {
		_ = txn.Rollback()
		edr.log.Errorf("Failed during statement preparation: %v", err)
		return
	}
	defer func() { _ = stmt.Close() }()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := edr.getWorkspaceID(metric.ConnectionDetails.SourceID)
		metric := *metric
		destinationDetail := edr.getDestDetail(metric.ConnectionDetails.DestinationID)
		edr.log.Debugf("For DestId: %v -> DestDetail: %v", metric.ConnectionDetails.DestinationID, destinationDetail)

		// extract error-message & error-code
		errDets := edr.extractErrorDetails(metric.StatusDetail.SampleResponse)
		_, err = stmt.Exec(
			workspaceID,
			edr.namespace,
			edr.instanceID,
			metric.ConnectionDetails.SourceDefinitionId,
			metric.ConnectionDetails.SourceID,
			destinationDetail.DestinationDefinitionID,
			metric.ConnectionDetails.DestinationID,
			destinationDetail.DestType,
			metric.PUDetails.PU,
			reportedAt,
			metric.StatusDetail.Count,
			metric.StatusDetail.StatusCode,
			metric.StatusDetail.EventType,
			errDets.ErrorCode,
			errDets.ErrorMessage,
		)
		if err != nil {
			edr.log.Errorf("Failed during statement execution(each metric): %v", err)
			return
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		edr.log.Errorf("Failed during statement preparation: %v", err)
		return
	}
}

func (*ErrorDetailReporter) IsPIIReportingDisabled(_ string) bool {
	// Since we don't see the necessity for error detail reporting, we are implementing a kind of NOOP method
	return false
}

func (edr *ErrorDetailReporter) migrate(c types.SyncerConfig) (*sql.DB, error) {
	dbHandle, err := sql.Open("postgres", c.ConnInfo)
	if err != nil {
		return nil, err
	}
	dbHandle.SetMaxOpenConns(edr.maxOpenConnections)

	m := &migrator.Migrator{
		Handle:          dbHandle,
		MigrationsTable: fmt.Sprintf("%v_migrations", ErrorDetailReportsTable),
		// TODO: shall we use separate env ?
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate(ErrorDetailReportsTable)
	if err != nil {
		return nil, fmt.Errorf("could not run %v migrations: %w", ErrorDetailReportsTable, err)
	}
	return dbHandle, nil
}

func (edr *ErrorDetailReporter) getWorkspaceID(sourceID string) string {
	edr.backendConfigMu.RLock()
	defer edr.backendConfigMu.RUnlock()
	return edr.workspaceIDForSourceIDMap[sourceID]
}

func (edr *ErrorDetailReporter) getDestDetail(destID string) destDetail {
	edr.backendConfigMu.RLock()
	defer edr.backendConfigMu.RUnlock()
	return edr.destinationIDMap[destID]
}

func (edr *ErrorDetailReporter) extractErrorDetails(sampleResponse string) errorDetails {
	errMsg := edr.errorDetailExtractor.GetErrorMessage(sampleResponse)
	cleanedErrMsg := edr.errorDetailExtractor.CleanUpErrorMessage(errMsg)
	return errorDetails{
		ErrorMessage: cleanedErrMsg,
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
		"workspaceID": edr.workspaceID,
		"clientName":  label,
		"instanceId":  edr.instanceID,
	}
}

// Sending metrics to Reporting service --- STARTS
func (edr *ErrorDetailReporter) mainLoop(ctx context.Context, c types.SyncerConfig) {
	<-edr.init
	tags := edr.getTags(c.Label)

	mainLoopTimer := stats.Default.NewTaggedStat("error_detail_reports_main_loop_time", stats.TimerType, tags)
	getReportsTimer := stats.Default.NewTaggedStat("error_detail_reports_get_reports_time", stats.TimerType, tags)
	aggregateTimer := stats.Default.NewTaggedStat("error_detail_reports_aggregate_time", stats.TimerType, tags)
	getReportsSize := stats.Default.NewTaggedStat("error_detail_reports_size", stats.HistogramType, tags)
	getAggregatedReportsSize := stats.Default.NewTaggedStat("error_detail_reports_aggregated_size", stats.HistogramType, tags)

	errorDetailReportsDeleteQueryTimer := stats.Default.NewTaggedStat("error_detail_reports_delete_query_time", stats.TimerType, tags)

	edr.minReportedAtQueryTime = stats.Default.NewTaggedStat("error_detail_reports_min_reported_at_query_time", stats.TimerType, tags)
	edr.errorDetailReportsQueryTime = stats.Default.NewTaggedStat("error_detail_reports_query_time", stats.TimerType, tags)
	edr.edReportingRequestLatency = stats.Default.NewTaggedStat("error_detail_reporting_request_latency", stats.TimerType, tags)

	// In infinite loop
	// Get Reports
	// Aggregate
	// Send in a separate go-routine
	// Delete in a separate go-routine
	for {
		if ctx.Err() != nil {
			edr.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
			return
		}
		requestChan := make(chan struct{}, edr.maxConcurrentRequests.Load())
		loopStart := time.Now()
		currentMs := time.Now().UTC().Unix() / 60

		getReportsStart := time.Now()
		reports, reportedAt := edr.getReports(ctx, currentMs, c.ConnInfo)
		getReportsTimer.Since(getReportsStart)
		getReportsSize.Observe(float64(len(reports)))

		if len(reports) == 0 {
			select {
			case <-ctx.Done():
				edr.log.Infof("stopping mainLoop for syncer %s : %s", c.Label, ctx.Err())
				return
			case <-time.After(edr.sleepInterval.Load()):
			}
			continue
		}

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
			var delRows *sql.Rows
			delRows, err = dbHandle.Query(`DELETE FROM `+ErrorDetailReportsTable+` WHERE reported_at = $1`, reportedAt)
			errorDetailReportsDeleteQueryTimer.Since(deleteReportsStart)
			if err != nil {
				edr.log.Errorf(`[ Error Detail Reporting ]: Error deleting local reports from %s: %v`, ErrorDetailReportsTable, err)
			}
			delRows.Close()
		}

		mainLoopTimer.Since(loopStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(edr.mainLoopSleepInterval.Load()):
		}
	}
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
			&dbEdMetric.EDErrorDetails.StatusCode,
			&dbEdMetric.EDErrorDetails.EventType,
			&dbEdMetric.EDErrorDetails.ErrorCode,
			&dbEdMetric.EDErrorDetails.ErrorMessage,
			&dbEdMetric.EDConnectionDetails.DestType,
		)
		if err != nil {
			edr.log.Errorf("Failed while scanning rows(reported_at=%v): %v", queryMin.Int64, err)
			return []*types.EDReportsDB{}, queryMin.Int64
		}
		metrics = append(metrics, dbEdMetric)
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
		var errs []types.EDErrorDetails

		reportsCountMap := lo.CountValuesBy(reports, func(rep *types.EDReportsDB) types.EDErrorDetails {
			return types.EDErrorDetails{
				StatusCode:   rep.StatusCode,
				ErrorCode:    rep.ErrorCode,
				ErrorMessage: rep.ErrorMessage,
				EventType:    rep.EventType,
			}
		})

		reportGrpKeys := lo.Keys(reportsCountMap)
		sort.SliceStable(reportGrpKeys, func(i, j int) bool {
			irep := reportGrpKeys[i]
			jrep := reportGrpKeys[j]

			return (irep.StatusCode < jrep.StatusCode ||
				irep.ErrorMessage < jrep.ErrorMessage ||
				irep.EventType < jrep.EventType)
		})
		for _, rep := range reportGrpKeys {
			errs = append(errs, types.EDErrorDetails{
				StatusCode:   rep.StatusCode,
				ErrorCode:    rep.ErrorCode,
				ErrorMessage: rep.ErrorMessage,
				EventType:    rep.EventType,
				Count:        reportsCountMap[rep],
			})
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
		stats.Default.NewTaggedStat("error_detail_reporting_http_request", stats.CountType, httpStatTags).Increment()

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

// Sending metrics to Reporting service --- ENDS

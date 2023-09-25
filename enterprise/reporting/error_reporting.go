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
	onceInit                  sync.Once
	init                      chan struct{}
	reportingServiceURL       string
	Table                     string
	clients                   map[string]*types.Client
	log                       logger.Logger
	namespace                 string
	workspaceID               string
	instanceID                string
	region                    string
	sleepInterval             misc.ValueLoader[time.Duration]
	mainLoopSleepInterval     misc.ValueLoader[time.Duration]
	maxConcurrentRequests     misc.ValueLoader[int]
	maxOpenConnections        misc.ValueLoader[int]
	workspaceIDForSourceIDMap map[string]string
	destinationIDMap          map[string]destDetail
	srcWspMutex               sync.RWMutex
	httpClient                *http.Client
	clientsMapLock            sync.RWMutex

	errorDetailExtractor *ExtractorHandle

	minReportedAtQueryTime      stats.Measurement
	errorDetailReportsQueryTime stats.Measurement
	edReportingRequestLatency   stats.Measurement
}

type errorDetails struct {
	ErrorCode    string
	ErrorMessage string
}

func NewEdReporterFromEnvConfig() *ErrorDetailReporter {
	tr := &http.Transport{}
	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.dev.rudderlabs.com")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")

	netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	mainLoopSleepInterval := config.GetReloadableDurationVar(5, time.Second, "Reporting.mainLoopSleepInterval")
	sleepInterval := config.GetReloadableDurationVar(30, time.Second, "Reporting.sleepInterval")
	maxConcurrentRequests := config.GetReloadableIntVar(32, 1, "Reporting.maxConcurrentRequests")
	maxOpenConnections := config.GetReloadableIntVar(16, 1, "Reporting.errorReporting.maxOpenConnections")

	log := logger.NewLogger().Child("enterprise").Child("error-detail-reporting")
	extractor := NewErrorDetailExtractor(log)

	return &ErrorDetailReporter{
		reportingServiceURL:   reportingServiceURL,
		Table:                 ErrorDetailReportsTable,
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
		clients:                   make(map[string]*types.Client),
		errorDetailExtractor:      extractor,
		maxOpenConnections:        maxOpenConnections,
	}
}

func (edRep *ErrorDetailReporter) AddClient(ctx context.Context, c types.Config) {
	if c.ClientName == "" {
		c.ClientName = types.CoreReportingClient
	}

	edRep.clientsMapLock.RLock()
	if _, ok := edRep.clients[c.ClientName]; ok {
		return
	}
	edRep.clientsMapLock.RUnlock()
	dbHandle, err := edRep.migrate(c)
	if err != nil {
		panic(fmt.Errorf("failed during migration: %v", err))
	}
	edRep.clientsMapLock.Lock()
	edRep.clients[c.ClientName] = &types.Client{Config: c, DbHandle: dbHandle}
	edRep.clientsMapLock.Unlock()
	// start main-loop
	edRep.mainLoop(ctx, c.ClientName)
}

func (edRep *ErrorDetailReporter) GetClient(clientName string) *types.Client {
	edRep.clientsMapLock.RLock()
	defer edRep.clientsMapLock.RUnlock()
	if c, ok := edRep.clients[clientName]; ok {
		return c
	}
	return nil
}

func (edRep *ErrorDetailReporter) setup(beConfigHandle backendconfig.BackendConfig) {
	edRep.log.Info("[Error Detail Reporting] Setting up reporting handler")

	ch := beConfigHandle.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)

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
		edRep.srcWspMutex.Lock()
		edRep.workspaceID = newWorkspaceID
		edRep.workspaceIDForSourceIDMap = newWorkspaceIDForSourceIDMap
		edRep.destinationIDMap = newDestinationIDMap
		edRep.srcWspMutex.Unlock()
		// r.piiReportingSettings = newPIIReportingSettings
		edRep.onceInit.Do(func() {
			close(edRep.init)
		})
	}

	edRep.onceInit.Do(func() {
		close(edRep.init)
	})
}

func (edRep *ErrorDetailReporter) Report(metrics []*types.PUReportedMetric, txn *sql.Tx) {
	edRep.log.Debug("[ErrorDetailReport] Report method called\n")
	if len(metrics) == 0 {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(edRep.Table, ErrorDetailReportsColumns...))
	if err != nil {
		_ = txn.Rollback()
		edRep.log.Errorf("Failed during statement preparation: %v", err)
		return
	}
	defer func() { _ = stmt.Close() }()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := edRep.getWorkspaceID(metric.ConnectionDetails.SourceID)
		metric := *metric
		destinationDetail := edRep.getDestDetail(metric.ConnectionDetails.DestinationID)
		edRep.log.Debugf("For DestId: %v -> DestDetail: %v", metric.ConnectionDetails.DestinationID, destinationDetail)

		// extract error-message & error-code
		errDets := edRep.extractErrorDetails(metric.StatusDetail.SampleResponse)
		_, err = stmt.Exec(
			workspaceID,
			edRep.namespace,
			edRep.instanceID,
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
			edRep.log.Errorf("Failed during statement execution(each metric): %v", err)
			return
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		edRep.log.Errorf("Failed during statement preparation: %v", err)
		return
	}
}

func (edRep *ErrorDetailReporter) WaitForSetup(ctx context.Context, clientName string) error {
	for {
		if edRep.GetClient(clientName) != nil {
			break
		}
		if err := misc.SleepCtx(ctx, time.Second); err != nil {
			return fmt.Errorf("wait for setup: %w", ctx.Err())
		}
	}

	return nil
}

func (*ErrorDetailReporter) IsPIIReportingDisabled(_ string) bool {
	// Since we don't see the necessity for error detail reporting, we are implementing a kind of NOOP method
	return false
}

func (edRep *ErrorDetailReporter) migrate(c types.Config) (*sql.DB, error) {
	dbHandle, err := sql.Open("postgres", c.ConnInfo)
	if err != nil {
		return nil, err
	}
	dbHandle.SetMaxOpenConns(edRep.maxOpenConnections.Load())

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

func (edRep *ErrorDetailReporter) getWorkspaceID(sourceID string) string {
	edRep.srcWspMutex.RLock()
	defer func() { edRep.srcWspMutex.RUnlock() }()
	return edRep.workspaceIDForSourceIDMap[sourceID]
}

func (edRep *ErrorDetailReporter) getDestDetail(destID string) destDetail {
	edRep.srcWspMutex.RLock()
	defer func() { edRep.srcWspMutex.RUnlock() }()
	return edRep.destinationIDMap[destID]
}

func (edRep *ErrorDetailReporter) extractErrorDetails(sampleResponse string) errorDetails {
	errMsg := edRep.errorDetailExtractor.GetErrorMessage(sampleResponse)
	cleanedErrMsg := edRep.errorDetailExtractor.CleanUpErrorMessage(errMsg)
	return errorDetails{
		ErrorMessage: cleanedErrMsg,
	}
}

func (edRep *ErrorDetailReporter) getDBHandle(clientName string) (*sql.DB, error) {
	client := edRep.GetClient(clientName)
	if client != nil {
		return client.DbHandle, nil
	}

	return nil, fmt.Errorf("DBHandle not found for client name: %s", clientName)
}

func (edRep *ErrorDetailReporter) getTags(clientName string) stats.Tags {
	return stats.Tags{
		"workspaceID": edRep.workspaceID,
		"clientName":  clientName,
		"instanceId":  edRep.instanceID,
	}
}

// Sending metrics to Reporting service --- STARTS
func (edRep *ErrorDetailReporter) mainLoop(ctx context.Context, clientName string) {
	tags := edRep.getTags(clientName)

	mainLoopTimer := stats.Default.NewTaggedStat("error_detail_reports_main_loop_time", stats.TimerType, tags)
	getReportsTimer := stats.Default.NewTaggedStat("error_detail_reports_get_reports_time", stats.TimerType, tags)
	aggregateTimer := stats.Default.NewTaggedStat("error_detail_reports_aggregate_time", stats.TimerType, tags)
	getReportsSize := stats.Default.NewTaggedStat("error_detail_reports_size", stats.HistogramType, tags)
	getAggregatedReportsSize := stats.Default.NewTaggedStat("error_detail_reports_aggregated_size", stats.HistogramType, tags)

	errorDetailReportsDeleteQueryTimer := stats.Default.NewTaggedStat("error_detail_reports_delete_query_time", stats.TimerType, tags)

	edRep.minReportedAtQueryTime = stats.Default.NewTaggedStat("error_detail_reports_min_reported_at_query_time", stats.TimerType, tags)
	edRep.errorDetailReportsQueryTime = stats.Default.NewTaggedStat("error_detail_reports_query_time", stats.TimerType, tags)
	edRep.edReportingRequestLatency = stats.Default.NewTaggedStat("error_detail_reporting_request_latency", stats.TimerType, tags)

	// In infinite loop
	// Get Reports
	// Aggregate
	// Send in a separate go-routine
	// Delete in a separate go-routine
	for {
		if ctx.Err() != nil {
			edRep.log.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
			return
		}
		requestChan := make(chan struct{}, edRep.maxConcurrentRequests.Load())
		loopStart := time.Now()
		currentMs := time.Now().UTC().Unix() / 60

		getReportsStart := time.Now()
		reports, reportedAt := edRep.getReports(ctx, currentMs, clientName)
		getReportsTimer.Since(getReportsStart)
		getReportsSize.Observe(float64(len(reports)))

		if len(reports) == 0 {
			select {
			case <-ctx.Done():
				edRep.log.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
				return
			case <-time.After(edRep.sleepInterval.Load()):
			}
			continue
		}

		aggregationStart := time.Now()
		metrics := edRep.aggregate(reports)
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
				err := edRep.sendMetric(errCtx, clientName, metricToSend)
				if err != nil {
					edRep.log.Error("Error while sending to Reporting service:", err)
				}
				<-requestChan
				return err
			})
		}

		err := errGroup.Wait()
		if err == nil {
			// sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_at = %d`, ErrorDetailReportsTable, reportedAt)
			dbHandle, err := edRep.getDBHandle(clientName)
			if err != nil {
				edRep.log.Errorf("error reports deletion getDbhandle failed: %v", err)
				continue
			}
			deleteReportsStart := time.Now()
			var delRows *sql.Rows
			delRows, err = dbHandle.Query(`DELETE FROM `+ErrorDetailReportsTable+` WHERE reported_at = $1`, reportedAt)
			errorDetailReportsDeleteQueryTimer.Since(deleteReportsStart)
			if err != nil {
				edRep.log.Errorf(`[ Error Detail Reporting ]: Error deleting local reports from %s: %v`, ErrorDetailReportsTable, err)
			}
			delRows.Close()
		}

		mainLoopTimer.Since(loopStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(edRep.mainLoopSleepInterval.Load()):
		}
	}
}

func (edRep *ErrorDetailReporter) getReports(ctx context.Context, currentMs int64, clientName string) ([]*types.EDReportsDB, int64) {
	var queryMin sql.NullInt64
	dbHandle, err := edRep.getDBHandle(clientName)
	if err != nil {
		edRep.log.Errorf("Failed while getting DbHandle: %v", err)
		return []*types.EDReportsDB{}, queryMin.Int64
	}

	queryStart := time.Now()
	err = dbHandle.QueryRowContext(ctx, "SELECT reported_at FROM "+ErrorDetailReportsTable+" WHERE reported_at < $1 ORDER BY reported_at ASC LIMIT 1", currentMs).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		edRep.log.Errorf("Failed while getting reported_at: %v", err)
		return []*types.EDReportsDB{}, queryMin.Int64
	}
	edRep.minReportedAtQueryTime.Since(queryStart)
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
		edRep.log.Errorf("Failed while getting reports(reported_at=%v): %v", queryMin.Int64, err)
		return []*types.EDReportsDB{}, queryMin.Int64
	}
	edRep.errorDetailReportsQueryTime.Since(queryStart)
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
			edRep.log.Errorf("Failed while scanning rows(reported_at=%v): %v", queryMin.Int64, err)
			return []*types.EDReportsDB{}, queryMin.Int64
		}
		metrics = append(metrics, dbEdMetric)
	}
	return metrics, queryMin.Int64
}

func (edRep *ErrorDetailReporter) aggregate(reports []*types.EDReportsDB) []*types.EDMetric {
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
	var edReportingMetrics []*types.EDMetric
	groupKeys := lo.Keys(groupedReports)
	sort.Strings(groupKeys)

	for _, key := range groupKeys {
		reports := groupedReports[key]
		firstReport := reports[0]
		edRepSchema := types.EDMetric{
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
		edRepSchema.Errors = errs
		edReportingMetrics = append(edReportingMetrics, &edRepSchema)
	}
	return edReportingMetrics
}

func (edRep *ErrorDetailReporter) sendMetric(ctx context.Context, clientName string, metric *types.EDMetric) error {
	payload, err := json.Marshal(metric)
	if err != nil {
		return fmt.Errorf("marshal failure: %w", err)
	}
	operation := func() error {
		uri := fmt.Sprintf("%s/recordErrors", edRep.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		if edRep.region != "" {
			q := req.URL.Query()
			q.Add("region", edRep.region)
			req.URL.RawQuery = q.Encode()
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		httpRequestStart := time.Now()
		resp, err := edRep.httpClient.Do(req)
		if err != nil {
			edRep.log.Errorf("Sending request failed: %v", err)
			return err
		}

		edRep.edReportingRequestLatency.Since(httpRequestStart)
		httpStatTags := edRep.getTags(clientName)
		httpStatTags["status"] = strconv.Itoa(resp.StatusCode)
		stats.Default.NewTaggedStat("error_detail_reporting_http_request", stats.CountType, httpStatTags).Increment()

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		edRep.log.Debugf("[ErrorDetailReporting]Response from ReportingAPI: %v\n", string(respBody))
		if err != nil {
			edRep.log.Errorf("Reading response failed: %w", err)
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode: %d error: %v`, resp.StatusCode, string(respBody))
			edRep.log.Error(err.Error())
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		edRep.log.Errorf(`[ Error Detail Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		edRep.log.Errorf(`[ Error Detail Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

// Sending metrics to Reporting service --- ENDS

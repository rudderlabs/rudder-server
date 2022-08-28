package reporting

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/errgroup"
)

const REPORTS_TABLE = "reports"

var maxConcurrentRequests int

const (
	STAT_REPORTING_MAIN_LOOP_TIME                 = "reporting_client_main_loop_time"
	STAT_REPORTING_GET_REPORTS_TIME               = "reporting_client_get_reports_time"
	STAT_REPORTING_GET_REPORTS_COUNT              = "reporting_client_get_reports_count"
	STAT_REPORTING_GET_AGGREGATED_REPORTS_TIME    = "reporting_client_get_aggregated_reports_time"
	STAT_REPORTING_GET_AGGREGATED_REPORTS_COUNT   = "reporting_client_get_aggregated_reports_count"
	STAT_REPORTING_HTTP_REQ_LATENCY               = "reporting_client_http_request_latency"
	STAT_REPORTING_HTTP_REQ                       = "reporting_client_http_request"
	STAT_REPORTING_GET_MIN_REPORTED_AT_QUERY_TIME = "reporting_client_get_min_reported_at_query_time"
	STAT_REPORTING_GET_REPORTS_QUERY_TIME         = "reporting_client_get_reports_query_time"
)

type HandleT struct {
	clients                       map[string]*types.Client
	clientsMapLock                sync.RWMutex
	logger                        logger.LoggerI
	reportingServiceURL           string
	namespace                     string
	workspaceID                   string
	instanceID                    string
	workspaceIDForSourceIDMap     map[string]string
	workspaceIDForSourceIDMapLock sync.RWMutex
	whActionsOnly                 bool
	sleepInterval                 time.Duration
	mainLoopSleepInterval         time.Duration

	getMinReportedAtQueryTime stats.RudderStats
	getReportsQueryTime       stats.RudderStats
	requestLatency            stats.RudderStats
}

func NewFromEnvConfig() *HandleT {
	var sleepInterval, mainLoopSleepInterval time.Duration
	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")
	config.RegisterDurationConfigVariable(5, &mainLoopSleepInterval, true, time.Second, "Reporting.mainLoopSleepInterval")
	config.RegisterDurationConfigVariable(30, &sleepInterval, true, time.Second, "Reporting.sleepInterval")
	config.RegisterIntConfigVariable(32, &maxConcurrentRequests, true, 1, "Reporting.maxConcurrentRequests")
	reportingLogger := logger.NewLogger().Child("enterprise").Child("reporting")
	// only send reports for wh actions sources if whActionsOnly is configured
	whActionsOnly := config.GetEnvAsBool("REPORTING_WH_ACTIONS_ONLY", false)
	if whActionsOnly {
		reportingLogger.Info("REPORTING_WH_ACTIONS_ONLY enabled.only sending reports relevant to wh actions.")
	}

	return &HandleT{
		logger:                    reportingLogger,
		clients:                   make(map[string]*types.Client),
		reportingServiceURL:       reportingServiceURL,
		namespace:                 config.GetKubeNamespace(),
		instanceID:                config.GetEnv("INSTANCE_ID", "1"),
		workspaceIDForSourceIDMap: make(map[string]string),
		whActionsOnly:             whActionsOnly,
		sleepInterval:             sleepInterval,
		mainLoopSleepInterval:     mainLoopSleepInterval,
	}
}

func (handle *HandleT) setup(beConfigHandle backendconfig.BackendConfig) {
	handle.logger.Info("[[ Reporting ]] Setting up reporting handler")

	ch := beConfigHandle.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)

	for beconfig := range ch {
		sources := beconfig.Data.(backendconfig.ConfigT)
		handle.workspaceIDForSourceIDMapLock.Lock()
		if sources.WorkspaceID == "" {
			handle.workspaceIDForSourceIDMap = make(map[string]string)
			for _, source := range sources.Sources {
				if _, ok := handle.workspaceIDForSourceIDMap[source.ID]; !ok {
					handle.workspaceIDForSourceIDMap[source.ID] = source.WorkspaceID
				}
			}
		} else {
			handle.workspaceID = sources.WorkspaceID
		}
		handle.workspaceIDForSourceIDMapLock.Unlock()
	}
}

func (handle *HandleT) getWorkspaceID(sourceID string) string {
	if handle.workspaceID != "" {
		return handle.workspaceID
	}

	handle.workspaceIDForSourceIDMapLock.RLock()
	defer handle.workspaceIDForSourceIDMapLock.RUnlock()

	return handle.workspaceIDForSourceIDMap[sourceID]
}

func (handle *HandleT) AddClient(ctx context.Context, c types.Config) {
	if c.ClientName == "" {
		c.ClientName = types.CORE_REPORTING_CLIENT
	}

	handle.clientsMapLock.RLock()
	if _, ok := handle.clients[c.ClientName]; ok {
		return
	}
	handle.clientsMapLock.RUnlock()

	dbHandle, err := sql.Open("postgres", c.ConnInfo)
	if err != nil {
		panic(err)
	}

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "reports_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate("reports")
	if err != nil {
		panic(fmt.Errorf("could not run reports migrations: %w", err))
	}

	handle.clientsMapLock.Lock()
	handle.clients[c.ClientName] = &types.Client{Config: c, DbHandle: dbHandle}
	handle.clientsMapLock.Unlock()

	handle.mainLoop(ctx, c.ClientName)
}

func (handle *HandleT) WaitForSetup(ctx context.Context, clientName string) {
	for {
		if handle.GetClient(clientName) == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

func (handle *HandleT) GetClient(clientName string) *types.Client {
	handle.clientsMapLock.RLock()
	defer handle.clientsMapLock.RUnlock()

	if c, ok := handle.clients[clientName]; ok {
		return c
	}

	return nil
}

func (handle *HandleT) getDBHandle(clientName string) (*sql.DB, error) {
	client := handle.GetClient(clientName)
	if client != nil {
		return client.DbHandle, nil
	}

	return nil, fmt.Errorf("DBHandle not found for client name: %s", clientName)
}

func (handle *HandleT) getReports(current_ms int64, clientName string) (reports []*types.ReportByStatus, reportedAt int64) {
	sqlStatement := fmt.Sprintf(`SELECT reported_at FROM %s WHERE reported_at < %d ORDER BY reported_at ASC LIMIT 1`, REPORTS_TABLE, current_ms)
	var queryMin sql.NullInt64
	dbHandle, err := handle.getDBHandle(clientName)
	if err != nil {
		panic(err)
	}

	queryStart := time.Now()
	err = dbHandle.QueryRow(sqlStatement).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	handle.getMinReportedAtQueryTime.Since(queryStart)
	if !queryMin.Valid {
		return nil, 0
	}

	sqlStatement = fmt.Sprintf(`SELECT workspace_id, namespace, instance_id, source_definition_id, source_category, source_id, destination_definition_id, destination_id, source_batch_id, source_task_id, source_task_run_id, source_job_id, source_job_run_id, in_pu, pu, reported_at, status, count, terminal_state, initial_state, status_code, sample_response, sample_event, event_name, event_type FROM %s WHERE reported_at = %d`, REPORTS_TABLE, queryMin.Int64)
	var rows *sql.Rows
	queryStart = time.Now()
	rows, err = dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	handle.getReportsQueryTime.Since(queryStart)
	defer rows.Close()

	var metricReports []*types.ReportByStatus
	for rows.Next() {
		metricReport := types.ReportByStatus{StatusDetail: &types.StatusDetail{}}
		err = rows.Scan(&metricReport.InstanceDetails.WorkspaceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID, &metricReport.ConnectionDetails.SourceDefinitionId, &metricReport.ConnectionDetails.SourceCategory, &metricReport.ConnectionDetails.SourceID, &metricReport.ConnectionDetails.DestinationDefinitionId, &metricReport.ConnectionDetails.DestinationID, &metricReport.ConnectionDetails.SourceBatchID, &metricReport.ConnectionDetails.SourceTaskID, &metricReport.ConnectionDetails.SourceTaskRunID, &metricReport.ConnectionDetails.SourceJobID, &metricReport.ConnectionDetails.SourceJobRunID, &metricReport.PUDetails.InPU, &metricReport.PUDetails.PU, &metricReport.ReportedAt, &metricReport.StatusDetail.Status, &metricReport.StatusDetail.Count, &metricReport.PUDetails.TerminalPU, &metricReport.PUDetails.InitialPU, &metricReport.StatusDetail.StatusCode, &metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent, &metricReport.StatusDetail.EventName, &metricReport.StatusDetail.EventType)
		if err != nil {
			panic(err)
		}
		metricReports = append(metricReports, &metricReport)
	}

	return metricReports, queryMin.Int64
}

func (handle *HandleT) getAggregatedReports(reports []*types.ReportByStatus) []*types.Metric {
	metricsByGroup := map[string]*types.Metric{}

	reportIdentifier := func(report *types.ReportByStatus) string {
		groupingIdentifiers := []string{report.InstanceDetails.WorkspaceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID, report.ConnectionDetails.SourceID, report.ConnectionDetails.DestinationID, report.ConnectionDetails.SourceBatchID, report.ConnectionDetails.SourceTaskID, report.ConnectionDetails.SourceTaskRunID, report.ConnectionDetails.SourceJobID, report.ConnectionDetails.SourceJobRunID, report.PUDetails.InPU, report.PUDetails.PU, report.StatusDetail.Status, fmt.Sprint(report.StatusDetail.StatusCode), report.StatusDetail.EventName, report.StatusDetail.EventType}
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
					SourceBatchID:           report.SourceBatchID,
					SourceTaskID:            report.SourceTaskID,
					SourceTaskRunID:         report.SourceTaskRunID,
					SourceJobID:             report.SourceJobID,
					SourceJobRunID:          report.SourceJobRunID,
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
		statusDetailInterface := funk.Find(metricsByGroup[identifier].StatusDetails, func(i *types.StatusDetail) bool {
			return i.Status == report.StatusDetail.Status && i.StatusCode == report.StatusDetail.StatusCode
		})
		if statusDetailInterface == nil {
			metricsByGroup[identifier].StatusDetails = append(metricsByGroup[identifier].StatusDetails, &types.StatusDetail{
				Status:         report.StatusDetail.Status,
				StatusCode:     report.StatusDetail.StatusCode,
				Count:          report.StatusDetail.Count,
				SampleResponse: report.StatusDetail.SampleResponse,
				SampleEvent:    report.StatusDetail.SampleEvent,
				EventName:      report.StatusDetail.EventName,
				EventType:      report.StatusDetail.EventType,
			})
			continue
		}
		statusDetail := statusDetailInterface.(*types.StatusDetail)
		statusDetail.Count += report.StatusDetail.Count
		statusDetail.SampleResponse = report.StatusDetail.SampleResponse
		statusDetail.SampleEvent = report.StatusDetail.SampleEvent
	}

	var values []*types.Metric
	for _, val := range metricsByGroup {
		values = append(values, val)
	}
	return values
}

func (handle *HandleT) mainLoop(ctx context.Context, clientName string) {
	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	tags := handle.getTags(clientName)
	mainLoopTimer := stats.NewTaggedStat(STAT_REPORTING_MAIN_LOOP_TIME, stats.TimerType, tags)
	getReportsTimer := stats.NewTaggedStat(STAT_REPORTING_GET_REPORTS_TIME, stats.TimerType, tags)
	getReportsCount := stats.NewTaggedStat(STAT_REPORTING_GET_REPORTS_COUNT, stats.HistogramType, tags)
	getAggregatedReportsTimer := stats.NewTaggedStat(STAT_REPORTING_GET_AGGREGATED_REPORTS_TIME, stats.TimerType, tags)
	getAggregatedReportsCount := stats.NewTaggedStat(STAT_REPORTING_GET_AGGREGATED_REPORTS_COUNT, stats.HistogramType, tags)

	handle.getMinReportedAtQueryTime = stats.NewTaggedStat(STAT_REPORTING_GET_MIN_REPORTED_AT_QUERY_TIME, stats.TimerType, tags)
	handle.getReportsQueryTime = stats.NewTaggedStat(STAT_REPORTING_GET_REPORTS_QUERY_TIME, stats.TimerType, tags)
	handle.requestLatency = stats.NewTaggedStat(STAT_REPORTING_HTTP_REQ_LATENCY, stats.TimerType, tags)
	for {
		if ctx.Err() != nil {
			handle.logger.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
			return
		}
		requestChan := make(chan struct{}, maxConcurrentRequests)
		loopStart := time.Now()
		currentMs := time.Now().UTC().Unix() / 60

		getReportsStart := time.Now()
		reports, reportedAt := handle.getReports(currentMs, clientName)
		getReportsTimer.Since(getReportsStart)
		getReportsCount.Observe(float64(len(reports)))
		if len(reports) == 0 {
			select {
			case <-ctx.Done():
				handle.logger.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
				return
			case <-time.After(handle.sleepInterval):
			}
			continue
		}

		getAggregatedReportsStart := time.Now()
		metrics := handle.getAggregatedReports(reports)
		getAggregatedReportsTimer.Since(getAggregatedReportsStart)
		getAggregatedReportsCount.Observe(float64(len(metrics)))

		errGroup, errCtx := errgroup.WithContext(ctx)
		for _, metric := range metrics {
			if handle.whActionsOnly && metric.SourceCategory != "warehouse" {
				// if whActionsOnly is true, we only send reports for wh actions sources
				// we silently drop all other reports
				continue
			}
			metricToSend := metric
			requestChan <- struct{}{}
			if errCtx.Err() != nil {
				// if any of errGroup's goroutines fail - dont send anymore requests for this batch
				break
			}
			errGroup.Go(func() error {
				err := handle.sendMetric(errCtx, netClient, clientName, metricToSend)
				<-requestChan
				return err
			})
		}

		err := errGroup.Wait()
		if err == nil {
			sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_at = %d`, REPORTS_TABLE, reportedAt)
			dbHandle, err := handle.getDBHandle(clientName)
			if err != nil {
				panic(err)
			}
			_, err = dbHandle.Exec(sqlStatement)
			if err != nil {
				handle.logger.Errorf(`[ Reporting ]: Error deleting local reports from %s: %v`, REPORTS_TABLE, err)
			}
		}

		mainLoopTimer.Since(loopStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(handle.mainLoopSleepInterval):
		}
	}
}

func (handle *HandleT) sendMetric(ctx context.Context, netClient *http.Client, clientName string, metric *types.Metric) error {
	payload, err := json.Marshal(metric)
	if err != nil {
		panic(err)
	}
	operation := func() error {
		uri := fmt.Sprintf("%s/metrics?version=v1", handle.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")

		httpRequestStart := time.Now()
		resp, err := netClient.Do(req)
		if err != nil {
			handle.logger.Error(err.Error())
			return err
		}

		handle.requestLatency.Since(httpRequestStart)
		httpStatTags := handle.getTags(clientName)
		httpStatTags["status"] = strconv.Itoa(resp.StatusCode)
		stats.NewTaggedStat(STAT_REPORTING_HTTP_REQ, stats.CountType, httpStatTags).Count(1)

		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			handle.logger.Error(err.Error())
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`Received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
		}
		return err
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err = backoff.RetryNotify(operation, b, func(err error, t time.Duration) {
		handle.logger.Errorf(`[ Reporting ]: Error reporting to service: %v`, err)
	})
	if err != nil {
		handle.logger.Errorf(`[ Reporting ]: Error making request to reporting service: %v`, err)
	}
	return err
}

func isMetricPosted(status int) bool {
	if status == 429 {
		return false
	}

	return status >= 200 && status < 500
}

func getPIIColumnsToExclude() []string {
	piiColumnsToExclude := strings.Split(config.GetString("REPORTING_PII_COLUMNS_TO_EXCLUDE", ""), ",")
	for i := range piiColumnsToExclude {
		piiColumnsToExclude[i] = strings.Trim(piiColumnsToExclude[i], " ")
	}
	return piiColumnsToExclude
}

func transformMetricForPII(metric types.PUReportedMetric, piiColumns []string) types.PUReportedMetric {
	for _, col := range piiColumns {
		if col == "sample_event" {
			metric.StatusDetail.SampleEvent = []byte(`{}`)
		} else if col == "sample_response" {
			metric.StatusDetail.SampleResponse = ""
		}
	}

	return metric
}

func (handle *HandleT) Report(metrics []*types.PUReportedMetric, txn *sql.Tx) {
	if len(metrics) == 0 {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(REPORTS_TABLE, "workspace_id", "namespace", "instance_id", "source_definition_id", "source_category", "source_id", "destination_definition_id", "destination_id", "source_batch_id", "source_task_id", "source_task_run_id", "source_job_id", "source_job_run_id", "in_pu", "pu", "reported_at", "status", "count", "terminal_state", "initial_state", "status_code", "sample_response", "sample_event", "event_name", "event_type"))
	if err != nil {
		txn.Rollback()
		panic(err)
	}
	defer stmt.Close()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := handle.getWorkspaceID(metric.ConnectionDetails.SourceID)
		metric := transformMetricForPII(*metric, getPIIColumnsToExclude())

		_, err = stmt.Exec(workspaceID, handle.namespace, handle.instanceID, metric.ConnectionDetails.SourceDefinitionId, metric.ConnectionDetails.SourceCategory, metric.ConnectionDetails.SourceID, metric.ConnectionDetails.DestinationDefinitionId, metric.ConnectionDetails.DestinationID, metric.ConnectionDetails.SourceBatchID, metric.ConnectionDetails.SourceTaskID, metric.ConnectionDetails.SourceTaskRunID, metric.ConnectionDetails.SourceJobID, metric.ConnectionDetails.SourceJobRunID, metric.PUDetails.InPU, metric.PUDetails.PU, reportedAt, metric.StatusDetail.Status, metric.StatusDetail.Count, metric.PUDetails.TerminalPU, metric.PUDetails.InitialPU, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse, string(metric.StatusDetail.SampleEvent), metric.StatusDetail.EventName, metric.StatusDetail.EventType)
		if err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
}

func (handle *HandleT) getTags(clientName string) stats.Tags {
	return stats.Tags{
		"namespace":   handle.namespace,
		"workspaceId": handle.workspaceID,
		"instanceId":  handle.instanceID,
		"clientName":  clientName,
	}
}

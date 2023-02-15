package reporting

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/errgroup"
)

const ReportsTable = "reports"

var maxConcurrentRequests int

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

type HandleT struct {
	init                      chan struct{}
	onceInit                  sync.Once
	clients                   map[string]*types.Client
	clientsMapLock            sync.RWMutex
	log                       logger.Logger
	reportingServiceURL       string
	namespace                 string
	workspaceID               string
	instanceID                string
	workspaceIDForSourceIDMap map[string]string
	piiReportingSettings      map[string]bool
	whActionsOnly             bool
	region                    string
	sleepInterval             time.Duration
	mainLoopSleepInterval     time.Duration

	getMinReportedAtQueryTime stats.Measurement
	getReportsQueryTime       stats.Measurement
	requestLatency            stats.Measurement
}

func NewFromEnvConfig(log logger.Logger) *HandleT {
	var sleepInterval, mainLoopSleepInterval time.Duration
	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")
	config.RegisterDurationConfigVariable(5, &mainLoopSleepInterval, true, time.Second, "Reporting.mainLoopSleepInterval")
	config.RegisterDurationConfigVariable(30, &sleepInterval, true, time.Second, "Reporting.sleepInterval")
	config.RegisterIntConfigVariable(32, &maxConcurrentRequests, true, 1, "Reporting.maxConcurrentRequests")
	// only send reports for wh actions sources if whActionsOnly is configured
	whActionsOnly := config.GetBool("REPORTING_WH_ACTIONS_ONLY", false)
	if whActionsOnly {
		log.Info("REPORTING_WH_ACTIONS_ONLY enabled.only sending reports relevant to wh actions.")
	}

	return &HandleT{
		init:                      make(chan struct{}),
		log:                       log,
		clients:                   make(map[string]*types.Client),
		reportingServiceURL:       reportingServiceURL,
		namespace:                 config.GetKubeNamespace(),
		instanceID:                config.GetString("INSTANCE_ID", "1"),
		workspaceIDForSourceIDMap: make(map[string]string),
		piiReportingSettings:      make(map[string]bool),
		whActionsOnly:             whActionsOnly,
		sleepInterval:             sleepInterval,
		mainLoopSleepInterval:     mainLoopSleepInterval,
		region:                    config.GetString("region", ""),
	}
}

func (r *HandleT) setup(beConfigHandle backendconfig.BackendConfig) {
	r.log.Info("[[ Reporting ]] Setting up reporting handler")

	ch := beConfigHandle.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)

	for c := range ch {
		conf := c.Data.(map[string]backendconfig.ConfigT)
		newWorkspaceIDForSourceIDMap := make(map[string]string)
		newPIIReportingSettings := make(map[string]bool)
		var newWorkspaceID string

		for workspaceID, wConfig := range conf {
			newWorkspaceID = workspaceID
			for _, source := range wConfig.Sources {
				newWorkspaceIDForSourceIDMap[source.ID] = workspaceID
			}
			newPIIReportingSettings[workspaceID] = wConfig.Settings.DataRetention.DisableReportingPII
		}
		if len(conf) > 1 {
			newWorkspaceID = ""
		}
		r.workspaceID = newWorkspaceID
		r.workspaceIDForSourceIDMap = newWorkspaceIDForSourceIDMap
		r.piiReportingSettings = newPIIReportingSettings
		r.onceInit.Do(func() {
			close(r.init)
		})
	}

	r.onceInit.Do(func() {
		close(r.init)
	})
}

func (r *HandleT) getWorkspaceID(sourceID string) string {
	<-r.init
	return r.workspaceIDForSourceIDMap[sourceID]
}

func (r *HandleT) AddClient(ctx context.Context, c types.Config) {
	if c.ClientName == "" {
		c.ClientName = types.CoreReportingClient
	}

	r.clientsMapLock.RLock()
	if _, ok := r.clients[c.ClientName]; ok {
		return
	}
	r.clientsMapLock.RUnlock()

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

	r.clientsMapLock.Lock()
	r.clients[c.ClientName] = &types.Client{Config: c, DbHandle: dbHandle}
	r.clientsMapLock.Unlock()

	r.mainLoop(ctx, c.ClientName)
}

func (r *HandleT) WaitForSetup(ctx context.Context, clientName string) error {
	for {
		if r.GetClient(clientName) != nil {
			break
		}
		if err := misc.SleepCtx(ctx, time.Second); err != nil {
			return fmt.Errorf("wait for setup: %w", ctx.Err())
		}
	}

	return nil
}

func (r *HandleT) GetClient(clientName string) *types.Client {
	r.clientsMapLock.RLock()
	defer r.clientsMapLock.RUnlock()

	if c, ok := r.clients[clientName]; ok {
		return c
	}

	return nil
}

func (r *HandleT) getDBHandle(clientName string) (*sql.DB, error) {
	client := r.GetClient(clientName)
	if client != nil {
		return client.DbHandle, nil
	}

	return nil, fmt.Errorf("DBHandle not found for client name: %s", clientName)
}

func (r *HandleT) getReports(currentMs int64, clientName string) (reports []*types.ReportByStatus, reportedAt int64) {
	sqlStatement := fmt.Sprintf(`SELECT reported_at FROM %s WHERE reported_at < %d ORDER BY reported_at ASC LIMIT 1`, ReportsTable, currentMs)
	var queryMin sql.NullInt64
	dbHandle, err := r.getDBHandle(clientName)
	if err != nil {
		panic(err)
	}

	queryStart := time.Now()
	err = dbHandle.QueryRow(sqlStatement).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	r.getMinReportedAtQueryTime.Since(queryStart)
	if !queryMin.Valid {
		return nil, 0
	}

	sqlStatement = fmt.Sprintf(`SELECT workspace_id, namespace, instance_id, source_definition_id, source_category, source_id, destination_definition_id, destination_id, source_task_run_id, source_job_id, source_job_run_id, in_pu, pu, reported_at, status, count, terminal_state, initial_state, status_code, sample_response, sample_event, event_name, event_type FROM %s WHERE reported_at = %d`, ReportsTable, queryMin.Int64)
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
		err = rows.Scan(&metricReport.InstanceDetails.WorkspaceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID, &metricReport.ConnectionDetails.SourceDefinitionId, &metricReport.ConnectionDetails.SourceCategory, &metricReport.ConnectionDetails.SourceID, &metricReport.ConnectionDetails.DestinationDefinitionId, &metricReport.ConnectionDetails.DestinationID, &metricReport.ConnectionDetails.SourceTaskRunID, &metricReport.ConnectionDetails.SourceJobID, &metricReport.ConnectionDetails.SourceJobRunID, &metricReport.PUDetails.InPU, &metricReport.PUDetails.PU, &metricReport.ReportedAt, &metricReport.StatusDetail.Status, &metricReport.StatusDetail.Count, &metricReport.PUDetails.TerminalPU, &metricReport.PUDetails.InitialPU, &metricReport.StatusDetail.StatusCode, &metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent, &metricReport.StatusDetail.EventName, &metricReport.StatusDetail.EventType)
		if err != nil {
			panic(err)
		}
		metricReports = append(metricReports, &metricReport)
	}

	return metricReports, queryMin.Int64
}

func (*HandleT) getAggregatedReports(reports []*types.ReportByStatus) []*types.Metric {
	metricsByGroup := map[string]*types.Metric{}

	reportIdentifier := func(report *types.ReportByStatus) string {
		groupingIdentifiers := []string{report.InstanceDetails.WorkspaceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID, report.ConnectionDetails.SourceID, report.ConnectionDetails.DestinationID, report.ConnectionDetails.SourceTaskRunID, report.ConnectionDetails.SourceJobID, report.ConnectionDetails.SourceJobRunID, report.PUDetails.InPU, report.PUDetails.PU, report.StatusDetail.Status, fmt.Sprint(report.StatusDetail.StatusCode), report.StatusDetail.EventName, report.StatusDetail.EventType}
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

func (r *HandleT) mainLoop(ctx context.Context, clientName string) {
	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	tags := r.getTags(clientName)
	mainLoopTimer := stats.Default.NewTaggedStat(StatReportingMainLoopTime, stats.TimerType, tags)
	getReportsTimer := stats.Default.NewTaggedStat(StatReportingGetReportsTime, stats.TimerType, tags)
	getReportsCount := stats.Default.NewTaggedStat(StatReportingGetReportsCount, stats.HistogramType, tags)
	getAggregatedReportsTimer := stats.Default.NewTaggedStat(StatReportingGetAggregatedReportsTime, stats.TimerType, tags)
	getAggregatedReportsCount := stats.Default.NewTaggedStat(StatReportingGetAggregatedReportsCount, stats.HistogramType, tags)

	r.getMinReportedAtQueryTime = stats.Default.NewTaggedStat(StatReportingGetMinReportedAtQueryTime, stats.TimerType, tags)
	r.getReportsQueryTime = stats.Default.NewTaggedStat(StatReportingGetReportsQueryTime, stats.TimerType, tags)
	r.requestLatency = stats.Default.NewTaggedStat(StatReportingHttpReqLatency, stats.TimerType, tags)
	for {
		if ctx.Err() != nil {
			r.log.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
			return
		}
		requestChan := make(chan struct{}, maxConcurrentRequests)
		loopStart := time.Now()
		currentMs := time.Now().UTC().Unix() / 60

		getReportsStart := time.Now()
		reports, reportedAt := r.getReports(currentMs, clientName)
		getReportsTimer.Since(getReportsStart)
		getReportsCount.Observe(float64(len(reports)))
		if len(reports) == 0 {
			select {
			case <-ctx.Done():
				r.log.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
				return
			case <-time.After(r.sleepInterval):
			}
			continue
		}

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
				err := r.sendMetric(errCtx, netClient, clientName, metricToSend)
				<-requestChan
				return err
			})
		}

		err := errGroup.Wait()
		if err == nil {
			sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_at = %d`, ReportsTable, reportedAt)
			dbHandle, err := r.getDBHandle(clientName)
			if err != nil {
				panic(err)
			}
			_, err = dbHandle.Exec(sqlStatement)
			if err != nil {
				r.log.Errorf(`[ Reporting ]: Error deleting local reports from %s: %v`, ReportsTable, err)
			}
		}

		mainLoopTimer.Since(loopStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.mainLoopSleepInterval):
		}
	}
}

func (r *HandleT) sendMetric(ctx context.Context, netClient *http.Client, clientName string, metric *types.Metric) error {
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
		httpStatTags := r.getTags(clientName)
		httpStatTags["status"] = strconv.Itoa(resp.StatusCode)
		stats.Default.NewTaggedStat(StatReportingHttpReq, stats.CountType, httpStatTags).Count(1)

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

func (r *HandleT) IsPIIReportingDisabled(workspaceID string) bool {
	<-r.init
	return r.piiReportingSettings[workspaceID]
}

func (r *HandleT) Report(metrics []*types.PUReportedMetric, txn *sql.Tx) {
	if len(metrics) == 0 {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(ReportsTable, "workspace_id", "namespace", "instance_id", "source_definition_id", "source_category", "source_id", "destination_definition_id", "destination_id", "source_task_run_id", "source_job_id", "source_job_run_id", "in_pu", "pu", "reported_at", "status", "count", "terminal_state", "initial_state", "status_code", "sample_response", "sample_event", "event_name", "event_type"))
	if err != nil {
		_ = txn.Rollback()
		panic(err)
	}
	defer func() { _ = stmt.Close() }()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := r.getWorkspaceID(metric.ConnectionDetails.SourceID)
		metric := *metric
		if r.IsPIIReportingDisabled(workspaceID) {
			metric = transformMetricForPII(metric, getPIIColumnsToExclude())
		}

		_, err = stmt.Exec(workspaceID, r.namespace, r.instanceID, metric.ConnectionDetails.SourceDefinitionId, metric.ConnectionDetails.SourceCategory, metric.ConnectionDetails.SourceID, metric.ConnectionDetails.DestinationDefinitionId, metric.ConnectionDetails.DestinationID, metric.ConnectionDetails.SourceTaskRunID, metric.ConnectionDetails.SourceJobID, metric.ConnectionDetails.SourceJobRunID, metric.PUDetails.InPU, metric.PUDetails.PU, reportedAt, metric.StatusDetail.Status, metric.StatusDetail.Count, metric.PUDetails.TerminalPU, metric.PUDetails.InitialPU, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse, string(metric.StatusDetail.SampleEvent), metric.StatusDetail.EventName, metric.StatusDetail.EventType)
		if err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
}

func (r *HandleT) getTags(clientName string) stats.Tags {
	return stats.Tags{
		"namespace":   r.namespace,
		"workspaceId": r.workspaceID,
		"instanceId":  r.instanceID,
		"clientName":  clientName,
	}
}

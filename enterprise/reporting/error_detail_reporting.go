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
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

const (
	ErrorDetailReportsTable = "error_detail_reports"
	MutexMapDelimitter      = "::"
)

var ErrorDetailReportsColumns = []string{
	"workspace_id",
	"namespace",
	"instance_id",
	"source_definition_id",
	"source_id",
	"destination_definition_id",
	"destination_id",
	"pu", // reportedBy
	"reported_at",
	"count",
	"status_code",
	"event_type",
	"error_code",
	"error_message",
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
	sleepInterval             time.Duration
	mainLoopSleepInterval     time.Duration
	maxConcurrentRequests     int
	workspaceIDForSourceIDMap map[string]string
	srcWspMutex               sync.RWMutex
	httpClient                *http.Client
	clientsMapLock            sync.RWMutex

	minReportedAtQueryTime      stats.Measurement
	errorDetailReportsQueryTime stats.Measurement
	edReportingRequestLatency   stats.Measurement
}

type errorDetails struct {
	ErrorCode    string
	ErrorMessage string
}

func NewEdReporterFromEnvConfig() *ErrorDetailReporter {
	var sleepInterval, mainLoopSleepInterval time.Duration
	var maxConcurrentRequests int
	tr := &http.Transport{}
	reportingServiceURL := config.GetString("REPORTING_URL", "https://reporting.rudderstack.com/")
	reportingServiceURL = strings.TrimSuffix(reportingServiceURL, "/")

	netClient := &http.Client{Transport: tr, Timeout: config.GetDuration("HttpClient.reporting.timeout", 60, time.Second)}
	config.RegisterDurationConfigVariable(5, &mainLoopSleepInterval, true, time.Second, "Reporting.mainLoopSleepInterval")
	config.RegisterDurationConfigVariable(30, &sleepInterval, true, time.Second, "Reporting.sleepInterval")
	config.RegisterIntConfigVariable(32, &maxConcurrentRequests, true, 1, "Reporting.maxConcurrentRequests")

	return &ErrorDetailReporter{
		reportingServiceURL:   reportingServiceURL,
		Table:                 ErrorDetailReportsTable,
		log:                   logger.NewLogger().Child("enterprise").Child("error-detail-reporting"),
		sleepInterval:         sleepInterval,
		mainLoopSleepInterval: mainLoopSleepInterval,
		maxConcurrentRequests: maxConcurrentRequests,
		httpClient:            netClient,

		namespace:  config.GetKubeNamespace(),
		instanceID: config.GetString("INSTANCE_ID", "1"),
		region:     config.GetString("region", ""),

		init:                      make(chan struct{}),
		workspaceIDForSourceIDMap: make(map[string]string),
		clients:                   make(map[string]*types.Client),
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
		panic(err)
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
	edRep.log.Info("[[ Error Detail Reporting ]] Setting up reporting handler")

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
		edRep.srcWspMutex.Lock()
		edRep.workspaceID = newWorkspaceID
		edRep.workspaceIDForSourceIDMap = newWorkspaceIDForSourceIDMap
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
	edRep.log.Info("[ErrorDetailReport] Report method called\n")
	if len(metrics) == 0 {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(edRep.Table, ErrorDetailReportsColumns...))
	if err != nil {
		_ = txn.Rollback()
		panic(err)
	}
	defer func() { _ = stmt.Close() }()

	reportedAt := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		workspaceID := edRep.getWorkspaceID(metric.ConnectionDetails.SourceID)
		metric := *metric

		// TODO: Think through with team on if PII exclusion is required
		// if edRep.IsPIIReportingDisabled(workspaceID) {
		// 	metric = transformMetricForPII(metric, getPIIColumnsToExclude())
		// }

		// extract error-message & error-code
		errDets := edRep.extractErrorDetails(metric.StatusDetail.SampleResponse)
		_, err = stmt.Exec(
			workspaceID,
			edRep.namespace,
			edRep.instanceID,
			metric.ConnectionDetails.SourceDefinitionId,
			metric.ConnectionDetails.SourceID,
			metric.ConnectionDetails.DestinationDefinitionId,
			metric.ConnectionDetails.DestinationID,
			metric.PUDetails.PU,
			reportedAt,
			metric.StatusDetail.Count,
			metric.StatusDetail.StatusCode,
			metric.StatusDetail.EventType,
			errDets.ErrorCode,
			errDets.ErrorMessage,
		)
		if err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
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

func (edRep *ErrorDetailReporter) extractErrorDetails(sampleResponse string) errorDetails {
	// TODO: to extract information
	// Transformer throws structured error -- tf <--> srv --- easy
	// Transformer throws error -- but I only get strings in sampleResponse -- easy
	// Router delivery -- destination schema(very random) -- hard
	if !gjson.Valid(sampleResponse) {
		return errorDetails{ErrorMessage: sampleResponse}
	}
	// Preliminary checks for error responses from transformer(tf proxy, router tf, process tf, batch tf)
	prelimResults := gjson.GetMany(sampleResponse, []string{"response.error", "response.message"}...)
	for _, prelimResult := range prelimResults {
		if prelimResult.Exists() && prelimResult.Type == gjson.String {
			return errorDetails{ErrorMessage: prelimResult.String()}
		}
	}
	// TODO: Incorportate iterative exhaustive search algo for possible message fields

	return errorDetails{}
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
		"namespace":   edRep.namespace,
		"clientName":  clientName,
		"instanceId":  edRep.instanceID,
	}
}

// Sending metrics to Reporting service --- STARTS
func (edRep *ErrorDetailReporter) mainLoop(ctx context.Context, clientName string) {
	tags := edRep.getTags(clientName)

	mainLoopTimer := stats.Default.NewTaggedStat("ed_main_loop_time", stats.TimerType, tags)
	getReportsTimer := stats.Default.NewTaggedStat("ed_get_reports_time", stats.TimerType, tags)
	aggregateTimer := stats.Default.NewTaggedStat("ed_aggregate_time", stats.TimerType, tags)

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
		requestChan := make(chan struct{}, maxConcurrentRequests)
		loopStart := time.Now()
		currentMs := time.Now().UTC().Unix() / 60

		getReportsStart := time.Now()
		reports, reportedAt := edRep.getReports(currentMs, clientName)
		getReportsTimer.Since(getReportsStart)
		// getReportsCount.Observe(float64(len(reports)))
		if len(reports) == 0 {
			select {
			case <-ctx.Done():
				edRep.log.Infof("stopping mainLoop for client %s : %s", clientName, ctx.Err())
				return
			case <-time.After(edRep.sleepInterval):
			}
			continue
		}

		aggregationStart := time.Now()
		metrics := edRep.aggregate(reports)
		aggregateTimer.Since(aggregationStart)
		// getAggregatedReportsCount.Observe(float64(len(metrics)))

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
				<-requestChan
				return err
			})
		}

		err := errGroup.Wait()
		if err == nil {
			sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_at = %d`, ErrorDetailReportsTable, reportedAt)
			dbHandle, err := edRep.getDBHandle(clientName)
			if err != nil {
				panic(err)
			}
			deleteReportsStart := time.Now()
			_, err = dbHandle.Exec(sqlStatement)
			errorDetailReportsDeleteQueryTimer.Since(deleteReportsStart)
			if err != nil {
				edRep.log.Errorf(`[ Error Detail Reporting ]: Error deleting local reports from %s: %v`, ErrorDetailReportsTable, err)
			}
		}

		mainLoopTimer.Since(loopStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(edRep.mainLoopSleepInterval):
		}
	}
}

func (edRep *ErrorDetailReporter) getReports(currentMs int64, clientName string) ([]*types.EDReportsDB, int64) {
	sqlStatement := fmt.Sprintf(`SELECT reported_at FROM %s WHERE reported_at < %d ORDER BY reported_at ASC LIMIT 1`, ErrorDetailReportsTable, currentMs)
	var queryMin sql.NullInt64
	dbHandle, err := edRep.getDBHandle(clientName)
	if err != nil {
		panic(err)
	}

	// queryStart := time.Now()
	err = dbHandle.QueryRow(sqlStatement).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	// edRep.getMinReportedAtQueryTime.Since(queryStart)
	if !queryMin.Valid {
		return nil, 0
	}
	edSelColumns := strings.Join(ErrorDetailReportsColumns, ", ")
	sqlStatement = fmt.Sprintf(`SELECT %s FROM %s WHERE reported_at = %d`, edSelColumns, ErrorDetailReportsTable, queryMin.Int64)
	edRep.log.Infof("[EdRep] sql statement: %s\n", sqlStatement)
	var rows *sql.Rows
	// queryStart = time.Now()
	rows, err = dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	// r.getReportsQueryTime.Since(queryStart)
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
		)
		if err != nil {
			panic(err)
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
		}
		return strings.Join(keys, "::")
	})
	var edReportingMetrics []*types.EDMetric
	for _, reports := range groupedReports {
		fstRep := reports[0]
		edRepSchema := types.EDMetric{
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: fstRep.WorkspaceID,
				Namespace:   fstRep.Namespace,
				InstanceID:  fstRep.InstanceID,
			},
			EDConnectionDetails: types.EDConnectionDetails{
				DestinationID:           fstRep.DestinationID,
				DestinationDefinitionId: fstRep.DestinationDefinitionId,
				SourceID:                fstRep.SourceID,
				SourceDefinitionId:      fstRep.SourceDefinitionId,
			},
			PU: fstRep.PU,
		}
		errs := lo.Map(reports, func(rep *types.EDReportsDB, _ int) types.EDErrorDetails {
			return types.EDErrorDetails{
				StatusCode:   rep.StatusCode,
				ErrorCode:    rep.ErrorCode,
				ErrorMessage: rep.ErrorMessage,
				EventType:    rep.EventType,
			}
		})
		// edRepSchema.errors = errs
		edRepSchema.Errors = errs
		edReportingMetrics = append(edReportingMetrics, &edRepSchema)
	}
	return edReportingMetrics
}

func (edRep *ErrorDetailReporter) sendMetric(ctx context.Context, clientName string, metric *types.EDMetric) error {
	payload, err := json.Marshal(metric)
	if err != nil {
		panic(err)
	}
	operation := func() error {
		uri := fmt.Sprintf("%s/recordErrors", edRep.reportingServiceURL)
		req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewBuffer(payload))
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
			edRep.log.Error(err.Error())
			return err
		}

		edRep.edReportingRequestLatency.Since(httpRequestStart)
		httpStatTags := edRep.getTags(clientName)
		httpStatTags["status"] = strconv.Itoa(resp.StatusCode)
		stats.Default.NewTaggedStat("error_detail_reporting_http_request", stats.CountType, httpStatTags).Increment()

		defer func() { httputil.CloseResponse(resp) }()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			edRep.log.Error(err.Error())
			return err
		}

		if !isMetricPosted(resp.StatusCode) {
			err = fmt.Errorf(`received response: statusCode:%d error:%v`, resp.StatusCode, string(respBody))
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

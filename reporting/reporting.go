package reporting

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/thoas/go-funk"
)

const REPORTS_TABLE = "reports"

const (
	CORE_CLIENT      = "core"
	WAREHOUSE_CLIENT = "warehouse"
)

var (
	clients             map[string]*Client
	clientsMapLock      sync.RWMutex
	dbHandles           map[string]*sql.DB
	dbHandlesMapLock    sync.RWMutex
	pkgLogger           logger.LoggerI
	reportingServiceURL string
)

type Config struct {
	ClientName  string
	Namespace   string
	WorksapceID string
	InstanceID  string
	ConnInfo    string
}

var (
	SuccessStatus   = jobsdb.Succeeded.State
	AbortStatus     = jobsdb.Aborted.State
	WaitingStatus   = jobsdb.Waiting.State
	ThrottledStatus = jobsdb.Throttled.State
	FailStatus      = jobsdb.Failed.State
	DiffStatus      = "diff"

	//Module names
	GATEWAY          = "gateway"
	USER_TRANSFORMER = "user_transformer"
	DEST_TRANSFORMER = "dest_transformer"
	ROUTER           = "router"
	BATCH_ROUTER     = "batch_router"
	WAREHOUSE        = "warehouse"
)

type StatusDetail struct {
	Status         string          `json:"state"`
	Count          int64           `json:"count"`
	StatusCode     int             `json:"status_code"`
	SampleResponse string          `json:"sample_response"`
	SampleEvent    json.RawMessage `json:"sample_event"`
}

type ReportByStatus struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetail *StatusDetail
}

type InstanceDetails struct {
	WorksapceID string `json:"workspaceId"`
	Namespace   string `json:"namespace"`
	InstanceID  string `json:"instanceId"`
}

type ReportMetadata struct {
	ReportedAt int64 `json:"reportedAt"`
}

type Metric struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetails []*StatusDetail `json:"reports"`
}

type ConnectionDetails struct {
	SourceID        string `json:"sourceId"`
	DestinationID   string `json:"destinationId"`
	SourceBatchID   string `json:"sourceBatchId"`
	SourceTaskID    string `json:"sourceTaskId"`
	SourceTaskRunID string `json:"sourceTaskRunId"`
	SourceJobID     string `json:"sourceJobId"`
	SourceJobRunID  string `json:"sourceJobRunId"`
}
type PUDetails struct {
	InPU       string `json:"inReportedBy"`
	PU         string `json:"reportedBy"`
	TerminalPU bool   `json:"terminalState"`
	InitialPU  bool   `json:"initialState"`
}

type PUReportedMetric struct {
	ConnectionDetails
	PUDetails
	StatusDetail *StatusDetail
}

type Client struct {
	Config
}

func init() {
	pkgLogger = logger.NewLogger().Child("reporting")
	reportingServiceURL = config.GetString("Reporting.serviceURL", "http://a02013f3ae48a4436aec4b2aa3d1159c-1237165542.us-east-1.elb.amazonaws.com/metrics")
	clients = make(map[string]*Client)
	dbHandles = make(map[string]*sql.DB)
}

func New(config Config) *Client {
	cl := &Client{}
	return cl
}

func setupTable(dbHandle *sql.DB) error {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		workspace_id VARCHAR(64) NOT NULL,
		namespace VARCHAR(64) NOT NULL,
		instance_id VARCHAR(64) NOT NULL,
		source_id VARCHAR(64),
		destination_id VARCHAR(64) NOT NULL,
		source_batch_id VARCHAR(64),
		source_task_id VARCHAR(64),
		source_task_run_id VARCHAR(64),
		source_job_id VARCHAR(64),
		source_job_run_id VARCHAR(64),
		in_pu VARCHAR(64),
		pu VARCHAR(64),
		reported_at BIGINT NOT NULL,
		status_code INT,
		sample_response TEXT,
		sample_event JSONB,
		status VARCHAR(64) NOT NULL,
		count BIGINT,
		terminal_state BOOLEAN,
		initial_state BOOLEAN
		);`, REPORTS_TABLE,
	)

	// TODO: Create indexes also
	_, err := dbHandle.Exec(sqlStatement)
	return err
}

func Setup(c Config, backendConfig backendconfig.BackendConfig) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)

	for {
		beconfig := <-ch
		sources := beconfig.Data.(backendconfig.ConfigT)
		if sources.WorkspaceID == "" {
			continue
		}

		dbHandle, err := sql.Open("postgres", c.ConnInfo)
		if err != nil {
			panic(err)
		}
		err = setupTable(dbHandle)
		if err != nil {
			panic(err)
		}
		if c.ClientName == "" {
			c.ClientName = CORE_CLIENT
		}

		dbHandlesMapLock.Lock()
		dbHandles[c.ClientName] = dbHandle
		dbHandlesMapLock.Unlock()

		c.WorksapceID = sources.WorkspaceID
		c.Namespace = config.GetKubeNamespace()
		c.InstanceID = config.GetEnv("INSTANCE_ID", "1")
		// if c.ClientName == "" {
		// 	c.ClientName = CORE_CLIENT
		// }
		clientsMapLock.Lock()
		clients[c.ClientName] = &Client{Config: c}
		clientsMapLock.Unlock()
		rruntime.Go(func() {
			mainLoop(c.ClientName)
		})
		break
	}
}

func WaitForSetup(clientName string) {
	for {
		if GetClient(clientName) == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
}

func GetClient(clientName string) *Client {
	clientsMapLock.RLock()
	defer clientsMapLock.RUnlock()
	if c, ok := clients[clientName]; ok {
		return c
	}
	coreClient, ok := clients[CORE_CLIENT]
	if !ok {
		return nil
	}
	return coreClient
}

func getDBHandle(clientName string) *sql.DB {
	dbHandlesMapLock.RLock()
	defer dbHandlesMapLock.RUnlock()
	if h, ok := dbHandles[clientName]; ok {
		return h
	}
	handle, ok := dbHandles[CORE_CLIENT]
	if !ok {
		panic(fmt.Sprintf("getDBHandle() returned nil for client: %s", clientName))
	}
	return handle
}

func CreateConnectionDetail(sid, did, sbid, stid, strid, sjid, sjrid string) *ConnectionDetails {
	return &ConnectionDetails{SourceID: sid,
		DestinationID:   did,
		SourceBatchID:   sbid,
		SourceTaskID:    stid,
		SourceTaskRunID: strid,
		SourceJobID:     sjid,
		SourceJobRunID:  sjrid}
}

func CreateStatusDetail(status string, count int64, code int, resp string, event json.RawMessage) *StatusDetail {
	return &StatusDetail{
		Status:         status,
		Count:          count,
		StatusCode:     code,
		SampleResponse: resp,
		SampleEvent:    event}
}

func CreatePUDetails(inPU, pu string, terminalPU, initialPU bool) *PUDetails {
	return &PUDetails{
		InPU:       inPU,
		PU:         pu,
		TerminalPU: terminalPU,
		InitialPU:  initialPU,
	}
}

func GetStatus(jobState string) string {
	if jobState == jobsdb.Succeeded.State {
		return SuccessStatus
	}
	if jobState == jobsdb.Failed.State {
		return FailStatus
	}
	if jobState == jobsdb.Aborted.State {
		return AbortStatus
	}
	if jobState == jobsdb.Waiting.State {
		return WaitingStatus
	}
	if jobState == jobsdb.Throttled.State {
		return ThrottledStatus
	}

	return FailStatus
}

func AssertSameKeys(m1 map[string]*ConnectionDetails, m2 map[string]*StatusDetail) {
	if len(m1) != len(m2) {
		panic("maps length don't match") //TODO improve msg
	}
	for k := range m1 {
		if _, ok := m2[k]; !ok {
			panic("key in map1 not found in map2") //TODO improve msg
		}
	}
}

func getReports(current_ms int64, clientName string) (reports []*ReportByStatus, reportedAt int64) {
	sqlStatement := fmt.Sprintf(`SELECT reported_at FROM %s WHERE reported_at < %d ORDER BY reported_at ASC LIMIT 1`, REPORTS_TABLE, current_ms)
	var queryMin sql.NullInt64
	err := getDBHandle(clientName).QueryRow(sqlStatement).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	if !queryMin.Valid {
		return nil, 0
	}

	sqlStatement = fmt.Sprintf(`SELECT workspace_id, namespace, instance_id, source_id, destination_id, source_batch_id, source_task_id, source_task_run_id, source_job_id, source_job_run_id, in_pu, pu, reported_at, status, count, terminal_state, initial_state, status_code, sample_response, sample_event FROM %s WHERE reported_at = %d`, REPORTS_TABLE, queryMin.Int64)
	var rows *sql.Rows
	rows, err = getDBHandle(clientName).Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var metricReports []*ReportByStatus
	for rows.Next() {
		metricReport := ReportByStatus{StatusDetail: &StatusDetail{}}
		err = rows.Scan(&metricReport.InstanceDetails.WorksapceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID, &metricReport.ConnectionDetails.SourceID, &metricReport.ConnectionDetails.DestinationID, &metricReport.ConnectionDetails.SourceBatchID, &metricReport.ConnectionDetails.SourceTaskID, &metricReport.ConnectionDetails.SourceTaskRunID, &metricReport.ConnectionDetails.SourceJobID, &metricReport.ConnectionDetails.SourceJobRunID, &metricReport.PUDetails.InPU, &metricReport.PUDetails.PU, &metricReport.ReportedAt, &metricReport.StatusDetail.Status, &metricReport.StatusDetail.Count, &metricReport.PUDetails.TerminalPU, &metricReport.PUDetails.InitialPU, &metricReport.StatusDetail.StatusCode, &metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent)
		if err != nil {
			panic(err)
		}
		metricReports = append(metricReports, &metricReport)
	}

	return metricReports, queryMin.Int64
}

func getAggregatedReports(reports []*ReportByStatus) []*Metric {
	metricsByGroup := map[string]*Metric{}

	reportIdentifier := func(report *ReportByStatus) string {
		groupingIdentifiers := []string{report.InstanceDetails.WorksapceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID, report.ConnectionDetails.SourceID, report.ConnectionDetails.DestinationID, report.ConnectionDetails.SourceBatchID, report.ConnectionDetails.SourceTaskID, report.ConnectionDetails.SourceTaskRunID, report.ConnectionDetails.SourceJobID, report.ConnectionDetails.SourceJobRunID, report.PUDetails.InPU, report.PUDetails.PU, report.StatusDetail.Status, fmt.Sprint(report.StatusDetail.StatusCode)}
		return strings.Join(groupingIdentifiers, `::`)
	}

	for _, report := range reports {
		identifier := reportIdentifier(report)
		if _, ok := metricsByGroup[identifier]; !ok {
			metricsByGroup[identifier] = &Metric{
				InstanceDetails: InstanceDetails{
					WorksapceID: report.WorksapceID,
					Namespace:   report.Namespace,
					InstanceID:  report.InstanceID,
				},
				ConnectionDetails: ConnectionDetails{
					SourceID:        report.SourceID,
					DestinationID:   report.DestinationID,
					SourceBatchID:   report.SourceBatchID,
					SourceTaskID:    report.SourceTaskID,
					SourceTaskRunID: report.SourceTaskRunID,
					SourceJobID:     report.SourceJobID,
					SourceJobRunID:  report.SourceJobRunID,
				},
				PUDetails: PUDetails{
					InPU:       report.InPU,
					PU:         report.PU,
					TerminalPU: report.TerminalPU,
					InitialPU:  report.InitialPU,
				},
				ReportMetadata: ReportMetadata{
					ReportedAt: report.ReportedAt,
				},
			}
		}
		statusDetailInterface := funk.Find(metricsByGroup[identifier].StatusDetails, func(i *StatusDetail) bool {
			return i.Status == report.StatusDetail.Status && i.StatusCode == report.StatusDetail.StatusCode
		})
		if statusDetailInterface == nil {
			metricsByGroup[identifier].StatusDetails = append(metricsByGroup[identifier].StatusDetails, &StatusDetail{
				Status:         report.StatusDetail.Status,
				StatusCode:     report.StatusDetail.StatusCode,
				Count:          report.StatusDetail.Count,
				SampleResponse: report.StatusDetail.SampleResponse,
				SampleEvent:    report.StatusDetail.SampleEvent,
			})
			continue
		}
		statusDetail := statusDetailInterface.(*StatusDetail)
		statusDetail.Count += report.StatusDetail.Count
		statusDetail.SampleResponse = report.StatusDetail.SampleResponse
		statusDetail.SampleEvent = report.StatusDetail.SampleEvent
	}

	var values []*Metric
	for _, val := range metricsByGroup {
		values = append(values, val)
	}
	return values
}

func mainLoop(clientName string) {
	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr}
	for {
		allReported := true
		currentMs := time.Now().UTC().UnixNano() / int64(time.Millisecond)
		reports, reportedAt := getReports(currentMs, clientName)
		if reports == nil || len(reports) == 0 {
			time.Sleep(30 * time.Second)
			continue
		}
		metrics := getAggregatedReports(reports)
		for _, metric := range metrics {
			payload, err := json.Marshal(metric)
			if err != nil {
				panic(err)
			}
			operation := func() error {
				uri := reportingServiceURL
				resp, err := netClient.Post(uri, "application/json; charset=utf-8",
					bytes.NewBuffer(payload))
				if resp != nil && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
					respBody, _ := ioutil.ReadAll(resp.Body)
					defer resp.Body.Close()
					err = errors.New(fmt.Sprintf(`Received reponse: statusCode:%d error:%v`, resp.StatusCode, string(respBody)))
				}
				return err
			}

			err = backoff.RetryNotify(operation, backoff.NewExponentialBackOff(), func(err error, t time.Duration) {
				pkgLogger.Errorf(`[ Reporting ]: Error reporting to service: %v`, err)
			})
			if err != nil {
				pkgLogger.Errorf(`[ Reporting ]: Error making request to reporting service: %v`, err)
				allReported = false
				break
			}
		}
		if allReported {
			sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_at = %d`, REPORTS_TABLE, reportedAt)
			_, err := getDBHandle(clientName).Exec(sqlStatement)
			if err != nil {
				pkgLogger.Errorf(`[ Reporting ]: Error deleting local reports from %s: %v`, REPORTS_TABLE, err)
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func (client *Client) Report(metrics []*PUReportedMetric, txn *sql.Tx) {
	stmt, err := txn.Prepare(pq.CopyIn(REPORTS_TABLE, "workspace_id", "namespace", "instance_id", "source_id", "destination_id", "source_batch_id", "source_task_id", "source_task_run_id", "source_job_id", "source_job_run_id", "in_pu", "pu", "reported_at", "status", "count", "terminal_state", "initial_state", "status_code", "sample_response", "sample_event"))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	reportedAt := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	for _, metric := range metrics {
		_, err = stmt.Exec(client.Config.WorksapceID, client.Config.Namespace, client.Config.WorksapceID, metric.ConnectionDetails.SourceID, metric.ConnectionDetails.DestinationID, metric.ConnectionDetails.SourceBatchID, metric.ConnectionDetails.SourceTaskID, metric.ConnectionDetails.SourceTaskRunID, metric.ConnectionDetails.SourceJobID, metric.ConnectionDetails.SourceJobRunID, metric.PUDetails.InPU, metric.PUDetails.PU, reportedAt, metric.StatusDetail.Status, metric.StatusDetail.Count, metric.PUDetails.TerminalPU, metric.PUDetails.InitialPU, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse, metric.StatusDetail.SampleEvent)
		if err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
}

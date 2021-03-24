package reporting

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/thoas/go-funk"
)

const REPORTS_TABLE = "reports"

var (
	client    *Client
	dbHandle  *sql.DB
	pkgLogger logger.LoggerI
)

type Config struct {
	Namespace   string
	WorksapceID string
	InstanceID  string
	ConnInfo    string
}

const (
	SuccessStatus   = jobsdb.Succeeded.State
	AbortStatus     = jobsdb.Aborted.State
	WaitingStatus   = jobsdb.Waiting.State
	ThrottledStatus = jobsdb.Throttled.State
	FailStatus      = jobsdb.Failed.State
	DiffStatus      = "diff"
)

type StatusDetail struct {
	Status         string
	Count          int64
	StatusCode     int
	SampleResponse string
	SampleEvent    json.RawMessage
}

type ReportByStatus struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetail *StatusDetail
}

type InstanceDetails struct {
	WorksapceID string
	Namespace   string
	InstanceID  string
}

type ReportMetadata struct {
	ReportedMin int64
}

type Metric struct {
	InstanceDetails
	ConnectionDetails
	PUDetails
	ReportMetadata
	StatusDetails []*StatusDetail
}

type ConnectionDetails struct {
	SourceID      string
	DestinationID string
	BatchID       string
}
type PUDetails struct {
	InPU       string
	PU         string
	TerminalPU bool
	InitialPU  bool
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
}

func New(config Config) *Client {
	cl := &Client{}
	return cl
}

func setupTable() error {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		workspace_id VARCHAR(64) NOT NULL,
		namespace VARCHAR(64) NOT NULL,
		instance_id VARCHAR(64) NOT NULL,
		source_id VARCHAR(64),
		destination_id VARCHAR(64) NOT NULL,
		batch_id VARCHAR(64),
		in_pu VARCHAR(64),
		pu VARCHAR(64),
		reported_min BIGINT NOT NULL,
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
	backendConfig.WaitForConfig()
	workspaceConfig, _ := backendConfig.Get()
	var err error
	dbHandle, err = sql.Open("postgres", c.ConnInfo)
	if err != nil {
		panic(err)
	}
	err = setupTable()
	if err != nil {
		panic(err)
	}
	c.WorksapceID = workspaceConfig.WorkspaceID
	c.Namespace = config.GetKubeNamespace()
	c.InstanceID = config.GetEnv("INSTANCE_ID", "1")
	client = &Client{Config: c}
	rruntime.Go(func() {
		mainLoop()
	})
}

func GetClient() *Client {
	return client
}

func CreateConnectionDetail(sid, did, bid string) *ConnectionDetails {
	return &ConnectionDetails{SourceID: sid,
		DestinationID: did,
		BatchID:       bid}
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

func getReports(current_min int64) (reports []*ReportByStatus, reportedMin int64) {
	sqlStatement := fmt.Sprintf(`SELECT reported_min FROM %s WHERE reported_min < %d ORDER BY reported_min ASC LIMIT 1`, REPORTS_TABLE, current_min)
	var queryMin sql.NullInt64
	err := dbHandle.QueryRow(sqlStatement).Scan(&queryMin)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	if !queryMin.Valid {
		return nil, 0
	}

	sqlStatement = fmt.Sprintf(`SELECT workspace_id, namespace, instance_id, source_id, destination_id, batch_id, in_pu, pu, reported_min, status, count, terminal_state, initial_state, status_code, sample_response, sample_event FROM %s WHERE reported_min = %d`, REPORTS_TABLE, queryMin.Int64)
	var rows *sql.Rows
	rows, err = dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var metricReports []*ReportByStatus
	for rows.Next() {
		metricReport := ReportByStatus{StatusDetail: &StatusDetail{}}
		err = rows.Scan(&metricReport.InstanceDetails.WorksapceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID, &metricReport.ConnectionDetails.SourceID, &metricReport.ConnectionDetails.DestinationID, &metricReport.ConnectionDetails.BatchID, &metricReport.PUDetails.InPU, &metricReport.PUDetails.PU, &metricReport.ReportedMin, &metricReport.StatusDetail.Status, &metricReport.StatusDetail.Count, &metricReport.PUDetails.TerminalPU, &metricReport.PUDetails.InitialPU, &metricReport.StatusDetail.StatusCode, &metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent)
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
		groupingIdentifiers := []string{report.InstanceDetails.WorksapceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID, report.ConnectionDetails.SourceID, report.ConnectionDetails.DestinationID, report.ConnectionDetails.BatchID, report.PUDetails.InPU, report.PUDetails.PU, report.StatusDetail.Status, fmt.Sprint(report.StatusDetail.StatusCode)}
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
					SourceID:      report.SourceID,
					DestinationID: report.DestinationID,
					BatchID:       report.BatchID,
				},
				PUDetails: PUDetails{
					InPU: report.InPU,
					PU:   report.PU,
				},
				ReportMetadata: ReportMetadata{
					ReportedMin: report.ReportedMin,
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

func mainLoop() {
	tr := &http.Transport{}
	netClient := &http.Client{Transport: tr}
	for {
		allReported := true
		currentMin := time.Now().UTC().Unix() / 60
		reports, reportedMin := getReports(currentMin)
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
				uri := "https://webhook.site/f44086e2-bbd8-4a2b-b41d-9f7c635783db"
				_, err := netClient.Post(uri, "application/json; charset=utf-8",
					bytes.NewBuffer(payload))
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
			sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE reported_min = %d`, REPORTS_TABLE, reportedMin)
			_, err := dbHandle.Exec(sqlStatement)
			if err != nil {
				pkgLogger.Errorf(`[ Reporting ]: Error deleting local reports from %s: %v`, REPORTS_TABLE, err)
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func (client *Client) Report(metrics []*PUReportedMetric, txn *sql.Tx) {
	stmt, err := txn.Prepare(pq.CopyIn(REPORTS_TABLE, "workspace_id", "namespace", "instance_id", "source_id", "destination_id", "batch_id", "in_pu", "pu", "reported_min", "status", "count", "terminal_state", "initial_state", "status_code", "sample_response", "sample_event"))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	reported_min := time.Now().UTC().Unix() / 60
	for _, metric := range metrics {
		_, err = stmt.Exec(client.Config.WorksapceID, client.Config.Namespace, client.Config.WorksapceID, metric.ConnectionDetails.SourceID, metric.ConnectionDetails.DestinationID, metric.ConnectionDetails.BatchID, metric.PUDetails.InPU, metric.PUDetails.PU, reported_min, metric.StatusDetail.Status, metric.StatusDetail.Count, metric.PUDetails.TerminalPU, metric.PUDetails.InitialPU, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse, metric.StatusDetail.SampleEvent)
		if err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
}

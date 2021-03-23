package reporting

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
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

// const (
// 	SuccessCount = "success_count"
// 	AbortCount   = "abort_count"
// 	FailCount    = "fail_count"
// 	DiffCount    = "diff_count"
// )

const (
	SuccessStatus = "success_count"
	AbortStatus   = "abort_count"
	FailStatus    = "fail_count"
	DiffStatus    = "diff_count"
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
	InPU         string
	PU           string
	TerminalStae bool
	InitialState bool
}

type PUReportedMetric struct {
	ConnectionDetails
	PUDetails
	StatusDetail *StatusDetail
}

type Client struct {
	Config
	// Report(reports []Report, txn *sql.Tx) error
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
	x, _ := backendConfig.Get()
	fmt.Println("*****")
	fmt.Println(x.WorkspaceID)
	fmt.Println("*****")
	var err error
	dbHandle, err = sql.Open("postgres", c.ConnInfo)
	if err != nil {
		panic(err)
	}
	err = setupTable()
	if err != nil {
		panic(err)
	}
	c.WorksapceID = x.WorkspaceID
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
		var metricReport ReportByStatus
		err = rows.Scan(&metricReport.InstanceDetails.WorksapceID, &metricReport.InstanceDetails.Namespace, &metricReport.InstanceDetails.InstanceID, &metricReport.ConnectionDetails.SourceID, &metricReport.ConnectionDetails.DestinationID, &metricReport.ConnectionDetails.BatchID, &metricReport.PUDetails.InPU, &metricReport.PUDetails.PU, &metricReport.ReportedMin, &metricReport.StatusDetail.Status, &metricReport.StatusDetail.Count, &metricReport.PUDetails.TerminalStae, &metricReport.PUDetails.InitialState, &metricReport.StatusDetail.StatusCode, &metricReport.StatusDetail.SampleResponse, &metricReport.StatusDetail.SampleEvent)
		if err != nil {
			panic(err)
		}
		metricReports = append(metricReports, &metricReport)
	}

	return metricReports, queryMin.Int64
}

func getAggregatedReports(reports []*ReportByStatus) []*Metric {
	var x map[string]*Metric

	reportIdentifier := func(report *ReportByStatus) string {
		x := []string{report.InstanceDetails.WorksapceID, report.InstanceDetails.Namespace, report.InstanceDetails.InstanceID, report.ConnectionDetails.SourceID, report.ConnectionDetails.DestinationID, report.ConnectionDetails.BatchID, report.PUDetails.InPU, report.PUDetails.PU, report.StatusDetail.Status, fmt.Sprint(report.StatusDetail.StatusCode)}
		return strings.Join(x, `::`)
	}

	for _, report := range reports {
		identifier := reportIdentifier(report)
		if _, ok := x[identifier]; !ok {
			x[identifier] = &Metric{
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
		r := funk.Find(x[identifier].StatusDetails, func(i StatusDetail) bool {
			return i.Status == report.StatusDetail.Status && i.StatusCode == report.StatusDetail.StatusCode
		})
		if r == nil {
			x[identifier].StatusDetails = append(x[identifier].StatusDetails, &StatusDetail{
				Status:         report.StatusDetail.Status,
				StatusCode:     report.StatusDetail.StatusCode,
				Count:          report.StatusDetail.Count,
				SampleResponse: report.StatusDetail.SampleResponse,
				SampleEvent:    report.StatusDetail.SampleEvent,
			})
			continue
		}
		j := r.(*StatusDetail)
		j.Count += report.StatusDetail.Count
		j.SampleResponse = report.StatusDetail.SampleResponse
		j.SampleEvent = report.StatusDetail.SampleEvent
	}

	var values []*Metric
	for _, val := range x {
		values = append(values, val)
	}
	return values
}

func mainLoop() {
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	for {
		allReported := true
		currentMin := int64(timeutil.Now().Sub(time.Time{}.UTC()).Minutes())
		reports, reportedMin := getReports(currentMin)
		if reports == nil || len(reports) == 0 {
			time.Sleep(30 * time.Second)
		}
		metrics := getAggregatedReports(reports)
		for _, metric := range metrics {
			payload, err := json.Marshal(metric)
			if err != nil {
				panic(err)
			}
			operation := func() error {
				uri := "https://webhook.site/dde3d1aa-abc1-4270-8e2d-ffbb84c1fa94"
				_, err := client.Post(uri, "application/json; charset=utf-8",
					bytes.NewBuffer(payload))
				return err
			}

			// backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
			err = backoff.RetryNotify(operation, &backoff.ExponentialBackOff{}, func(err error, t time.Duration) {
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

// loop() {
// 	getFromByReportedBy()
// 	aggregateByDest-StatusCode()
// 	sendWithBackoff()
// 	deleteFromTable()
// }

func (client *Client) Report(metrics []*PUReportedMetric, txn *sql.Tx) {
	stmt, err := txn.Prepare(pq.CopyIn(REPORTS_TABLE, "workspace_id", "namespace", "instance_id", "source_id", "destination_id", "batch_id", "in_pu", "pu", "reported_min", "status", "count", "terminal_state", "initial_state", "status_code", "sample_response", "sample_event"))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	reported_min := int64(timeutil.Now().Sub(time.Time{}.UTC()).Minutes())
	for _, metric := range metrics {
		_, err = stmt.Exec(client.Config.WorksapceID, client.Config.Namespace, client.Config.WorksapceID, metric.ConnectionDetails.SourceID, metric.ConnectionDetails.DestinationID, metric.ConnectionDetails.BatchID, metric.PUDetails.InPU, metric.PUDetails.PU, reported_min, metric.StatusDetail.Status, metric.StatusDetail.Count, metric.PUDetails.TerminalStae, metric.PUDetails.InitialState, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse, metric.StatusDetail.SampleEvent)
		if err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
}

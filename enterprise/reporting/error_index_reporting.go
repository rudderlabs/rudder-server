package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	errorIndex = "err_idx"
	dirName    = "error-index"
)

type payload struct {
	MessageID        string    `json:"messageId"`
	SourceID         string    `json:"sourceId"`
	DestinationID    string    `json:"destinationId"`
	TransformationID string    `json:"transformationId"`
	TrackingPlanID   string    `json:"trackingPlanId"`
	FailedStage      string    `json:"failedStage"`
	EventType        string    `json:"eventType"`
	EventName        string    `json:"eventName"`
	ReceivedAt       time.Time `json:"receivedAt"`
	FailedAt         time.Time `json:"failedAt"`
}

type Writer interface {
	Write(w io.Writer, payloads []payload) error
}

type ErrorIndexReporter struct {
	ctx              context.Context
	log              logger.Logger
	configSubscriber *configSubscriber
	errIndexDB       *jobsdb.Handle
	now              func() time.Time
	writer           Writer

	config struct {
		dsLimit              misc.ValueLoader[int]
		skipMaintenanceError bool
		jobRetention         time.Duration
	}
}

func NewErrorIndexReporter(
	ctx context.Context,
	conf *config.Config,
	log logger.Logger,
	configSubscriber *configSubscriber,
) *ErrorIndexReporter {
	eir := &ErrorIndexReporter{
		ctx:              ctx,
		log:              log,
		configSubscriber: configSubscriber,
		now:              time.Now,
		writer:           newWriterParquet(conf),
	}

	eir.config.dsLimit = conf.GetReloadableIntVar(0, 1, "Reporting.errorIndexReporting.dsLimit")
	eir.config.skipMaintenanceError = conf.GetBool("Reporting.errorIndexReporting.skipMaintenanceError", false)
	eir.config.jobRetention = conf.GetDurationVar(24, time.Hour, "Reporting.errorIndexReporting.jobRetention")

	eir.errIndexDB = jobsdb.NewForReadWrite(
		errorIndex,
		jobsdb.WithDSLimit(eir.config.dsLimit),
		jobsdb.WithConfig(conf),
		jobsdb.WithSkipMaintenanceErr(eir.config.skipMaintenanceError),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return eir.config.jobRetention
			},
		),
	)
	if err := eir.errIndexDB.Start(); err != nil {
		panic(fmt.Sprintf("starting error index db: %v", err))
	}

	return eir
}

// Report reports the metrics to the errorIndex JobsDB
func (eir *ErrorIndexReporter) Report(metrics []*types.PUReportedMetric, _ *sql.Tx) error {
	failedAt := eir.now()

	var jobs []*jobsdb.JobT
	for _, metric := range metrics {
		if metric.StatusDetail == nil {
			continue
		}

		for _, failedMessage := range metric.StatusDetail.FailedMessages {
			workspaceID := eir.configSubscriber.WorkspaceIDFromSource(metric.SourceID)

			payload := payload{
				MessageID:        failedMessage.MessageID,
				SourceID:         metric.SourceID,
				DestinationID:    metric.DestinationID,
				TransformationID: metric.TransformationID,
				TrackingPlanID:   metric.TrackingPlanID,
				FailedStage:      metric.PUDetails.PU,
				EventName:        metric.StatusDetail.EventName,
				EventType:        metric.StatusDetail.EventType,
				ReceivedAt:       failedMessage.ReceivedAt,
				FailedAt:         failedAt,
			}
			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("marshalling payload: %v", err)
			}

			params := struct {
				WorkspaceID string `json:"workspaceId"`
				SourceID    string `json:"source_id"`
			}{
				WorkspaceID: workspaceID,
				SourceID:    metric.SourceID,
			}
			paramsJSON, err := json.Marshal(params)
			if err != nil {
				return fmt.Errorf("marshalling params: %v", err)
			}

			jobs = append(jobs, &jobsdb.JobT{
				UUID:         uuid.New(),
				Parameters:   paramsJSON,
				EventPayload: payloadJSON,
				EventCount:   1,
				WorkspaceId:  workspaceID,
			})
		}
	}

	if len(jobs) == 0 {
		return nil
	}

	if err := eir.errIndexDB.Store(eir.ctx, jobs); err != nil {
		return fmt.Errorf("storing jobs: %v", err)
	}

	return nil
}

// DatabaseSyncer returns a syncer that syncs the errorIndex jobsDB. Once the context is done, it stops the errorIndex jobsDB
func (eir *ErrorIndexReporter) DatabaseSyncer(
	types.SyncerConfig,
) types.ReportingSyncer {
	return func() {
		<-eir.ctx.Done()

		eir.errIndexDB.Stop()
	}
}

// processJobs
// 1. groups by sourceID
// 2. create parquet files
// 3. upload parquet files
func (eir *ErrorIndexReporter) processJobs(jobs []*jobsdb.JobT) error {
	payloadsBySrcMap := make(map[string][]payload)
	for _, job := range jobs {
		var p payload
		if err := json.Unmarshal(job.EventPayload, &p); err != nil {
			return fmt.Errorf("unmarshalling payload: %v", err)
		}

		payloadsBySrcMap[p.SourceID] = append(payloadsBySrcMap[p.SourceID], p)
	}

	var files []*os.File
	defer func() {
		for _, file := range files {
			misc.RemoveFilePaths(file.Name())
		}
	}()
	for _, payloadsBySrc := range payloadsBySrcMap {
		f, err := eir.createFile(payloadsBySrc)
		if err != nil {
			return fmt.Errorf("creating file: %v", err)
		}

		files = append(files, f)
	}

	for _, file := range files {
		if err := eir.uploadParquetFile(file); err != nil {
			return fmt.Errorf("uploading parquet file: %v", err)
		}
	}

	return nil
}

func (eir *ErrorIndexReporter) createFile(payloads []payload) (*os.File, error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	fileName := fmt.Sprintf("%d.%s.parquet",
		eir.now().Unix(),
		uuid.New().String(),
	)
	filePath := path.Join(tmpDirPath, dirName, fileName)

	if err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating tmp dir: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err = eir.writer.Write(f, payloads); err != nil {
		return nil, fmt.Errorf("writing to file: %w", err)
	}

	return f, nil
}

func (eir *ErrorIndexReporter) uploadParquetFile(*os.File) error {
	return nil
}

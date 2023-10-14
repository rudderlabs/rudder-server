package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
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
	MessageID        string    `json:"messageId" parquet:"name=messageId, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	SourceID         string    `json:"sourceId" parquet:"name=sourceId, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	DestinationID    string    `json:"destinationId" parquet:"name=destinationId, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	TransformationID string    `json:"transformationId" parquet:"name=transformationId, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	TrackingPlanID   string    `json:"trackingPlanId" parquet:"name=trackingPlanId, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	FailedStage      string    `json:"failedStage" parquet:"name=failedStage, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	EventType        string    `json:"eventType" parquet:"name=eventType, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	EventName        string    `json:"eventName" parquet:"name=eventName, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	ReceivedAt       time.Time `json:"receivedAt" parquet:"name=receivedAt, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"`
	FailedAt         time.Time `json:"failedAt" parquet:"name=failedAt, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"`
}

type ErrorIndexReporter struct {
	ctx              context.Context
	log              logger.Logger
	configSubscriber *configSubscriber
	errIndexDB       *jobsdb.Handle
	now              func() time.Time

	config struct {
		dsLimit              misc.ValueLoader[int]
		skipMaintenanceError bool
		jobRetention         time.Duration

		parquetParallelWriters misc.ValueLoader[int64]
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
	}

	eir.config.dsLimit = conf.GetReloadableIntVar(0, 1, "Reporting.errorIndexReporting.dsLimit")
	eir.config.skipMaintenanceError = conf.GetBool("Reporting.errorIndexReporting.skipMaintenanceError", false)
	eir.config.jobRetention = conf.GetDurationVar(24, time.Hour, "Reporting.errorIndexReporting.jobRetention")
	eir.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Reporting.errorIndexReporting.parquetParallelWriters")

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

	var files []*source.ParquetFile
	defer func() {
		for _, f := range files {
			misc.RemoveFilePaths(f.Name())
		}
	}()
	for _, payloadsBySrc := range payloadsBySrcMap {
		f, err := eir.createParquetFile(payloadsBySrc)
		if err != nil {
			return fmt.Errorf("creating parquet file: %v", err)
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

func (eir *ErrorIndexReporter) createParquetFile(payloadsBySrc []payload) (*source.ParquetFile, error) {
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
	fmt.Println(filePath)

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return nil, fmt.Errorf("creating local file writer: %v", err)
	}
	defer func() { _ = fw.Close() }()

	pw, err := writer.NewParquetWriter(
		fw,
		new(payload),
		eir.config.parquetParallelWriters.Load(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating parquet writer: %v", err)
	}

	pw.RowGroupSize = 128 * bytesize.MB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, payloadBySrc := range payloadsBySrc {
		if err = pw.Write(payloadBySrc); err != nil {
			return nil, fmt.Errorf("writing parquet: %v", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return nil, fmt.Errorf("stopping parquet writer: %v", err)
	}

	return &fw, nil
}

func (eir *ErrorIndexReporter) uploadParquetFile(*source.ParquetFile) error {
	return nil
}

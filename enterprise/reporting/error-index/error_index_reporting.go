package error_index

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	ptypes "github.com/xitongsys/parquet-go/types"
)

const (
	folderName = "rudder-failed-messages"
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

type payloadParquet struct {
	MessageID        string `parquet:"name=message_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	SourceID         string `parquet:"name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	DestinationID    string `parquet:"name=destination_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	TransformationID string `parquet:"name=transformation_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	TrackingPlanID   string `parquet:"name=tracking_plan_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	FailedStage      string `parquet:"name=failed_stage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	EventType        string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	EventName        string `parquet:"name=event_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	ReceivedAt       int64  `parquet:"name=received_at, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"`
	FailedAt         int64  `parquet:"name=failed_at, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"`
}

func (p payload) toParquet() payloadParquet {
	return payloadParquet{
		MessageID:        p.MessageID,
		SourceID:         p.SourceID,
		DestinationID:    p.DestinationID,
		TransformationID: p.TransformationID,
		TrackingPlanID:   p.TrackingPlanID,
		FailedStage:      p.FailedStage,
		EventType:        p.EventType,
		EventName:        p.EventName,
		ReceivedAt:       ptypes.TimeToTIMESTAMP_MICROS(p.ReceivedAt, true),
		FailedAt:         ptypes.TimeToTIMESTAMP_MICROS(p.FailedAt, true),
	}
}

type Writer interface {
	Write(w io.Writer, payloads []payload) error
	Extension() string
}

type configFetcher interface {
	WorkspaceIDFromSource(sourceID string) string
	IsPIIReportingDisabled(workspaceID string) bool
}

type ErrorIndexReporter struct {
	ctx           context.Context
	log           logger.Logger
	configFetcher configFetcher
	errIndexDB    *jobsdb.Handle
	now           func() time.Time
	writer        Writer
	fileManager   filemanager.FileManager

	config struct {
		dsLimit              misc.ValueLoader[int]
		skipMaintenanceError bool
		jobRetention         time.Duration
		instanceID           string
	}
}

func NewErrorIndexReporter(
	ctx context.Context,
	conf *config.Config,
	log logger.Logger,
	configSubscriber configFetcher,
) *ErrorIndexReporter {
	eir := &ErrorIndexReporter{
		ctx:           ctx,
		log:           log,
		configFetcher: configSubscriber,
		now:           time.Now,
		writer:        newWriterParquet(conf),
	}

	eir.config.dsLimit = conf.GetReloadableIntVar(0, 1, "Reporting.errorIndexReporting.dsLimit")
	eir.config.skipMaintenanceError = conf.GetBool("Reporting.errorIndexReporting.skipMaintenanceError", false)
	eir.config.jobRetention = conf.GetDurationVar(24, time.Hour, "Reporting.errorIndexReporting.jobRetention")
	eir.config.instanceID = conf.GetString("INSTANCE_ID", "1")

	var err error

	provider := conf.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	eir.fileManager, err = filemanager.New(&filemanager.Settings{
		Provider: provider,
		Config: filemanager.GetProviderConfigFromEnv(
			filemanagerutil.ProviderConfigOpts(
				ctx,
				provider,
				conf,
			),
		),
		Conf: conf,
	})
	if err != nil {
		panic(fmt.Sprintf("creating filemanager: %v", err))
	}

	eir.errIndexDB = jobsdb.NewForReadWrite(
		"err_idx",
		jobsdb.WithDSLimit(eir.config.dsLimit),
		jobsdb.WithConfig(conf),
		jobsdb.WithSkipMaintenanceErr(eir.config.skipMaintenanceError),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return eir.config.jobRetention
			},
		),
	)
	if err = eir.errIndexDB.Start(); err != nil {
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
			workspaceID := eir.configFetcher.WorkspaceIDFromSource(metric.SourceID)

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
// 1. Group jobs by aggregateKey.
// 2. Creates a file for each group and upload it to the rudder object storage.
func (eir *ErrorIndexReporter) processJobs(
	ctx context.Context,
	jobs []*jobsdb.JobT,
) error {
	aggregatedJobs, err := eir.aggregateJobs(jobs)
	if err != nil {
		return fmt.Errorf("aggregating jobs: %v", err)
	}

	var files []*os.File
	defer func() {
		for _, file := range files {
			misc.RemoveFilePaths(file.Name())
		}
	}()
	for _, aggregatedJob := range aggregatedJobs {
		if len(aggregatedJob) == 0 {
			continue
		}

		workspaceID := eir.configFetcher.WorkspaceIDFromSource(aggregatedJob[0].SourceID)
		//windowFormat := aggregatedJob[0].FailedAt.Format("2006/01/02/15")

		if eir.configFetcher.IsPIIReportingDisabled(workspaceID) {
			eir.transformPayloadForPII(aggregatedJob)
		}

		f, err := eir.createFile(aggregatedJob)
		if err != nil {
			return fmt.Errorf("creating file: %v", err)
		}

		//if err := eir.uploadFile(ctx, f, windowFormat); err != nil {
		//	return fmt.Errorf("uploading file: %v", err)
		//}

		files = append(files, f)
	}
	return nil
}

// transformPayloadForPII transforms the payload for pii
func (eir *ErrorIndexReporter) transformPayloadForPII(
	payloads []payload,
) {
	for _, payload := range payloads {
		payload.EventName = ""
	}
}

// aggregateJobs aggregates the jobs by aggregateKey
func (eir *ErrorIndexReporter) aggregateJobs(
	jobs []*jobsdb.JobT,
) (map[string][]payload, error) {
	aggregatedJobs := make(map[string][]payload)

	for _, job := range jobs {
		var p payload
		if err := json.Unmarshal(job.EventPayload, &p); err != nil {
			return nil, fmt.Errorf("unmarshalling payload: %v", err)
		}

		key := eir.aggregateKey(p)
		aggregatedJobs[key] = append(aggregatedJobs[key], p)
	}

	return aggregatedJobs, nil
}

func (eir *ErrorIndexReporter) aggregateKey(
	payload payload,
) string {
	keys := []string{
		payload.SourceID,
		//payload.FailedAt.String(),
	}
	return strings.Join(keys, "::")
}

func (eir *ErrorIndexReporter) createFile(
	payloads []payload,
) (*os.File, error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	fileName := fmt.Sprintf("%s.%s.%d%s", payloads[0].SourceID,
		//payloads[0].FailedAt.Unix(),
		uuid.New().String(),
		eir.now().Unix(),
		eir.writer.Extension(),
	)
	filePath := path.Join(tmpDirPath, folderName, fileName)

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

// failed-messages/<source_id>/2000-01-01/12/946684800_946688400_<instance-name>.parquet
// failed_at min_max
// Check with @atzoum how to have snakecase or current in parquet
func (eir *ErrorIndexReporter) uploadFile(
	ctx context.Context,
	file *os.File,
	windowFormat string,
) error {
	_, err := eir.fileManager.Upload(ctx, file,
		eir.config.instanceID,
		folderName,
		windowFormat,
	)
	if err != nil {
		return fmt.Errorf("uploading file %s: %v", file.Name(), err)
	}
	return nil
}

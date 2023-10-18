package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
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
	folderName = "error-index"
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
	fileManager      filemanager.FileManager

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
	if err = eir.errIndexDB.Start(); err != nil {
		panic(fmt.Sprintf("starting error index db: %v", err))
	}

	return eir
}

// Report reports the metrics to the errorIndex JobsDB
func (eir *ErrorIndexReporter) Report(metrics []*types.PUReportedMetric, _ *sql.Tx) error {
	failedAt := eir.now().UTC()

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
// 1. Groups by aggregateKey
// 2. Creates a file for each aggregateKey
// 3. Uploads the files
func (eir *ErrorIndexReporter) processJobs(jobs []*jobsdb.JobT) error {
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
		f, err := eir.createFile(aggregatedJob)
		if err != nil {
			return fmt.Errorf("creating file: %v", err)
		}

		files = append(files, f)
	}

	if err := eir.uploadFiles(files); err != nil {
		return fmt.Errorf("uploading file: %v", err)
	}
	return nil
}

// aggregateJobs
// 1. Groups jobs by aggregateKey
// 2. Sorts the jobs by sortKey to achieve better encoding
func (eir *ErrorIndexReporter) aggregateJobs(jobs []*jobsdb.JobT) (map[string][]payload, error) {
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

func (eir *ErrorIndexReporter) aggregateKey(p payload) string {
	keys := []string{
		p.SourceID,
		p.FailedAt.String(),
	}
	return strings.Join(keys, "::")
}

func (eir *ErrorIndexReporter) createFile(payloads []payload) (*os.File, error) {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("creating tmp directory: %w", err)
	}

	fileName := fmt.Sprintf("%s.%s.%d.parquet", payloads[0].SourceID, uuid.New().String(),
		eir.now().Unix(),
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

func (eir *ErrorIndexReporter) uploadFiles([]*os.File) error {
	keyPrefixes := []string{folderName, batchJobs.Connection.Source.ID, brt.customDatePrefix.Load() + datePrefixLayout}

	prefixes := []string{"rudder-error-index", time.Now().Format("01-02-2006")}
	return nil
}

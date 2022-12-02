package loadfiles

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPublishBatchSize = 100
)

var warehousesToVerifyLoadFilesFolder = []string{warehouseutils.SNOWFLAKE}

type Notifier interface {
	Publish(payload pgnotifier.MessagePayload, schema *warehouseutils.SchemaT, priority int) (ch chan []pgnotifier.ResponseT, err error)
}

type StageFileRepo interface {
	SetStatuses(ctx context.Context, ids []int64, status string) (err error)
	SetErrorStatus(ctx context.Context, stagingFileID int64, stageFileErr error) error
}

type LoadFileRepo interface {
	Insert(ctx context.Context, loadFiles []model.LoadFile) error
	DeleteByStagingFiles(ctx context.Context, stagingFileIDs []int64) error
	GetByStagingFiles(ctx context.Context, stagingFileIDs []int64) ([]model.LoadFile, error)
}

type ControlPlaneClient interface {
	DestinationHistory(ctx context.Context, revisionID string) (backendconfig.DestinationT, error)
}

type LoadFileGenerator struct {
	Logger   logger.Logger
	Notifier Notifier

	StageRepo StageFileRepo
	LoadRepo  LoadFileRepo

	ControlPlaneClient ControlPlaneClient

	publishBatchSize int
}

type jobResponse struct {
	TableName             string
	Location              string
	TotalRows             int
	ContentLength         int64
	StagingFileID         int64
	DestinationRevisionID string
	UseRudderStorage      bool
}

type Payload struct {
	BatchID                      string
	UploadID                     int64
	StagingFileID                int64
	StagingFileLocation          string
	UploadSchema                 map[string]map[string]string
	WorkspaceID                  string
	SourceID                     string
	SourceName                   string
	DestinationID                string
	DestinationName              string
	DestinationType              string
	DestinationNamespace         string
	DestinationRevisionID        string
	StagingDestinationRevisionID string
	DestinationConfig            map[string]interface{}
	StagingDestinationConfig     interface{}
	UseRudderStorage             bool
	StagingUseRudderStorage      bool
	UniqueLoadGenID              string
	RudderStoragePrefix          string
	Output                       []jobResponse
	LoadFilePrefix               string // prefix for the load file name
	LoadFileType                 string
}

func WithConfig(ld *LoadFileGenerator, config *config.Config) {
	ld.publishBatchSize = config.GetInt("Warehouse.loadFileGenerator.publishBatchSize", 100)
}

func (lf *LoadFileGenerator) CreateLoadFiles(ctx context.Context, job model.UploadJob, generateAll bool) (startLoadFileID, endLoadFileID int64, err error) {
	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	stagingFiles := job.StagingFiles

	publishBatchSize := lf.publishBatchSize
	if publishBatchSize == 0 {
		publishBatchSize = defaultPublishBatchSize
	}

	lf.Logger.Infof("[WH]: Starting batch processing %v stage files for %s:%s", publishBatchSize, destType, destID)
	uniqueLoadGenID := misc.FastUUID().String()
	job.Upload.LoadFileGenStartTime = timeutil.Now()

	// Getting distinct destination revision ID from staging files metadata
	destinationRevisionIDMap, err := lf.destinationRevisionIDMap(ctx, job)
	if err != nil {
		return 0, 0, fmt.Errorf("populating destination revision ID: %w", err)
	}

	var toProcessStagingFiles []*model.StagingFile
	if generateAll {
		toProcessStagingFiles = stagingFiles
	} else {
		// skip processing staging files marked succeeded
		for _, stagingFile := range stagingFiles {
			if stagingFile.Status != warehouseutils.StagingFileSucceededState {
				toProcessStagingFiles = append(toProcessStagingFiles, stagingFile)
			}
		}
	}

	stagingFileIDs := repo.StagingFileIDs(toProcessStagingFiles)

	err = lf.LoadRepo.DeleteByStagingFiles(ctx, stagingFileIDs)
	if err != nil {
		return 0, 0, fmt.Errorf("deleting previous load files: %w", err)
	}

	err = lf.StageRepo.SetStatuses(ctx, stagingFileIDs, warehouseutils.StagingFileExecutingState)
	if err != nil {
		return 0, 0, fmt.Errorf("set staging file status to executing: %w", err)
	}

	defer func() {
		if err != nil {
			err = lf.StageRepo.SetStatuses(ctx, stagingFileIDs, warehouseutils.StagingFileFailedState)
		}
	}()

	var g errgroup.Group

	var sampleError error
	for i := 0; i < len(toProcessStagingFiles); i += publishBatchSize {
		j := i + publishBatchSize
		if j > len(toProcessStagingFiles) {
			j = len(toProcessStagingFiles)
		}

		// td : add prefix to payload for s3 dest
		var messages []pgnotifier.JobPayload
		for _, stagingFile := range toProcessStagingFiles[i:j] {
			payload := Payload{
				UploadID:                     job.Upload.ID,
				StagingFileID:                stagingFile.ID,
				StagingFileLocation:          stagingFile.Location,
				LoadFileType:                 job.Upload.LoadFileType,
				SourceID:                     job.Warehouse.Source.ID,
				SourceName:                   job.Warehouse.Source.Name,
				DestinationID:                destID,
				DestinationName:              job.Warehouse.Destination.Name,
				DestinationType:              destType,
				DestinationNamespace:         job.Warehouse.Namespace,
				DestinationConfig:            job.Warehouse.Destination.Config,
				WorkspaceID:                  job.Warehouse.Destination.WorkspaceID,
				UniqueLoadGenID:              uniqueLoadGenID,
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				UseRudderStorage:             job.Upload.UseRudderStorage,
				StagingUseRudderStorage:      stagingFile.UseRudderStorage,
				DestinationRevisionID:        job.Warehouse.Destination.RevisionID,
				StagingDestinationRevisionID: stagingFile.DestinationRevisionID,
			}
			if revisionConfig, ok := destinationRevisionIDMap[stagingFile.DestinationRevisionID]; ok {
				payload.StagingDestinationConfig = revisionConfig.Config
			}
			if slices.Contains(warehouseutils.TimeWindowDestinations, job.Warehouse.Type) {
				payload.LoadFilePrefix = warehouseutils.GetLoadFilePrefix(stagingFile.TimeWindow, job.Warehouse)
			}

			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return 0, 0, fmt.Errorf("error marshalling payload: %w", err)
			}
			messages = append(messages, payloadJSON)
		}

		schema := &job.Upload.UploadSchema
		if job.Upload.LoadFileType == warehouseutils.LOAD_FILE_TYPE_PARQUET {
			schema = &job.Upload.MergedSchema
		}

		lf.Logger.Infof("[WH]: Publishing %d staging files for %s:%s to PgNotifier", len(messages), destType, destID)
		messagePayload := pgnotifier.MessagePayload{
			Jobs:    messages,
			JobType: "upload",
		}
		ch, err := lf.Notifier.Publish(messagePayload, schema, job.Upload.Priority)
		if err != nil {
			return 0, 0, fmt.Errorf("error publishing to PgNotifier: %w", err)
		}
		// set messages to nil to release mem allocated
		messages = nil
		batchStartIdx := i
		batchEndIdx := j

		g.Go(func() error {
			responses := <-ch
			lf.Logger.Infof("[WH]: Received responses for staging files %d:%d for %s:%s from PgNotifier", toProcessStagingFiles[batchStartIdx].ID, toProcessStagingFiles[batchEndIdx-1].ID, destType, destID)

			var loadFiles []model.LoadFile
			var successfulStagingFileIDs []int64
			for _, resp := range responses {
				// Error handling during generating_load_files step:
				// 1. any error returned by pgnotifier is set on corresponding staging_file
				// 2. any error effecting a batch/all the staging files like saving load file records to wh db
				//    is returned as error to caller of the func to set error on all staging files and the whole generating_load_files step
				if resp.Status == "aborted" {
					lf.Logger.Errorf("[WH]: Error in generating load files: %v", resp.Error)
					sampleError = fmt.Errorf(resp.Error)
					err = lf.StageRepo.SetErrorStatus(ctx, resp.JobID, sampleError)
					if err != nil {
						return fmt.Errorf("set staging file error status: %w", err)
					}
					continue
				}
				var output []jobResponse
				err = json.Unmarshal(resp.Output, &output)
				if err != nil {
					return fmt.Errorf("unmarshalling response from pgnotifier: %w", err)
				}
				if len(output) == 0 {
					lf.Logger.Errorf("[WH]: No LoadFiles returned by wh worker")
					continue
				}
				for i := range output {
					loadFiles = append(loadFiles, model.LoadFile{
						TableName:             output[i].TableName,
						Location:              output[i].Location,
						TotalRows:             output[i].TotalRows,
						ContentLength:         output[i].ContentLength,
						StagingFileID:         output[i].StagingFileID,
						DestinationRevisionID: output[i].DestinationRevisionID,
						UseRudderStorage:      output[i].UseRudderStorage,
						SourceID:              job.Upload.SourceID,
						DestinationID:         job.Upload.DestinationID,
						DestinationType:       job.Upload.DestinationType,
					})
				}

				successfulStagingFileIDs = append(successfulStagingFileIDs, resp.JobID)
			}

			err = lf.LoadRepo.Insert(ctx, loadFiles)
			if err != nil {
				return fmt.Errorf("inserting load files: %w", err)
			}
			err = lf.StageRepo.SetStatuses(ctx, successfulStagingFileIDs, warehouseutils.StagingFileSucceededState)
			if err != nil {
				return fmt.Errorf("setting staging file status to succeeded: %w", err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return startLoadFileID, endLoadFileID, err
	}

	loadFiles, err := lf.LoadRepo.GetByStagingFiles(ctx, repo.StagingFileIDs(toProcessStagingFiles))
	if err != nil {
		return 0, 0, fmt.Errorf("getting load files: %w", err)
	}

	if len(loadFiles) == 0 {
		err = fmt.Errorf(`no load files generated. Sample error: %v`, sampleError)
		return startLoadFileID, endLoadFileID, err
	}

	// verify if all load files are in same folder in object storage
	if slices.Contains(warehousesToVerifyLoadFilesFolder, job.Warehouse.Type) {
		for _, loadFile := range loadFiles {
			if !strings.Contains(loadFile.Location, uniqueLoadGenID) {
				err = fmt.Errorf(`all loadfiles do not contain the same uniqueLoadGenID: %s`, uniqueLoadGenID)
				return
			}
		}
	}

	return loadFiles[0].ID, loadFiles[len(loadFiles)-1].ID, nil
}

func (lf *LoadFileGenerator) destinationRevisionIDMap(ctx context.Context, job model.UploadJob) (revisionIDMap map[string]backendconfig.DestinationT, err error) {
	revisionIDMap = make(map[string]backendconfig.DestinationT)

	// TODO: ensure DestinationRevisionID is populated
	for _, file := range job.StagingFiles {
		revisionID := file.DestinationRevisionID
		// No need to make config backend api call for the current config
		if revisionID == job.Warehouse.Destination.RevisionID {
			revisionIDMap[revisionID] = job.Warehouse.Destination
			continue
		}

		destination, err := lf.ControlPlaneClient.DestinationHistory(ctx, revisionID)
		if err != nil {
			return nil, err
		}
		revisionIDMap[revisionID] = destination
	}
	return
}

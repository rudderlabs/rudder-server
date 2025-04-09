package loadfiles

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	defaultPublishBatchSize = 100
)

var warehousesToVerifyLoadFilesFolder = []string{warehouseutils.SNOWFLAKE}

type Notifier interface {
	Publish(ctx context.Context, payload *notifier.PublishRequest) (ch <-chan *notifier.PublishResponse, err error)
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
	Conf     *config.Config
	Logger   logger.Logger
	Notifier Notifier

	StageRepo StageFileRepo
	LoadRepo  LoadFileRepo

	ControlPlaneClient ControlPlaneClient

	publishBatchSize             int
	publishBatchSizePerWorkspace map[string]int
}

type WorkerJobResponse struct {
	StagingFileID int64            `json:"StagingFileID"`
	Output        []LoadFileUpload `json:"Output"`
}

type LoadFileUpload struct {
	TableName             string
	Location              string
	TotalRows             int
	ContentLength         int64
	StagingFileID         int64
	DestinationRevisionID string
	UseRudderStorage      bool
}

// baseWorkerJobRequest contains common fields for both v1 and v2 job requests
type baseWorkerJobRequest struct {
	BatchID                      string                 `json:"batch_id"`
	UploadID                     int64                  `json:"upload_id"`
	WorkspaceID                  string                 `json:"workspace_id"`
	SourceID                     string                 `json:"source_id"`
	SourceName                   string                 `json:"source_name"`
	DestinationID                string                 `json:"destination_id"`
	DestinationName              string                 `json:"destination_name"`
	DestinationType              string                 `json:"destination_type"`
	DestinationNamespace         string                 `json:"destination_namespace"`
	DestinationRevisionID        string                 `json:"destination_revision_id"`
	StagingDestinationRevisionID string                 `json:"staging_destination_revision_id"`
	DestinationConfig            map[string]interface{} `json:"destination_config"`
	StagingDestinationConfig     interface{}            `json:"staging_destination_config"`
	UseRudderStorage             bool                   `json:"use_rudder_storage"`
	StagingUseRudderStorage      bool                   `json:"staging_use_rudder_storage"`
	UniqueLoadGenID              string                 `json:"unique_load_gen_id"`
	RudderStoragePrefix          string                 `json:"rudder_storage_prefix"`
	LoadFilePrefix               string                 `json:"load_file_prefix"` // prefix for the load file name
	LoadFileType                 string                 `json:"load_file_type"`
	UploadSchema                 model.Schema           `json:"upload_schema"`
}

type WorkerJobRequest struct {
	baseWorkerJobRequest
	StagingFileID       int64  `json:"staging_file_id"`
	StagingFileLocation string `json:"staging_file_location"`
}

type StagingFileInfo struct {
	ID       int64  `json:"id"`
	Location string `json:"location"`
}

type WorkerJobRequestV2 struct {
	baseWorkerJobRequest
	StagingFiles []StagingFileInfo `json:"staging_files"`
}

func WithConfig(ld *LoadFileGenerator, config *config.Config) {
	ld.publishBatchSize = config.GetInt("Warehouse.loadFileGenerator.publishBatchSize", defaultPublishBatchSize)
	mapConfig := config.GetStringMap("Warehouse.pgNotifierPublishBatchSizeWorkspaceIDs", nil)

	ld.publishBatchSizePerWorkspace = make(map[string]int, len(mapConfig))
	for k, v := range mapConfig {
		val, ok := v.(float64)
		if !ok {
			ld.publishBatchSizePerWorkspace[k] = defaultPublishBatchSize
			continue
		}
		ld.publishBatchSizePerWorkspace[k] = int(val)
	}
}

// CreateLoadFiles for the staging files that have not been successfully processed.
func (lf *LoadFileGenerator) CreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error) {
	return lf.createFromStaging(
		ctx,
		job,
		lo.Filter(
			job.StagingFiles,
			func(stagingFile *model.StagingFile, _ int) bool {
				return stagingFile.Status != warehouseutils.StagingFileSucceededState
			},
		),
	)
}

// ForceCreateLoadFiles creates load files for the staging files, regardless if they are already successfully processed.
func (lf *LoadFileGenerator) ForceCreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error) {
	return lf.createFromStaging(ctx, job, job.StagingFiles)
}

func (lf *LoadFileGenerator) AllowUploadV2JobCreation(job *model.UploadJob) bool {
	return lf.Conf.GetBool(fmt.Sprintf("Warehouse.%s.enableV2NotifierJob", job.Upload.DestinationType), false) || lf.Conf.GetBool("Warehouse.enableV2NotifierJob", false)
}

func (lf *LoadFileGenerator) createFromStaging(ctx context.Context, job *model.UploadJob, toProcessStagingFiles []*model.StagingFile) (int64, int64, error) {
	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	var err error

	publishBatchSize := lf.publishBatchSize
	if publishBatchSize == 0 {
		publishBatchSize = defaultPublishBatchSize
	}
	if size, ok := lf.publishBatchSizePerWorkspace[strings.ToLower(job.Warehouse.WorkspaceID)]; ok {
		publishBatchSize = size
	}

	uniqueLoadGenID := misc.FastUUID().String()

	lf.Logger.Infof("[WH]: Starting batch processing %v stage files for %s:%s", publishBatchSize, destType, destID)

	job.LoadFileGenStartTime = timeutil.Now()

	// Delete previous load files for the staging files
	stagingFileIDs := repo.StagingFileIDs(toProcessStagingFiles)
	if err = lf.LoadRepo.DeleteByStagingFiles(ctx, stagingFileIDs); err != nil {
		return 0, 0, fmt.Errorf("deleting previous load files: %w", err)
	}

	// Set staging file status to executing
	if err = lf.StageRepo.SetStatuses(
		ctx,
		stagingFileIDs,
		warehouseutils.StagingFileExecutingState,
	); err != nil {
		return 0, 0, fmt.Errorf("set staging file status to executing: %w", err)
	}

	defer func() {
		// ensure that if there is an error, we set the staging file status to failed
		if err != nil {
			if errStatus := lf.StageRepo.SetStatuses(
				ctx,
				stagingFileIDs,
				warehouseutils.StagingFileFailedState,
			); errStatus != nil {
				err = fmt.Errorf("%w, and also: %v", err, errStatus)
			}
		}
	}()

	if !lf.AllowUploadV2JobCreation(job) {
		err = lf.createUploadJobs(ctx, job, toProcessStagingFiles, publishBatchSize, uniqueLoadGenID)
		if err != nil {
			return 0, 0, err
		}
		return lf.getLoadFileIDs(ctx, job, stagingFileIDs, uniqueLoadGenID)
	}

	v1Files := make([]*model.StagingFile, 0)
	v2Files := make([]*model.StagingFile, 0)
	for _, file := range toProcessStagingFiles {
		if len(file.BytesPerTable) > 0 {
			v2Files = append(v2Files, file)
		} else {
			v1Files = append(v1Files, file)
		}
	}

	// Process both groups in parallel. This might violate the publishBatchSize constraint
	// but that should be fine since we don't expect too many such instances where the batch will have both v1 and v2 files
	// Usually a batch will have only v1 or v2 files
	g, gCtx := errgroup.WithContext(ctx)
	if len(v1Files) > 0 {
		g.Go(func() error {
			return lf.createUploadJobs(gCtx, job, v1Files, publishBatchSize, uniqueLoadGenID)
		})
	}
	if len(v2Files) > 0 {
		g.Go(func() error {
			return lf.createUploadV2Jobs(gCtx, job, v2Files, publishBatchSize, uniqueLoadGenID)
		})
	}

	if err = g.Wait(); err != nil {
		return 0, 0, err
	}
	return lf.getLoadFileIDs(ctx, job, stagingFileIDs, uniqueLoadGenID)
}

func (lf *LoadFileGenerator) getLoadFileIDs(ctx context.Context, job *model.UploadJob, stagingFileIDs []int64, uniqueLoadGenID string) (int64, int64, error) {
	loadFiles, err := lf.LoadRepo.GetByStagingFiles(ctx, stagingFileIDs)
	if err != nil {
		return 0, 0, fmt.Errorf("getting load files: %w", err)
	}
	if len(loadFiles) == 0 {
		return 0, 0, fmt.Errorf("no load files generated")
	}

	if !slices.IsSortedFunc(loadFiles, func(a, b model.LoadFile) int {
		return int(a.ID - b.ID)
	}) {
		return 0, 0, fmt.Errorf(`assertion: load files returned from repo not sorted by id`)
	}

	// verify if all load files are in same folder in object storage
	if slices.Contains(warehousesToVerifyLoadFilesFolder, job.Warehouse.Type) {
		for _, loadFile := range loadFiles {
			if !strings.Contains(loadFile.Location, uniqueLoadGenID) {
				err = fmt.Errorf(`all loadfiles do not contain the same uniqueLoadGenID: %s`, uniqueLoadGenID)
				return 0, 0, err
			}
		}
	}

	return loadFiles[0].ID, loadFiles[len(loadFiles)-1].ID, nil
}

func (lf *LoadFileGenerator) generateBaseRequest(
	job *model.UploadJob,
	uniqueLoadGenID string,
	stagingFile *model.StagingFile,
	destinationRevisionIDMap map[string]backendconfig.DestinationT,
) baseWorkerJobRequest {
	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	baseReq := baseWorkerJobRequest{
		UploadID:                     job.Upload.ID,
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
		baseReq.StagingDestinationConfig = revisionConfig.Config
	}
	if slices.Contains(warehouseutils.TimeWindowDestinations, job.Warehouse.Type) {
		baseReq.LoadFilePrefix = lf.GetLoadFilePrefix(stagingFile.TimeWindow, job.Warehouse)
	}
	return baseReq
}

func (lf *LoadFileGenerator) publishToNotifier(
	ctx context.Context,
	job *model.UploadJob,
	messages []stdjson.RawMessage,
	jobType notifier.JobType,
) (<-chan *notifier.PublishResponse, error) {
	uploadSchemaJSON, err := jsonrs.Marshal(struct {
		UploadSchema model.Schema
	}{
		UploadSchema: job.Upload.UploadSchema,
	})
	if err != nil {
		return nil, fmt.Errorf("error marshalling upload schema: %w", err)
	}

	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	lf.Logger.Infof("[WH]: Publishing %d staging files for %s:%s to notifier", len(messages), destType, destID)

	ch, err := lf.Notifier.Publish(ctx, &notifier.PublishRequest{
		Payloads:     messages,
		JobType:      jobType,
		UploadSchema: uploadSchemaJSON,
		Priority:     job.Upload.Priority,
	})
	if err != nil {
		return nil, fmt.Errorf("error publishing to notifier: %w", err)
	}
	return ch, nil
}

func (lf *LoadFileGenerator) processNotifierResponse(ctx context.Context, ch <-chan *notifier.PublishResponse, job *model.UploadJob, chunk []*model.StagingFile) error {
	responses, ok := <-ch
	if !ok {
		return fmt.Errorf("receiving notifier channel closed")
	}

	logNotifierResponse(lf.Logger, job, chunk)
	if responses.Err != nil {
		return fmt.Errorf("receiving responses from notifier: %w", responses.Err)
	}

	var loadFiles []model.LoadFile
	var successfulStagingFileIDs []int64
	for _, resp := range responses.Jobs {
		// Error handling during generating_load_files step:
		// 1. any error returned by notifier is set on corresponding staging_file
		// 2. any error effecting a batch/all the staging files like saving load file records to wh db
		//    is returned as error to caller of the func to set error on all staging files and the whole generating_load_files step
		var jobResponse WorkerJobResponse
		if err := jsonrs.Unmarshal(resp.Payload, &jobResponse); err != nil {
			return fmt.Errorf("unmarshalling response from notifier: %w", err)
		}

		if resp.Status == notifier.Aborted && resp.Error != nil {
			lf.Logger.Errorf("[WH]: Error in generating load files: %v", resp.Error)
			sampleError := errors.New(resp.Error.Error())
			err := lf.StageRepo.SetErrorStatus(ctx, jobResponse.StagingFileID, sampleError)
			if err != nil {
				return fmt.Errorf("set staging file error status: %w", err)
			}
			continue
		}
		if len(jobResponse.Output) == 0 {
			lf.Logger.Errorf("[WH]: No LoadFiles returned by worker")
			continue
		}
		for _, output := range jobResponse.Output {
			loadFile := toLoadFile(output, job)
			loadFile.StagingFileID = jobResponse.StagingFileID
			loadFiles = append(loadFiles, loadFile)
		}

		successfulStagingFileIDs = append(successfulStagingFileIDs, jobResponse.StagingFileID)
	}

	if len(loadFiles) == 0 {
		return nil
	}

	if err := lf.LoadRepo.Insert(ctx, loadFiles); err != nil {
		return fmt.Errorf("inserting load files: %w", err)
	}
	if err := lf.StageRepo.SetStatuses(ctx, successfulStagingFileIDs, warehouseutils.StagingFileSucceededState); err != nil {
		return fmt.Errorf("setting staging file status to succeeded: %w", err)
	}
	return nil
}

// Unlike upload type job, for v2 we are not setting the status of staging files
func (lf *LoadFileGenerator) processNotifierResponseV2(ctx context.Context, ch <-chan *notifier.PublishResponse, job *model.UploadJob, chunk []*model.StagingFile) error {
	responses, ok := <-ch
	if !ok {
		return fmt.Errorf("receiving notifier channel closed")
	}
	logNotifierResponse(lf.Logger, job, chunk)
	if responses.Err != nil {
		return fmt.Errorf("receiving responses from notifier: %w", responses.Err)
	}

	var loadFiles []model.LoadFile
	for _, resp := range responses.Jobs {
		var loadFileUploads []LoadFileUpload
		if err := jsonrs.Unmarshal(resp.Payload, &loadFileUploads); err != nil {
			return fmt.Errorf("unmarshalling response from notifier: %w", err)
		}

		if resp.Status == notifier.Aborted && resp.Error != nil {
			lf.Logger.Errorf("[WH]: Error in generating load files: %v", resp.Error)
			continue
		}
		if len(loadFileUploads) == 0 {
			lf.Logger.Errorf("[WH]: No LoadFiles returned by worker")
			continue
		}
		for _, output := range loadFileUploads {
			loadFiles = append(loadFiles, toLoadFile(output, job))
		}
	}
	if len(loadFiles) == 0 {
		return nil
	}
	if err := lf.LoadRepo.Insert(ctx, loadFiles); err != nil {
		return fmt.Errorf("inserting load files: %w", err)
	}
	return nil
}

func (lf *LoadFileGenerator) createUploadJobs(ctx context.Context, job *model.UploadJob, stagingFiles []*model.StagingFile, publishBatchSize int, uniqueLoadGenID string) error {
	var g errgroup.Group
	destinationRevisionIDMap, err := lf.destinationRevisionIDMap(ctx, job)
	if err != nil {
		return fmt.Errorf("populating destination revision ID: %w", err)
	}

	for _, chunk := range lo.Chunk(stagingFiles, publishBatchSize) {
		var messages []stdjson.RawMessage
		for _, stagingFile := range chunk {
			baseReq := lf.generateBaseRequest(job, uniqueLoadGenID, stagingFile, destinationRevisionIDMap)
			rawPayload, err := jsonrs.Marshal(WorkerJobRequest{
				baseWorkerJobRequest: baseReq,
				StagingFileID:        stagingFile.ID,
				StagingFileLocation:  stagingFile.Location,
			})
			if err != nil {
				return fmt.Errorf("error generating raw payload: %w", err)
			}
			messages = append(messages, rawPayload)
		}

		ch, err := lf.publishToNotifier(ctx, job, messages, notifier.JobTypeUpload)
		if err != nil {
			return fmt.Errorf("error publishing to notifier: %w", err)
		}
		// set messages to nil to release mem allocated
		messages = nil

		g.Go(func() error {
			return lf.processNotifierResponse(ctx, ch, job, chunk)
		})
	}
	return g.Wait()
}

func (lf *LoadFileGenerator) createUploadV2Jobs(ctx context.Context, job *model.UploadJob, stagingFiles []*model.StagingFile, publishBatchSize int, uniqueLoadGenID string) error {
	var g errgroup.Group
	destinationRevisionIDMap, err := lf.destinationRevisionIDMap(ctx, job)
	if err != nil {
		return fmt.Errorf("populating destination revision ID: %w", err)
	}

	for _, chunk := range lo.Chunk(stagingFiles, publishBatchSize) {
		fileGroups := lf.GroupStagingFiles(chunk, lf.Conf.GetInt("Warehouse.loadFiles.maxSizeInMB", 128))
		for _, group := range fileGroups {
			baseReq := lf.generateBaseRequest(job, uniqueLoadGenID, group[0], destinationRevisionIDMap)

			stagingFileInfos := make([]StagingFileInfo, len(group))
			for i, sf := range group {
				stagingFileInfos[i] = StagingFileInfo{
					ID:       sf.ID,
					Location: sf.Location,
				}
			}

			rawPayload, err := jsonrs.Marshal(WorkerJobRequestV2{
				baseWorkerJobRequest: baseReq,
				StagingFiles:         stagingFileInfos,
			})
			if err != nil {
				return fmt.Errorf("error generating raw payload: %w", err)
			}

			messages := []stdjson.RawMessage{rawPayload}
			ch, err := lf.publishToNotifier(ctx, job, messages, notifier.JobTypeUploadV2)
			if err != nil {
				return fmt.Errorf("error publishing to notifier: %w", err)
			}
			// set messages to nil to release mem allocated
			messages = nil

			gr := group // capture for goroutine
			g.Go(func() error {
				return lf.processNotifierResponseV2(ctx, ch, job, gr)
			})
		}
	}
	return g.Wait()
}

func (lf *LoadFileGenerator) destinationRevisionIDMap(ctx context.Context, job *model.UploadJob) (map[string]backendconfig.DestinationT, error) {
	revisionIDMap := make(map[string]backendconfig.DestinationT)

	for _, file := range job.StagingFiles {
		revisionID := file.DestinationRevisionID
		// No need to make config backend api call for the current config
		if revisionID == job.Warehouse.Destination.RevisionID {
			revisionIDMap[revisionID] = job.Warehouse.Destination
			continue
		}
		// No need to make config backend api call for the same revision ID
		if _, ok := revisionIDMap[revisionID]; ok {
			continue
		}
		destination, err := lf.ControlPlaneClient.DestinationHistory(ctx, revisionID)
		if err != nil {
			return nil, err
		}
		revisionIDMap[revisionID] = destination
	}
	return revisionIDMap, nil
}

func (lf *LoadFileGenerator) GetLoadFilePrefix(timeWindow time.Time, warehouse model.Warehouse) string {
	switch warehouse.Type {
	case warehouseutils.GCSDatalake:
		windowFormat := timeWindow.Format(warehouseutils.DatalakeTimeWindowFormat)

		if windowLayout := warehouse.GetStringDestinationConfig(lf.Conf, model.TimeWindowLayoutSetting); windowLayout != "" {
			windowFormat = timeWindow.Format(windowLayout)
		}
		if suffix := warehouse.GetStringDestinationConfig(lf.Conf, model.TableSuffixSetting); suffix != "" {
			windowFormat = fmt.Sprintf("%v/%v", suffix, windowFormat)
		}
		return windowFormat
	case warehouseutils.S3Datalake:
		if !schemarepository.UseGlue(&warehouse) {
			return timeWindow.Format(warehouseutils.DatalakeTimeWindowFormat)
		}
		if windowLayout := warehouse.GetStringDestinationConfig(lf.Conf, model.TimeWindowLayoutSetting); windowLayout != "" {
			return timeWindow.Format(windowLayout)
		}
	}
	return timeWindow.Format(warehouseutils.DatalakeTimeWindowFormat)
}

func logNotifierResponse(lfLogger logger.Logger, job *model.UploadJob, chunk []*model.StagingFile) {
	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	startId := chunk[0].ID
	endId := chunk[len(chunk)-1].ID
	lfLogger.Infon("Received responses for staging files from notifier",
		logger.NewIntField("startId", startId),
		logger.NewIntField("endID", endId),
		obskit.DestinationID(destID),
		obskit.DestinationType(destType),
	)
}

func toLoadFile(output LoadFileUpload, job *model.UploadJob) model.LoadFile {
	return model.LoadFile{
		TableName:             output.TableName,
		Location:              output.Location,
		TotalRows:             output.TotalRows,
		ContentLength:         output.ContentLength,
		DestinationRevisionID: output.DestinationRevisionID,
		UseRudderStorage:      output.UseRudderStorage,
		SourceID:              job.Upload.SourceID,
		DestinationID:         job.Upload.DestinationID,
		DestinationType:       job.Upload.DestinationType,
		UploadID:              &job.Upload.ID,
	}
}

type fileWithMaxSize struct {
	file    *model.StagingFile
	maxSize int64
}

// GroupStagingFiles groups staging files based on their key characteristics (UseRudderStorage, DestinationRevisionID, TimeWindow)
// and then applies size constraints within each group. The maxSizeMB parameter controls the maximum size of any table within a group.
func (lf *LoadFileGenerator) GroupStagingFiles(files []*model.StagingFile, maxSizeMB int) [][]*model.StagingFile {
	// The fields in this struct are determined by the fields being used in the generateBaseRequest
	type stagingFileGroupKey struct {
		UseRudderStorage      bool
		DestinationRevisionID string
		TimeWindow            time.Time
	}
	groups := make(map[stagingFileGroupKey][]*fileWithMaxSize)

	filesForSizing := make([]*fileWithMaxSize, len(files))
	for i, file := range files {
		key := stagingFileGroupKey{
			UseRudderStorage:      file.UseRudderStorage,
			DestinationRevisionID: file.DestinationRevisionID,
			TimeWindow:            file.TimeWindow,
		}
		maxSize := int64(0)
		for _, size := range file.BytesPerTable {
			if size > maxSize {
				maxSize = size
			}
		}
		filesForSizing[i] = &fileWithMaxSize{
			file:    file,
			maxSize: maxSize,
		}
		groups[key] = append(groups[key], filesForSizing[i])
	}

	result := make([][]*model.StagingFile, 0, len(groups))

	// For each group, apply size constraints
	for _, group := range groups {
		result = append(result, lf.groupBySize(group, maxSizeMB)...)
	}
	return result
}

// splits a group of staging files based on size constraints
func (lf *LoadFileGenerator) groupBySize(files []*fileWithMaxSize, maxSizeMB int) [][]*model.StagingFile {
	maxSizeBytes := int64(maxSizeMB) * 1024 * 1024 // Convert MB to bytes
	// Sort by the largest table size in each file
	slices.SortFunc(files, func(a, b *fileWithMaxSize) int {
		return int(b.maxSize - a.maxSize)
	})

	var result [][]*model.StagingFile
	for len(files) > 0 {
		// Start a new batch
		var currentBatch []*model.StagingFile
		batchTableSizes := make(map[string]int64)

		// Try to add files to the current batch
		i := 0
		for i < len(files) {
			// Check if adding this file would exceed size limit for any table
			canAdd := true
			for tableName, size := range files[i].file.BytesPerTable {
				newSize := batchTableSizes[tableName] + size
				if newSize > maxSizeBytes {
					canAdd = false
					break
				}
			}

			if canAdd {
				// Add file to batch and update table sizes
				currentBatch = append(currentBatch, files[i].file)
				for tableName, size := range files[i].file.BytesPerTable {
					batchTableSizes[tableName] += size
				}
				// Remove the file from remaining by moving the last element
				files[i] = files[len(files)-1]
				files = files[:len(files)-1]
			} else {
				i++
			}
		}

		// If we couldn't add any files to the batch, add the first file anyway
		// This ensures we make progress even with files larger than maxSizeBytes
		if len(currentBatch) == 0 {
			currentBatch = append(currentBatch, files[0].file)
			files = files[1:]
		}
		result = append(result, currentBatch)
	}

	return result
}

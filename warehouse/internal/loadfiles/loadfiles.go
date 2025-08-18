package loadfiles

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
	Delete(ctx context.Context, uploadID int64) error
	Get(ctx context.Context, uploadID int64) ([]model.LoadFile, error)
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

type WorkerJobResponseV2 struct {
	Output []LoadFileUpload `json:"output"`
}

type LoadFileUpload struct {
	TableName             string
	Location              string
	TotalRows             int
	ContentLength         int64
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

func (lf *LoadFileGenerator) CreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error) {
	toProcessStagingFiles := job.StagingFiles
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

	lf.Logger.Infon("[WH]: Starting batch processing stage files",
		logger.NewIntField("publishBatchSize", int64(publishBatchSize)),
		obskit.DestinationType(destType),
		obskit.DestinationID(destID))

	job.LoadFileGenStartTime = timeutil.Now()

	// Delete previous load files for the upload
	if err := lf.LoadRepo.Delete(ctx, job.Upload.ID); err != nil {
		return 0, 0, fmt.Errorf("deleting previous load files: %w", err)
	}

	// Set staging file status to executing
	stagingFileIDs := repo.StagingFileIDs(toProcessStagingFiles)
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

	// Always use V2 job creation (no more V1 vs V2 branching)
	lf.Logger.Infon("Processing staging files with V2 jobs",
		logger.NewIntField("count", int64(len(toProcessStagingFiles))))

	err = lf.createUploadV2Jobs(ctx, job, toProcessStagingFiles, publishBatchSize, uniqueLoadGenID)
	if err != nil {
		return 0, 0, fmt.Errorf("creating upload V2 jobs: %w", err)
	}

	return lf.getLoadFileIDs(ctx, job, uniqueLoadGenID)
}

func (lf *LoadFileGenerator) getLoadFileIDs(ctx context.Context, job *model.UploadJob, uniqueLoadGenID string) (int64, int64, error) {
	loadFiles, err := lf.LoadRepo.Get(ctx, job.Upload.ID)
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

func (lf *LoadFileGenerator) prepareJobRequestV2(
	job *model.UploadJob,
	uniqueLoadGenID string,
	stagingFiles []*model.StagingFile,
	destinationRevisionIDMap map[string]backendconfig.DestinationT,
) *WorkerJobRequestV2 {
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
		StagingUseRudderStorage:      stagingFiles[0].UseRudderStorage,
		DestinationRevisionID:        job.Warehouse.Destination.RevisionID,
		StagingDestinationRevisionID: stagingFiles[0].DestinationRevisionID,
	}
	if revisionConfig, ok := destinationRevisionIDMap[stagingFiles[0].DestinationRevisionID]; ok {
		baseReq.StagingDestinationConfig = revisionConfig.Config
	}
	if slices.Contains(warehouseutils.TimeWindowDestinations, job.Warehouse.Type) {
		baseReq.LoadFilePrefix = lf.GetLoadFilePrefix(stagingFiles[0].TimeWindow, job.Warehouse)
	}
	stagingFileInfos := make([]StagingFileInfo, len(stagingFiles))
	for i, sf := range stagingFiles {
		stagingFileInfos[i] = StagingFileInfo{
			ID:       sf.ID,
			Location: sf.Location,
		}
	}
	return &WorkerJobRequestV2{
		baseWorkerJobRequest: baseReq,
		StagingFiles:         stagingFileInfos,
	}
}

func (lf *LoadFileGenerator) publishToNotifier(
	ctx context.Context,
	job *model.UploadJob,
	messages []stdjson.RawMessage,
	jobType notifier.JobType,
) (<-chan *notifier.PublishResponse, error) {
	uploadSchemaJSON, err := jsonrs.Marshal(struct {
		UploadSchema model.Schema `json:"upload_schema"`
	}{
		UploadSchema: job.Upload.UploadSchema,
	})
	if err != nil {
		return nil, fmt.Errorf("error marshalling upload schema: %w", err)
	}

	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	lf.Logger.Infon("[WH]: Publishing jobs to notifier",
		logger.NewIntField("count", int64(len(messages))),
		obskit.DestinationType(destType),
		obskit.DestinationID(destID))

	ch, err := lf.Notifier.Publish(ctx, &notifier.PublishRequest{
		Payloads:     messages,
		JobType:      jobType,
		UploadSchema: uploadSchemaJSON,
		Priority:     job.Upload.Priority,
	})
	if err != nil {
		return nil, fmt.Errorf("publishing to notifier: %w", err)
	}
	return ch, nil
}

func (lf *LoadFileGenerator) processNotifierResponseV2(ctx context.Context, ch <-chan *notifier.PublishResponse, job *model.UploadJob, chunk []*model.StagingFile) error {
	responses, ok := <-ch
	if !ok {
		return fmt.Errorf("receiving notifier channel closed")
	}
	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	startId := chunk[0].ID
	endId := chunk[len(chunk)-1].ID
	lf.Logger.Infon("Received responses for staging files from notifier",
		logger.NewIntField("startId", startId),
		logger.NewIntField("endID", endId),
		obskit.DestinationID(destID),
		obskit.DestinationType(destType),
	)
	if responses.Err != nil {
		return fmt.Errorf("receiving responses from notifier: %w", responses.Err)
	}

	var loadFiles []model.LoadFile
	for _, resp := range responses.Jobs {
		var response WorkerJobResponseV2
		if err := jsonrs.Unmarshal(resp.Payload, &response); err != nil {
			return fmt.Errorf("unmarshalling response from notifier: %w", err)
		}

		if resp.Status == notifier.Aborted && resp.Error != nil {
			lf.Logger.Errorn("[WH]: Error in generating load files",
				obskit.Error(resp.Error))
			continue
		}
		if len(response.Output) == 0 {
			lf.Logger.Errorn("[WH]: No LoadFiles returned by worker")
			continue
		}
		for _, output := range response.Output {
			loadFiles = append(loadFiles, toLoadFile(output, job))
		}
	}
	if len(loadFiles) == 0 {
		return nil
	}
	if err := lf.LoadRepo.Insert(ctx, loadFiles); err != nil {
		return fmt.Errorf("inserting load files: %w", err)
	}
	stagingFileIds := repo.StagingFileIDs(chunk)
	if err := lf.StageRepo.SetStatuses(ctx, stagingFileIds, warehouseutils.StagingFileSucceededState); err != nil {
		return fmt.Errorf("setting staging file status to succeeded: %w", err)
	}
	return nil
}

func (lf *LoadFileGenerator) createUploadV2Jobs(ctx context.Context, job *model.UploadJob, stagingFiles []*model.StagingFile, publishBatchSize int, uniqueLoadGenID string) error {
	destinationRevisionIDMap, err := lf.destinationRevisionIDMap(ctx, job)
	if err != nil {
		return fmt.Errorf("populating destination revision ID: %w", err)
	}
	g, gCtx := errgroup.WithContext(ctx)
	stagingFileGroups := lf.GroupStagingFiles(stagingFiles, lf.Conf.GetInt("Warehouse.loadFiles.maxSizeInMB", 512))
	for i, fileGroups := range lo.Chunk(stagingFileGroups, publishBatchSize) {
		for j, group := range fileGroups {
			lf.Logger.Infon("Processing chunk and group",
				logger.NewIntField("chunk", int64(i)),
				logger.NewIntField("group", int64(j)),
				logger.NewIntField("size", int64(len(group))))
			jobRequest := lf.prepareJobRequestV2(job, uniqueLoadGenID, group, destinationRevisionIDMap)
			rawPayload, err := jsonrs.Marshal(jobRequest)
			if err != nil {
				return fmt.Errorf("marshalling job request: %w", err)
			}

			messages := []stdjson.RawMessage{rawPayload}
			ch, err := lf.publishToNotifier(ctx, job, messages, notifier.JobTypeUploadV2)
			if err != nil {
				return fmt.Errorf("publishing to notifier: %w", err)
			}

			gr := group // capture for goroutine
			g.Go(func() error {
				return lf.processNotifierResponseV2(gCtx, ch, job, gr)
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

// The fields in this struct are determined by the fields being used in the prepareBaseJobRequest
type stagingFileGroupKey struct {
	UseRudderStorage             bool
	StagingUseRudderStorage      bool
	DestinationRevisionID        string
	StagingDestinationRevisionID string
	TimeWindow                   time.Time
}

// GroupStagingFiles groups staging files based on their key characteristics
// and then applies size constraints within each group. The maxSizeMB parameter controls the maximum size of any table within a group.
func (lf *LoadFileGenerator) GroupStagingFiles(files []*model.StagingFile, maxSizeMB int) [][]*model.StagingFile {
	groups := lo.GroupBy(files, func(file *model.StagingFile) stagingFileGroupKey {
		return stagingFileGroupKey{
			UseRudderStorage:             file.UseRudderStorage,
			StagingUseRudderStorage:      file.UseRudderStorage,
			DestinationRevisionID:        file.DestinationRevisionID,
			StagingDestinationRevisionID: file.DestinationRevisionID,
			TimeWindow:                   file.TimeWindow,
		}
	})

	result := make([][]*model.StagingFile, 0, len(groups))
	// For each group, apply size constraints
	for _, group := range groups {
		result = append(result, lf.groupBySize(group, maxSizeMB)...)
	}
	return result
}

type tableSizeResult struct {
	sizes map[string]int64
	name  string
	size  int64
}

// groupBySize splits a group of staging files based on size constraints
func (lf *LoadFileGenerator) groupBySize(files []*model.StagingFile, maxSizeMB int) [][]*model.StagingFile {
	maxSizeBytes := int64(maxSizeMB) * 1024 * 1024 // Convert MB to bytes

	// Find the table with the maximum total size
	maxTable := lo.Reduce(files, func(acc tableSizeResult, file *model.StagingFile, _ int) tableSizeResult {
		for tableName, size := range file.BytesPerTable {
			acc.sizes[tableName] += size
			if acc.sizes[tableName] > acc.size {
				acc.name = tableName
				acc.size = acc.sizes[tableName]
			}
		}
		return acc
	}, tableSizeResult{
		sizes: make(map[string]int64),
	})

	lf.Logger.Infon("[groupBySize]", logger.NewStringField("maxTableName", maxTable.name), logger.NewIntField("maxTableSizeInBytes", maxTable.size))

	// Sorting ensures that minimum batches are created
	slices.SortFunc(files, func(a, b *model.StagingFile) int {
		// Assuming that there won't be any overflows
		return int(b.BytesPerTable[maxTable.name] - a.BytesPerTable[maxTable.name])
	})

	var result [][]*model.StagingFile
	processed := make(map[int64]bool, len(files))

	for len(processed) < len(files) {
		// Start a new batch
		var currentBatch []*model.StagingFile
		batchTableSizes := make(map[string]int64)

		// Try to add files to the current batch
		for _, file := range files {
			if processed[file.ID] {
				continue
			}

			// Check if adding this file would exceed size limit for any table
			canAdd := true
			for tableName, size := range file.BytesPerTable {
				newSize := batchTableSizes[tableName] + size
				if newSize > maxSizeBytes {
					canAdd = false
					break
				}
			}

			if canAdd {
				// Add file to batch and update table sizes
				currentBatch = append(currentBatch, file)
				for tableName, size := range file.BytesPerTable {
					batchTableSizes[tableName] += size
				}
				processed[file.ID] = true
			} else {
				// If this is the first file in this iteration and it exceeds limits,
				// add it to its own batch
				if len(currentBatch) == 0 {
					result = append(result, []*model.StagingFile{file})
					processed[file.ID] = true
					break
				}
			}
		}
		// This condition will be false if a file was already added to the batch because it exceeded the size limit
		// In that case, it should not be added again to result
		if len(currentBatch) > 0 {
			result = append(result, currentBatch)
		}
	}

	return result
}

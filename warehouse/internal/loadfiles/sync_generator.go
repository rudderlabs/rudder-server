package loadfiles

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"slices"
	"strings"
)

type SyncGenerator struct {
	Conf     *config.Config
	Logger   logger.Logger
	Notifier Notifier

	StageRepo StageFileRepo
	LoadRepo  LoadFileRepo

	ControlPlaneClient ControlPlaneClient

	publishBatchSize             int
	publishBatchSizePerWorkspace map[string]int
}

func (s *SyncGenerator) ForceCreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error) {
	return s.createFromStaging(ctx, job, job.StagingFiles)
}

func (s *SyncGenerator) CreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error) {
	return s.createFromStaging(
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

func (s *SyncGenerator) createFromStaging(ctx context.Context, job *model.UploadJob, toProcessStagingFiles []*model.StagingFile) (int64, int64, error) {
	destID := job.Upload.DestinationID
	destType := job.Upload.DestinationType
	var err error

	uniqueLoadGenID := misc.FastUUID().String()

	s.Logger.Infof("[WH]: Starting processing stage files for %s:%s", destType, destID)

	job.LoadFileGenStartTime = timeutil.Now()

	// Delete previous load files for the staging files
	stagingFileIDs := repo.StagingFileIDs(toProcessStagingFiles)
	if err := s.LoadRepo.Delete(ctx, job.Upload.ID, stagingFileIDs); err != nil {
		return 0, 0, fmt.Errorf("deleting previous load files: %w", err)
	}

	// Set staging file status to executing
	if err = s.StageRepo.SetStatuses(
		ctx,
		stagingFileIDs,
		warehouseutils.StagingFileExecutingState,
	); err != nil {
		return 0, 0, fmt.Errorf("set staging file status to executing: %w", err)
	}

	defer func() {
		// ensure that if there is an error, we set the staging file status to failed
		if err != nil {
			if errStatus := s.StageRepo.SetStatuses(
				ctx,
				stagingFileIDs,
				warehouseutils.StagingFileFailedState,
			); errStatus != nil {
				err = fmt.Errorf("%w, and also: %v", err, errStatus)
			}
		}
	}()
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(s.publishBatchSize)
	stagingFileGroups := s.GroupStagingFiles(toProcessStagingFiles, s.Conf.GetInt("Warehouse.loadFiles.maxSizeInMB", 128))
	for _, fileGroups := range stagingFileGroups {
		g.Go(func() error {
			return s.generateLoadFiles(gCtx, job, fileGroups)
		})
	}

	err = g.Wait()
	if err != nil {
		return 0, 0, fmt.Errorf("generating load files: %w", err)
	}

	return s.getLoadFileIDs(ctx, job, stagingFileIDs, uniqueLoadGenID)
}

func (s *SyncGenerator) destinationRevisionIDMap(ctx context.Context, job *model.UploadJob) (map[string]backendconfig.DestinationT, error) {
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
		destination, err := s.ControlPlaneClient.DestinationHistory(ctx, revisionID)
		if err != nil {
			return nil, err
		}
		revisionIDMap[revisionID] = destination
	}
	return revisionIDMap, nil
}

func (s *SyncGenerator) GroupStagingFiles(files []*model.StagingFile, maxSizeMB int) [][]*model.StagingFile {
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
		result = append(result, s.groupBySize(group, maxSizeMB)...)
	}
	return result
}

func (s *SyncGenerator) groupBySize(files []*model.StagingFile, maxSizeMB int) [][]*model.StagingFile {
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

	s.Logger.Infof("maxTable: %s, maxTableSize: %d", maxTable.name, maxTable.size)

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

func (s *SyncGenerator) getLoadFileIDs(ctx context.Context, job *model.UploadJob, stagingFileIDs []int64, uniqueLoadGenID string) (int64, int64, error) {
	loadFiles, err := s.LoadRepo.Get(ctx, job.Upload.ID, stagingFileIDs)
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

func (s *SyncGenerator) generateLoadFiles(ctx context.Context, job *model.UploadJob, stagingFiles []*model.StagingFile) error {
	return nil
}

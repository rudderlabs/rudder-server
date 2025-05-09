package slave

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/stagingfiles"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"slices"
	"strings"
)

var warehousesToVerifyLoadFilesFolder = []string{warehouseutils.SNOWFLAKE}

type SyncGenerator struct {
	Conf   *config.Config
	Logger logger.Logger

	StageRepo loadfiles.StageFileRepo
	LoadRepo  loadfiles.LoadFileRepo

	ControlPlaneClient loadfiles.ControlPlaneClient

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
	stagingFilesBatcher := stagingfiles.NewBatcher(s.Conf.GetInt("Warehouse.loadFiles.maxSizeInMB", 128), s.Logger)
	stagingFileGroups := stagingFilesBatcher.Batch(toProcessStagingFiles)
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

package router

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (job *UploadJob) generateLoadFiles() error {
	startLoadFileID, endLoadFileID, err := job.loadfile.CreateLoadFiles(job.ctx, job.DTO())
	if err != nil {
		return err
	}

	if err := job.setLoadFileIDs(startLoadFileID, endLoadFileID); err != nil {
		return err
	}
	if err := job.matchRowsInStagingAndLoadFiles(job.ctx); err != nil {
		return err
	}

	_ = job.recordLoadFileGenerationTimeStat(startLoadFileID, endLoadFileID)
	return nil
}

func (job *UploadJob) setLoadFileIDs(startLoadFileID, endLoadFileID int64) error {
	if startLoadFileID > endLoadFileID {
		return fmt.Errorf("end id less than start id: %d > %d", startLoadFileID, endLoadFileID)
	}

	job.upload.LoadFileStartID = startLoadFileID
	job.upload.LoadFileEndID = endLoadFileID

	return job.uploadsRepo.Update(
		job.ctx,
		job.upload.ID,
		[]repo.UpdateKeyValue{
			repo.UploadFieldStartLoadFileID(startLoadFileID),
			repo.UploadFieldEndLoadFileID(endLoadFileID),
		},
	)
}

func (job *UploadJob) matchRowsInStagingAndLoadFiles(ctx context.Context) error {
	rowsInStagingFiles, err := job.stagingFileRepo.TotalEventsForUploadID(ctx, job.upload.ID)
	if err != nil {
		return fmt.Errorf("total rows: %w", err)
	}
	rowsInLoadFiles := job.getTotalRowsInLoadFiles(ctx)
	if (rowsInStagingFiles != rowsInLoadFiles) || rowsInStagingFiles == 0 || rowsInLoadFiles == 0 {
		job.logger.Errorn(`Error: Rows count mismatch between staging and load files for upload`,
			logger.NewIntField(logfield.UploadJobID, job.upload.ID),
			logger.NewIntField("rowsInStagingFiles", rowsInStagingFiles),
			logger.NewIntField("rowsInLoadFiles", rowsInLoadFiles))
		job.stats.stagingLoadFileEventsCountMismatch.Gauge(rowsInStagingFiles - rowsInLoadFiles)
	}
	return nil
}

func (job *UploadJob) getTotalRowsInLoadFiles(ctx context.Context) int64 {
	exportedEvents, err := job.loadFilesRepo.TotalExportedEvents(ctx, job.upload.ID, []string{
		whutils.ToProviderCase(job.warehouse.Type, whutils.DiscardsTable),
	})
	if err != nil {
		job.logger.Errorn(`Getting total rows in load files`, obskit.Error(err))
		return 0
	}
	return exportedEvents
}

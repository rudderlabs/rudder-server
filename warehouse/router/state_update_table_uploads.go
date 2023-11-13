package router

import "fmt"

func (job *UploadJob) updateTableUploadsCounts() error {
	for tableName := range job.upload.UploadSchema {
		err := job.tableUploadsRepo.PopulateTotalEventsFromStagingFileIDs(
			job.ctx,
			job.upload.ID,
			tableName,
			job.stagingFileIDs,
		)
		if err != nil {
			return fmt.Errorf("populate table uploads total events from staging file: %w", err)
		}
	}
	return nil
}

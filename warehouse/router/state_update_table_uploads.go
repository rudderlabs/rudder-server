package router

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

func (job *UploadJob) updateTableUploadsCounts() error {
	return job.tableUploadsRepo.WithTx(job.ctx, func(tx *sqlquerywrapper.Tx) error {
		for tableName := range job.upload.UploadSchema {
			if err := job.tableUploadsRepo.PopulateTotalEventsWithTx(
				job.ctx,
				tx,
				job.upload.ID,
				tableName,
				job.stagingFileIDs,
			); err != nil {
				return fmt.Errorf("populate total events from staging file ids for table: %s, %w", tableName, err)
			}
		}
		return nil
	})
}

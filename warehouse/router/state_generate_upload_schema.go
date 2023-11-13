package router

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func (job *UploadJob) generateUploadSchema() error {
	uploadSchema, err := job.schemaHandle.ConsolidateStagingFilesUsingLocalSchema(job.ctx, job.stagingFiles)
	if err != nil {
		return fmt.Errorf("consolidate staging files schema using warehouse schema: %w", err)
	}

	marshalledSchema, err := json.Marshal(uploadSchema)
	if err != nil {
		return fmt.Errorf("marshal upload schema: %w", err)
	}

	err = job.uploadsRepo.Update(
		job.ctx,
		job.upload.ID,
		[]repo.UpdateKeyValue{
			repo.UploadFieldSchema(marshalledSchema),
		},
	)
	if err != nil {
		return fmt.Errorf("set upload schema: %w", err)
	}

	job.upload.UploadSchema = uploadSchema

	return nil
}

package router

import (
	"encoding/json"
	"fmt"
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

	err = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{
		{Column: UploadSchemaField, Value: marshalledSchema},
	}})
	if err != nil {
		return fmt.Errorf("set upload schema: %w", err)
	}

	job.upload.UploadSchema = uploadSchema
	return nil
}

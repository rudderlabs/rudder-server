package upload

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// UploadJobPayload represents the complete data needed to execute an upload job
type UploadJobPayload struct {
	Upload       model.Upload         `json:"upload"`
	Warehouse    model.Warehouse      `json:"warehouse"`
	StagingFiles []*model.StagingFile `json:"staging_files"`
}

// UnmarshalJSON implements custom JSON unmarshaling for UploadJobPayload
func (p *UploadJobPayload) UnmarshalJSON(data []byte) error {
	type Alias UploadJobPayload
	aux := &struct {
		*Alias
		Upload       json.RawMessage `json:"upload"`
		Warehouse    json.RawMessage `json:"warehouse"`
		StagingFiles json.RawMessage `json:"staging_files"`
	}{
		Alias: (*Alias)(p),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Unmarshal Upload
	if err := json.Unmarshal(aux.Upload, &p.Upload); err != nil {
		return err
	}

	// Unmarshal Warehouse
	if err := json.Unmarshal(aux.Warehouse, &p.Warehouse); err != nil {
		return err
	}

	// Unmarshal StagingFiles
	if err := json.Unmarshal(aux.StagingFiles, &p.StagingFiles); err != nil {
		return err
	}

	return nil
}

// Validate performs basic validation of the payload
func (p *UploadJobPayload) Validate() error {
	if p.Upload.ID == 0 {
		return fmt.Errorf("upload ID is required")
	}
	if p.Warehouse.Source.ID == "" {
		return fmt.Errorf("warehouse source ID is required")
	}
	if len(p.StagingFiles) == 0 {
		return fmt.Errorf("at least one staging file is required")
	}
	return nil
}

// NewUploadJobPayload creates a new UploadJobPayload from the given upload job
func NewUploadJobPayload(uploadJob *model.UploadJob) *UploadJobPayload {
	return &UploadJobPayload{
		Upload:       uploadJob.Upload,
		Warehouse:    uploadJob.Warehouse,
		StagingFiles: uploadJob.StagingFiles,
	}
}

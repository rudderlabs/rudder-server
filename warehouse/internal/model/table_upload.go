package model

import (
	"time"
)

type TableUpload struct {
	ID           int64
	UploadID     int64
	TableName    string
	Status       string
	Error        string
	LastExecTime time.Time
	TotalEvents  int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Location     string
}

const (
	TableUploadWaiting              = "waiting"
	TableUploadExecuting            = "executing"
	TableUploadUpdatingSchema       = "updating_schema"
	TableUploadUpdatingSchemaFailed = "updating_schema_failed"
	TableUploadUpdatedSchema        = "updated_schema"
	TableUploadExporting            = "exporting_data"
	TableUploadExportingFailed      = "exporting_data_failed"
	TableUploadExported             = "exported_data"
)

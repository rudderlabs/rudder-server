package model

import (
	"encoding/json"
	"time"
)

type UploadStatus = string

const (
	Waiting                   = "waiting"
	GeneratedUploadSchema     = "generated_upload_schema"
	CreatedTableUploads       = "created_table_uploads"
	GeneratedLoadFiles        = "generated_load_files"
	UpdatedTableUploadsCounts = "updated_table_uploads_counts"
	CreatedRemoteSchema       = "created_remote_schema"
	ExportedUserTables        = "exported_user_tables"
	ExportedData              = "exported_data"
	ExportedIdentities        = "exported_identities"
	Aborted                   = "aborted"
)

type Upload struct {
	UploadID        int64
	SourceID        string
	Namespace       string
	WorkspaceID     string
	DestinationID   string
	DestinationType string
	Status          string
	Schema          json.RawMessage
	Error           string
	FirstEventAt    time.Time
	LastEventAt     time.Time

	UseRudderStorage bool
	SourceBatchID    string
	SourceTaskID     string
	SourceTaskRunID  string
	SourceJobID      string
	SourceJobRunID   string
	LoadFileType     string
	NextRetryTime    time.Time
	Priority         int
}

package model

import (
	"database/sql"
	"encoding/json"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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

type UploadJob struct {
	Warehouse    warehouseutils.Warehouse
	Upload       Upload
	StagingFiles []*StagingFile
}

type Upload struct {
	ID                   int64
	Namespace            string
	WorkspaceID          string
	SourceID             string
	SourceType           string
	SourceCategory       string
	DestinationID        string
	DestinationType      string
	StartStagingFileID   int64
	EndStagingFileID     int64
	StartLoadFileID      int64
	EndLoadFileID        int64
	Status               string
	UploadSchema         warehouseutils.SchemaT
	MergedSchema         warehouseutils.SchemaT
	Error                json.RawMessage
	Timings              []map[string]string
	FirstAttemptAt       time.Time
	LastAttemptAt        time.Time
	Attempts             int64
	Metadata             json.RawMessage
	FirstEventAt         time.Time
	LastEventAt          time.Time
	UseRudderStorage     bool
	LoadFileGenStartTime time.Time
	TimingsObj           sql.NullString
	Priority             int
	// cloud sources specific info
	SourceBatchID   string
	SourceTaskID    string
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	LoadFileType    string
}

type AlterTableResponse struct {
	IsDependent bool // true if the column is dependent on another view or rules, false otherwise
	Query       string
}

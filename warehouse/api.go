package warehouse

import (
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type UploadReq struct {
	WorkspaceID string
	UploadId    int64
}

type UploadsRes struct {
	Uploads    []UploadRes      `json:"uploads"`
	Pagination UploadPagination `json:"pagination"`
}

type UploadPagination struct {
	Total  int32 `json:"total"`
	Limit  int32 `json:"limit"`
	Offset int32 `json:"offset"`
}
type UploadRes struct {
	ID              int64            `json:"id"`
	Namespace       string           `json:"namespace"`
	SourceID        string           `json:"source_id"`
	DestinationID   string           `json:"destination_id"`
	DestinationType string           `json:"destination_type"`
	Status          string           `json:"status"`
	Error           string           `json:"error"`
	Attempt         int32            `json:"attempt"`
	Duration        int32            `json:"duration"`
	NextRetryTime   string           `json:"nextRetryTime"`
	FirstEventAt    time.Time        `json:"first_event_at"`
	LastEventAt     time.Time        `json:"last_event_at"`
	Tables          []TableUploadRes `json:"tables,omitempty"`
}

type TablesRes struct {
	Tables []TableUploadRes `json:"tables,omitempty"`
}

type TableUploadReq struct {
	UploadID int64
	Name     string
}

type TableUploadRes struct {
	ID         int64     `json:"id"`
	UploadID   int64     `json:"upload_id"`
	Name       string    `json:"name"`
	Error      string    `json:"error"`
	Status     string    `json:"status"`
	Count      int32     `json:"count"`
	LastExecAt time.Time `json:"last_exec_at"`
	Duration   int32     `json:"duration"`
}

const ()

func InitWarehouseAPI(dbHandle *sql.DB, bcManager *backendConfigManager, log logger.Logger) error {
	return nil
}

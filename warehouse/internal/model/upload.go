package model

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type UploadStatus = string

const (
	Waiting                   = "waiting"
	GeneratedUploadSchema     = "generated_upload_schema"
	CreatedTableUploads       = "created_table_uploads"
	GeneratingLoadFiles       = "generating_load_files"
	GeneratedLoadFiles        = "generated_load_files"
	UpdatedTableUploadsCounts = "updated_table_uploads_counts"
	CreatedRemoteSchema       = "created_remote_schema"
	ExportedUserTables        = "exported_user_tables"
	ExportedData              = "exported_data"
	ExportedIdentities        = "exported_identities"
	Aborted                   = "aborted"
	Failed                    = "failed"
)

var ErrUploadNotFound = errors.New("upload not found")

type Upload struct {
	ID          int64
	WorkspaceID string

	Namespace       string
	SourceID        string
	DestinationID   string
	DestinationType string
	Status          string
	Error           json.RawMessage
	FirstEventAt    time.Time
	LastEventAt     time.Time

	UseRudderStorage bool
	SourceTaskRunID  string
	SourceJobID      string
	SourceJobRunID   string
	LoadFileType     string
	NextRetryTime    time.Time
	Priority         int
	Retried          bool

	StagingFileStartID int64
	StagingFileEndID   int64

	LoadFileStartID int64
	LoadFileEndID   int64

	Timings        Timings
	FirstAttemptAt time.Time
	LastAttemptAt  time.Time
	Attempts       int64

	UploadSchema Schema
	MergedSchema Schema
}

type Timings []map[string]time.Time

type Schema = warehouseutils.SchemaT

type UploadJobsStats struct {
	PendingJobs    int64
	PickupLag      time.Duration
	PickupWaitTime time.Duration
}

type UploadJob struct {
	Warehouse            warehouseutils.Warehouse
	Upload               Upload
	StagingFiles         []*StagingFile
	LoadFileGenStartTime time.Time
}

func GetLastFailedStatus(timingsMap Timings) (status string) {
	if len(timingsMap) > 0 {
		for index := len(timingsMap) - 1; index >= 0; index-- {
			for s := range timingsMap[index] {
				if strings.Contains(s, Failed) {
					return s
				}
			}
		}
	}
	return // zero values
}

func GetLoadFileGenTime(timingsMap Timings) (t time.Time) {
	if len(timingsMap) > 0 {
		for index := len(timingsMap) - 1; index >= 0; index-- {
			for s, t := range timingsMap[index] {
				if strings.Contains(s, GeneratingLoadFiles) {
					return t
				}
			}
		}
	}
	return // zero values
}

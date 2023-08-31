package model

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
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
	ExportedData              = "exported_data"
	ExportingData             = "exporting_data"
	ExportingDataFailed       = "exporting_data_failed"
	Aborted                   = "aborted"
	Failed                    = "failed"
)

const (
	PermissionError           JobErrorType = "permission_error"
	AlterColumnError          JobErrorType = "alter_column_error"
	ResourceNotFoundError     JobErrorType = "resource_not_found_error"
	ColumnCountError          JobErrorType = "column_count_error"
	ColumnSizeError           JobErrorType = "column_size_error"
	InsufficientResourceError JobErrorType = "insufficient_resource_error"
	ConcurrentQueriesError    JobErrorType = "concurrent_queries_error"
	UnknownError              JobErrorType = "unknown_error"
	Noop                      JobErrorType = "noop"
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
}

type Timings []map[string]time.Time

type UploadJobsStats struct {
	PendingJobs    int64
	PickupLag      time.Duration
	PickupWaitTime time.Duration
}

type UploadJob struct {
	Warehouse            Warehouse
	Upload               Upload
	StagingFiles         []*StagingFile
	LoadFileGenStartTime time.Time
}

type PendingTableUpload struct {
	UploadID      int64
	DestinationID string
	Namespace     string
	TableName     string
	Status        string
	Error         string
}

type Matcher interface {
	MatchString(string) bool
}

type JobErrorType string

type JobError struct {
	Type   JobErrorType
	Format Matcher
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

type AlterTableResponse struct {
	IsDependent bool // true if the column is dependent on another view or rules, false otherwise
	Query       string
}

type RetryOptions struct {
	WorkspaceID     string
	SourceIDs       []string
	DestinationID   string
	DestinationType string
	IntervalInHours int64
	UploadIds       []int64
	ForceRetry      bool
}

type SyncUploadOptions struct {
	SourceIDs       []string
	DestinationID   string
	DestinationType string
	Status          string
	UploadID        int64
}

type LatestUploadInfo struct {
	ID       int64
	Status   string
	Priority int
}

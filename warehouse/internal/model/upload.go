package model

import (
	"encoding/json"
	"errors"
	"slices"
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

type JobErrorType = string

const (
	UncategorizedError        JobErrorType = "uncategorised"
	PermissionError           JobErrorType = "permission_error"
	AlterColumnError          JobErrorType = "alter_column_error"
	ResourceNotFoundError     JobErrorType = "resource_not_found_error"
	ColumnCountError          JobErrorType = "column_count_error"
	ColumnSizeError           JobErrorType = "column_size_error"
	InsufficientResourceError JobErrorType = "insufficient_resource_error"
	ConcurrentQueriesError    JobErrorType = "concurrent_queries_error"
)

var userFriendlyJobErrorCategoryMap = map[JobErrorType]string{
	UncategorizedError:        "Uncategorized error",
	PermissionError:           "Permission error",
	AlterColumnError:          "Alter column error",
	ResourceNotFoundError:     "Resource not found error",
	ColumnCountError:          "Column count error",
	ColumnSizeError:           "Column size error",
	InsufficientResourceError: "Insufficient resource error",
	ConcurrentQueriesError:    "Concurrent queries error",
}

type JobError struct {
	Type   JobErrorType
	Format interface {
		MatchString(string) bool
	}
}

// GetUserFriendlyJobErrorCategory returns the user friendly error category for the given error type
func GetUserFriendlyJobErrorCategory(errorType JobErrorType) string {
	if errorMessage, ok := userFriendlyJobErrorCategoryMap[errorType]; ok {
		return errorMessage
	}
	return "Uncategorized error"
}

var (
	ErrUploadNotFound     = errors.New("upload not found")
	ErrSourcesJobNotFound = errors.New("sources job not found")
	ErrLoadFileNotFound   = errors.New("load file not found")
	ErrNoUploadsFound     = errors.New("no uploads found")
)

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

	// PartitionModes is keyed by table, so a fixed-partition table does not
	// change how other tables in the same upload are written.
	PartitionModes         map[string]string
	PartitionGranularities map[string]string
}

// PartitionModeForTable reads a table-specific mode, falling back to the
// destination's default. Both fields are optional for older destinations.
func PartitionModeForTable(config map[string]any, table string) string {
	for _, key := range []string{"partitionModes", "partitionMode"} {
		if modes, ok := config[key].(map[string]any); ok {
			if mode, ok := modes[table].(string); ok {
				return mode
			}
		}
		if modes, ok := config[key].(map[string]string); ok {
			if mode, ok := modes[table]; ok {
				return mode
			}
		}
	}
	if mode, ok := config["partitionMode"].(string); ok {
		return mode
	}
	return ""
}

// PartitionGranularityForTable resolves a table-specific partition type before
// the destination default. Empty values preserve the caller's default (day).
func PartitionGranularityForTable(config map[string]any, table string) string {
	for _, key := range []string{"partitionTypes", "partitionGranularities", "partitionType", "partitionGranularity"} {
		if types, ok := config[key].(map[string]any); ok {
			if value, ok := types[table].(string); ok && value != "" {
				return value
			}
		}
		if types, ok := config[key].(map[string]string); ok && types[table] != "" {
			return types[table]
		}
	}
	for _, key := range []string{"partitionType", "partitionGranularity"} {
		if value, ok := config[key].(string); ok {
			return value
		}
	}
	return ""
}

type PendingTableUpload struct {
	UploadID      int64
	DestinationID string
	Namespace     string
	TableName     string
	Status        string
	Error         string
}

func GetLastFailedStatus(timingsMap Timings) (status string) {
	if len(timingsMap) > 0 {
		for _, t := range slices.Backward(timingsMap) {
			for s := range t {
				if strings.Contains(s, Failed) {
					return s
				}
			}
		}
	}
	return status // zero values
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
	WorkspaceID     string
	DestinationType string
	Status          string
	UploadID        int64
}

type LatestUploadInfo struct {
	ID       int64
	Status   string
	Priority int
}

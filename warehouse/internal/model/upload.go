package model

import (
	"encoding/json"
	"errors"
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
	SourceBatchID    string
	SourceTaskID     string
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

type Schema map[string]map[string]string

type UploadJobsStats struct {
	PendingJobs    int64
	PickupLag      time.Duration
	PickupWaitTime time.Duration
}

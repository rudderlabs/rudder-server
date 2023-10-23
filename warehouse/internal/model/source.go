package model

import (
	"encoding/json"
	"time"
)

type SourceJobType = string

const (
	SourceJobTypeDeleteByJobRunID SourceJobType = "deletebyjobrunid"
)

type SourceJobStatus = string

const (
	SourceJobStatusWaiting   SourceJobStatus = "waiting"
	SourceJobStatusExecuting SourceJobStatus = "executing"
	SourceJobStatusFailed    SourceJobStatus = "failed"
	SourceJobStatusAborted   SourceJobStatus = "aborted"
	SourceJobStatusSucceeded SourceJobStatus = "succeeded"
)

type SourceJob struct {
	ID int64

	SourceID      string
	DestinationID string
	WorkspaceID   string

	TableName string

	Status  SourceJobStatus
	Error   error
	JobType SourceJobType

	Metadata json.RawMessage
	Attempts int64

	CreatedAt time.Time
	UpdatedAt time.Time
}

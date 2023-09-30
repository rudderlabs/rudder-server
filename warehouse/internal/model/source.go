package model

import (
	"encoding/json"
	"time"
)

type SourceJobType = string

const (
	DeleteByJobRunID SourceJobType = "deletebyjobrunid"
)

type SourceJobStatus = string

const (
	SourceJobStatusWaiting   SourceJobStatus = "waiting"
	SourceJobStatusExecuting SourceJobStatus = "executing"
	SourceJobStatusSucceeded SourceJobStatus = "succeeded"
	SourceJobStatusAborted   SourceJobStatus = "aborted"
	SourceJobStatusFailed    SourceJobStatus = "failed"
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

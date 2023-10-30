package model

import (
	"encoding/json"
	"time"
)

type SourceJobType interface {
	String() string
	sourceJobTypeProtected()
}

type sourceJobType string

func (s sourceJobType) String() string          { return string(s) }
func (s sourceJobType) sourceJobTypeProtected() {}

var (
	SourceJobTypeDeleteByJobRunID SourceJobType = sourceJobType("deletebyjobrunid")
)

type SourceJobStatus interface {
	String() string
	sourceJobStatusProtected()
}

type sourceJobStatus string

func (s sourceJobStatus) String() string            { return string(s) }
func (s sourceJobStatus) sourceJobStatusProtected() {}

var (
	SourceJobStatusWaiting   SourceJobStatus = sourceJobStatus("waiting")
	SourceJobStatusExecuting SourceJobStatus = sourceJobStatus("executing")
	SourceJobStatusFailed    SourceJobStatus = sourceJobStatus("failed")
	SourceJobStatusAborted   SourceJobStatus = sourceJobStatus("aborted")
	SourceJobStatusSucceeded SourceJobStatus = sourceJobStatus("succeeded")
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

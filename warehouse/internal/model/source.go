package model

import (
	"encoding/json"
	"fmt"
	"time"
)

type SourceJobType interface {
	String() string
	sourceJobTypeProtected()
}

type sourceJobType string

func (s sourceJobType) String() string          { return string(s) }
func (s sourceJobType) sourceJobTypeProtected() {}

var SourceJobTypeDeleteByJobRunID SourceJobType = sourceJobType("deletebyjobrunid")

func FromSourceJobType(jobType string) (SourceJobType, error) {
	switch jobType {
	case SourceJobTypeDeleteByJobRunID.String():
		return SourceJobTypeDeleteByJobRunID, nil
	default:
		return nil, fmt.Errorf("invalid job type %s", jobType)
	}
}

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

func FromSourceJobStatus(status string) (SourceJobStatus, error) {
	switch status {
	case SourceJobStatusWaiting.String():
		return SourceJobStatusWaiting, nil
	case SourceJobStatusExecuting.String():
		return SourceJobStatusExecuting, nil
	case SourceJobStatusFailed.String():
		return SourceJobStatusFailed, nil
	case SourceJobStatusAborted.String():
		return SourceJobStatusAborted, nil
	case SourceJobStatusSucceeded.String():
		return SourceJobStatusSucceeded, nil
	default:
		return nil, fmt.Errorf("invalid job status %s", status)
	}
}

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

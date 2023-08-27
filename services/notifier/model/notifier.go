package model

import (
	"encoding/json"
	"time"
)

type JobType string

const (
	JobTypeUpload JobType = "upload"
	JobTypeAsync  JobType = "async_job"
)

// Job a domain model for a notifier.
type Job struct {
	ID                  int64
	BatchID             string
	WorkerID            string
	WorkspaceIdentifier string

	Attempt  int
	Status   string
	Type     JobType
	Priority int
	Error    error

	Payload json.RawMessage

	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastExecTime time.Time
}

type JobMetadata json.RawMessage

type PublishRequest struct {
	Payloads        []json.RawMessage
	PayloadMetadata json.RawMessage
	JobType         JobType
	Priority        int
}

type PublishResponse struct {
	Jobs        []Job
	JobMetadata JobMetadata
	Err         error
}

type ClaimJob struct {
	Job         *Job
	JobMetadata JobMetadata
}

type ClaimJobResponse struct {
	Payload json.RawMessage
	Err     error
}

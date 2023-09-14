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

type JobStatus string

const (
	Waiting   JobStatus = "waiting"
	Executing JobStatus = "executing"
	Succeeded JobStatus = "succeeded"
	Failed    JobStatus = "failed"
	Aborted   JobStatus = "aborted"
)

type Job struct {
	ID                  int64
	BatchID             string
	WorkerID            string
	WorkspaceIdentifier string

	Attempt  int
	Status   JobStatus
	Type     JobType
	Priority int
	Error    error

	Payload json.RawMessage

	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastExecTime time.Time
}

type PublishRequest struct {
	Payloads     []json.RawMessage
	UploadSchema json.RawMessage // ATM Hack to support merging schema with the payload at the postgres level
	JobType      JobType
	Priority     int
}

type PublishResponse struct {
	Jobs []Job
	Err  error
}

type ClaimJob struct {
	Job *Job
}

type ClaimJobResponse struct {
	Payload json.RawMessage
	Err     error
}

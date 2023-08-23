package model

import (
	"encoding/json"
	"time"
)

const (
	Waiting   = "waiting"
	Executing = "executing"
	Succeeded = "succeeded"
	Failed    = "failed"
	Aborted   = "aborted"
)

type Payload json.RawMessage

// Job a domain model for a notifier.
type Job struct {
	ID                  int64
	BatchID             string
	WorkerID            string
	WorkspaceIdentifier string

	Attempt  int
	Status   string
	Type     string
	Priority int
	Error    error

	Payload Payload

	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastExecTime time.Time
}

type PublishRequest struct {
	Payloads []Payload
	JobType  string
	Schema   json.RawMessage
	Priority int
}

type PublishResponse struct {
	Notifiers []Job
	Err       error
}

type ClaimResponse struct {
	Payload Payload
	Err     error
}

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

// Notifier a domain model for a notifier.
type Notifier struct {
	ID                  int64
	BatchID             string
	WorkerID            string
	WorkspaceIdentifier string

	Attempt  int
	Status   string
	JobType  string
	Priority int
	Error    error

	Payload Payload

	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastExecTime time.Time
}

type PublishRequest struct {
	Jobs     []Payload
	Type     string
	Schema   json.RawMessage
	Priority int
}

type PublishResponse struct {
	Notifiers []Notifier
	Err       error
}

type ClaimResponse struct {
	Payload json.RawMessage
	Err     error
}

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

// Notifier a domain model for a notifier.
type Notifier struct {
	ID          int64
	BatchID     string
	WorkerID    string
	WorkspaceID string

	Attempt  int
	Status   string
	JobType  string
	Priority int
	Error    error

	Payload json.RawMessage

	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastExecTime time.Time
}

type Payload json.RawMessage

type PublishPayload struct {
	Jobs     []Payload
	Type     string
	Schema   json.RawMessage
	Priority int
}

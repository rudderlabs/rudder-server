//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

import (
	"encoding/json"
	"time"
)

type GatewayJob struct {
	JobID       uint64 `objectbox:"id"`
	UserID      string
	JobState    *JobState `objectbox:"link"`
	WorkspaceID string

	CreatedAt    time.Time `objectbox:"date"`
	ExpireAt     time.Time `objectbox:"date"`
	EventCount   int
	EventPayload json.RawMessage
	PayloadSize  int64

	// job state attributes
	ExecTime      time.Time `objectbox:"date"`
	RetryTime     time.Time `objectbox:"date"`
	ErrorResponse json.RawMessage

	// connection attributes
	SourceID           string
	SourceBatchID      string
	SourceTaskID       string
	SourceTaskRunID    string
	SourceJobID        string
	SourceJobRunID     string
	SourceDefinitionID string
}

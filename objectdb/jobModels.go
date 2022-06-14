//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

import (
	"encoding/json"
	"time"
)

type Job struct {
	//JobID -> Id
	JobID uint64 `objectbox:"id"`

	// Generic Attributes - Aggregation
	UserID      *UserID      `objectbox:"link"`
	CustomVal   *CustomVal   `objectbox:"link"`
	JobState    *JobState    `objectbox:"link"`
	WorkspaceID *WorkspaceID `objectbox:"link"`
	// ErrorCode

	// Attributes particular to a job
	// UUID         uuid.UUID `objectbox:"unique"`
	// UUID commented because: can't prepare bindings for models.go: unknown type uuid.UUID on property UUID found in Job
	CreatedAt    time.Time `objectbox:"date"`
	ExpireAt     time.Time `objectbox:"date"`
	EventCount   int
	EventPayload json.RawMessage
	PayloadSize  int64

	// ignoring job-parameters because other fields cover it
	// Parameters   json.RawMessage

	// Job State attributes
	ExecTime      time.Time `objectbox:"date"`
	RetryTime     time.Time `objectbox:"date"`
	ErrorResponse json.RawMessage
	// ignoring stateparameters as well
	// StateParameters json.RawMessage

	// connection attributes
	SourceID                *SourceID                `objectbox:"link"`
	DestinationID           *DestinationID           `objectbox:"link"`
	SourceBatchID           *SourceBatchID           `objectbox:"link"`
	SourceTaskID            *SourceTaskID            `objectbox:"link"`
	SourceTaskRunID         *SourceTaskRunID         `objectbox:"link"`
	SourceJobID             *SourceJobID             `objectbox:"link"`
	SourceJobRunID          *SourceJobRunID          `objectbox:"link"`
	SourceDefinitionID      *SourceDefinitionID      `objectbox:"link"`
	DestinationDefinitionID *DestinationDefinitionID `objectbox:"link"`
	SourceCategory          *SourceCategory          `objectbox:"link"`
}

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
	UserID      *UserID      `objectbox:"index:hash64"`
	CustomVal   *CustomVal   `objectbox:"index:hash64"`
	JobState    *JobState    `objectbox:"index:hash64"`
	WorkspaceID *WorkspaceID `objectbox:"index:hash64"`
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
	SourceID                *SourceID
	DestinationID           *DestinationID
	SourceBatchID           *SourceBatchID
	SourceTaskID            *SourceTaskID
	SourceTaskRunID         *SourceTaskRunID
	SourceJobID             *SourceJobID
	SourceJobRunID          *SourceJobRunID
	SourceDefinitionID      *SourceDefinitionID
	DestinationDefinitionID *DestinationDefinitionID
	SourceCategory          *SourceCategory
}

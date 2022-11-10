package model

import (
	"encoding/json"
	"time"
)

type StagingFile struct {
	ID                    int64
	WorkspaceID           string
	Location              string
	SourceID              string
	DestinationID         string
	Schema                json.RawMessage
	Status                string // enum
	Error                 error
	FirstEventAt          time.Time
	LastEventAt           time.Time
	UseRudderStorage      bool
	DestinationRevisionID string
	TotalEvents           int
	// cloud sources specific info
	SourceBatchID   string
	SourceTaskID    string
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	TimeWindow      time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

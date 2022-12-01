package model

import (
	"encoding/json"
	"time"
)

// StagingFile a domain model for a staging file.
//
//	The staging file contains events that should be loaded into a warehouse.
//	It is located in a cloud storage bucket.
//	The model includes ownership, file location, schema for included events, and other metadata.
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

func (s *StagingFile) PrimaryID() int64 {
	return s.ID
}

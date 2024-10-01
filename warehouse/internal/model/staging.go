package model

import (
	"encoding/json"
	"time"
)

// StagingFile a domain model for a staging file.
//
//	The staging file contains events that should be loaded into a warehouse.
//	It is located in a cloud storage bucket.
//	The model includes ownership, file location, and other metadata.
type StagingFile struct {
	ID                    int64
	WorkspaceID           string
	Location              string
	SourceID              string
	DestinationID         string
	Status                string // enum
	Error                 error
	FirstEventAt          time.Time
	LastEventAt           time.Time
	UseRudderStorage      bool
	DestinationRevisionID string
	TotalEvents           int
	TotalBytes            int
	// cloud sources specific info
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	TimeWindow      time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// StagingFileWithSchema is a StagingFile with schema field for included events.
//
//	schema size can be huge, and thus it should be included only when required.
type StagingFileWithSchema struct {
	StagingFile
	Schema json.RawMessage
}

func (s StagingFile) WithSchema(schema json.RawMessage) StagingFileWithSchema {
	return StagingFileWithSchema{
		StagingFile: s,
		Schema:      schema,
	}
}

type EventTimeRange struct {
	FirstEventAt time.Time
	LastEventAt  time.Time
}

package model

import "time"

type LoadFile struct {
	ID                    int64
	TableName             string
	Location              string
	TotalRows             int
	ContentLength         int64
	StagingFileID         int64
	DestinationRevisionID string
	UseRudderStorage      bool
	SourceID              string
	DestinationID         string
	DestinationType       string
	CreatedAt             time.Time
	// It is currently defined as a pointer (*int64) to support NULL values
	// during the migration phase. This is temporary - once all existing load
	// files are backfilled with their corresponding upload_id values, this
	// field will be changed to a non-nullable int64.
	UploadID *int64
}

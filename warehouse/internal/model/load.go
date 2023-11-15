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
}

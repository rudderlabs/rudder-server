package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type StagingFileSchemaSnapshot struct {
	ID            uuid.UUID
	Schema        json.RawMessage
	SourceID      string
	DestinationID string
	WorkspaceID   string
	CreatedAt     time.Time
}

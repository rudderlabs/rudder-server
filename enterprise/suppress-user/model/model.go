package model

import (
	"errors"
	"time"
)

var (
	ErrRestoring    = errors.New("repository is restoring")
	ErrNotSupported = errors.New("operation not supported")
	SyncDoneMarker  = ".sync_done"
	ErrNoValueFound = errors.New("no value found")
)
var Wildcard = "*"

type Suppression struct {
	WorkspaceID string    `json:"workspaceId"`
	Canceled    bool      `json:"canceled"`
	UserID      string    `json:"userId"`
	CreatedAt   time.Time `json:"createdAt"`
	SourceIDs   []string  `json:"sourceIds"`
}

type Metadata struct {
	CreatedAt time.Time
}

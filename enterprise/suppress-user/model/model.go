package model

import "errors"

var (
	ErrRestoring    = errors.New("repository is restoring")
	ErrNotSupported = errors.New("operation not supported")
)
var Wildcard = "*"

type Suppression struct {
	WorkspaceID string   `json:"workspaceId"`
	Canceled    bool     `json:"canceled"`
	UserID      string   `json:"userId"`
	SourceIDs   []string `json:"sourceIds"`
}

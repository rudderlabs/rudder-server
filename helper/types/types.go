package types

import (
	"time"
)

type DebugConfig struct {
	StartTime time.Time // should be in RFC3339 format
	EndTime   time.Time // should be in RFC3339 format
}

type DebuggableMetaInfo struct {
	Enabled        bool
	DestinationIDs []string
	DestTypes      []string
	WorkspaceIDs   []string
	EventNames     []string
}

type MetaInfo struct {
	DestinationID string `json:"destinationId"`
	WorkspaceID   string `json:"workspaceId"`
	DestType      string `json:"destType"`
	EventName     string `json:"eventName"`
}

type ResponseLogDetails struct {
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
	Headers    string `json:"headers"`
}

type DebugFields struct {
	Input    any      `json:"request"`
	Output   any      `json:"response"`
	Metainfo MetaInfo `json:"metadata"`
}

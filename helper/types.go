package helper

import "time"

type DebuggableMetaInfo struct {
	Enabled         bool
	DestinationIDs  []string
	DestTypes       []string
	WorkspaceIDs    []string
	EventNames      []string
	StartTime       time.Time // should be in RFC3339 format
	EndTime         time.Time // should be in RFC3339 format
	MaxBatchSize    int
	MaxBatchTimeout time.Duration
}

type MetaInfo struct {
	DestinationID string `json:"destinationId"`
	WorkspaceID   string `json:"workspaceId"`
	DestType      string `json:"destType"`
	EventName     string `json:"eventName"`
}

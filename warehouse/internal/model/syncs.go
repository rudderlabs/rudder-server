package model

import (
	"time"
)

type UploadInfo struct {
	ID               int64
	SourceID         string
	DestinationID    string
	DestinationType  string
	Namespace        string
	Error            string
	Attempt          int64
	Status           string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	FirstEventAt     time.Time
	LastEventAt      time.Time
	LastExecAt       time.Time
	NextRetryTime    time.Time
	Duration         time.Duration
	IsArchivedUpload bool
}

type TableUploadInfo struct {
	ID         int64
	UploadID   int64
	Name       string
	Status     string
	Error      string
	LastExecAt time.Time
	Count      int64
	Duration   int64
}

type RetrieveFailedBatchesRequest struct {
	DestinationID string
	WorkspaceID   string
	Start         time.Time
	End           time.Time
}

type RetrieveFailedBatchesResponse struct {
	ErrorCategory   string
	SourceID        string
	Status          string
	TotalEvents     int64
	TotalSyncs      int64
	LastHappenedAt  time.Time
	FirstHappenedAt time.Time
	Error           string
}

type RetryFailedBatchesRequest struct {
	DestinationID string
	WorkspaceID   string
	Start         time.Time
	End           time.Time
	ErrorCategory string
	SourceID      string
	Status        string
}

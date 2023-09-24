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
	IntervalInHrs int64
}

type RetrieveFailedBatchesResponse struct {
	ErrorCategory string
	SourceID      string
	Count         int64
	LastHappened  time.Time
	Status        string
}

type RetrieveFailedBatchRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
	ErrorCategory string
	SourceID      string
	Status        string
}

type RetrieveFailedBatchResponse struct {
	Error         string
	ErrorCategory string
	SourceID      string
	Status        string
	LastHappened  time.Time
}

type RetryFailedBatchesRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
}

type RetryFailedBatchRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
	ErrorCategory string
	SourceID      string
	Status        string
}

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

type FailedBatchRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
}

type FailedBatchResponse struct {
	ErrorCategory string
	SourceID      string
	Count         int64
	LastHappened  time.Duration
	Status        string
}

type FailedBatchDetailsRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
	ErrorCategory string
	SourceID      string
	Status        string
}

type FailedBatchDetailsResponse struct {
	ErrorCategory string
	SourceID      string
	Count         int64
	LastHappened  time.Duration
	Status        string
	Error         string
}

type RetryFailedBatchesRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
}

type RetryFailedBatchesResponse struct {
	ErrorCategory string
	SourceID      string
	Count         int64
	LastHappened  time.Duration
	Status        string
}

type RetryFailedBatchRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
	ErrorCategory string
	SourceID      string
	Status        string
}

type RetryFailedBatchResponse struct {
	ErrorCategory string
	SourceID      string
	Count         int64
	LastHappened  time.Duration
	Status        string
	Error         string
}

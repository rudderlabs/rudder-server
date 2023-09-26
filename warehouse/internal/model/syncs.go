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
	Status        string
	TotalEvents   int64
	UpdatedAt     time.Time
	Error         string
}

type RetryAllFailedBatchesRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
}

type RetrySpecificFailedBatchRequest struct {
	DestinationID string
	WorkspaceID   string
	IntervalInHrs int64
	ErrorCategory string
	SourceID      string
	Status        string
}

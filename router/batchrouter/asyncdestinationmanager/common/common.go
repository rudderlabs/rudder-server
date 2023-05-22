package common

import (
	stdjson "encoding/json"
)

type AsyncStatusResponse struct {
	Success        bool
	StatusCode     int
	HasFailed      bool
	HasWarning     bool
	FailedJobsURL  string
	WarningJobsURL string
}

type AsyncUploadOutput struct {
	Key                 string
	ImportingJobIDs     []int64
	ImportingParameters stdjson.RawMessage
	SuccessJobIDs       []int64
	FailedJobIDs        []int64
	SucceededJobIDs     []int64
	SuccessResponse     string
	FailedReason        string
	AbortJobIDs         []int64
	AbortReason         string
	ImportingCount      int
	FailedCount         int
	AbortCount          int
	DestinationID       string
}

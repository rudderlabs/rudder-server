package model

import (
	"errors"
	"time"
)

var (
	ErrDestTypeNotFound = errors.New("destination type not found for the destination ID")
	ErrDestDetail       = errors.New("error while getting destination details")
	ErrNoRunnableJob    = errors.New("no runnable job found")
)

type JobStatus string

const (
	JobStatusUndefined JobStatus = ""
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusComplete  JobStatus = "complete"
	JobStatusFailed    JobStatus = "failed"
)

type Job struct {
	ID             int
	WorkspaceID    string
	DestinationID  string
	Status         JobStatus
	UserAttributes []UserAttribute
	UpdatedAt      time.Time
}

type UserAttribute struct {
	UserID string
	Phone  *string
	Email  *string
}

type Destination struct {
	Config        map[string]interface{}
	DestinationID string
	Type          string
	Name          string
}

// type ConfigT struct {
// 	BucketName  interface{}
// 	Prefix      interface{}
// 	AccessKeyID interface{}
// 	AccessKey   interface{}
// 	EnableSSE   interface{}
// }

type APIReqErr struct {
	StatusCode int
	Body       string
	Err        error
}

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
	Phone  string
	Email  string
}

type Destination struct {
	DestinationID string
	Type          string
	Credentials   string
}

type APIReqErr struct {
	StatusCode int
	Body       string
	Err        error
}

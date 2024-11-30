package model

import (
	"errors"
	"time"
)

var (
	ErrNoRunnableJob      = errors.New("no runnable job found")
	ErrInvalidDestination = errors.New("invalid destination")
	ErrRequestTimeout     = errors.New("request timeout")
	ErrDestNotSupported   = errors.New("destination not supported")
)

type Status string

type JobStatus struct {
	Status Status
	Error  error
}

const (
	JobStatusPending  Status = "pending"
	JobStatusRunning  Status = "running"
	JobStatusComplete Status = "complete"
	JobStatusFailed   Status = "failed"
	JobStatusAborted  Status = "aborted"
)

type Job struct {
	ID             int
	WorkspaceID    string
	DestinationID  string
	Status         JobStatus
	Users          []User
	UpdatedAt      time.Time
	FailedAttempts int
}

type User struct {
	ID         string
	Attributes map[string]string
}

type Destination struct {
	Config        map[string]interface{}
	DestDefConfig map[string]interface{}
	DestinationID string
	Name          string
}

type APIReqErr struct {
	StatusCode int
	Body       string
	Err        error
}

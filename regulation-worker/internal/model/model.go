package model

import (
	"errors"
	"time"
)

var (
	ErrDestTypeNotFound   = errors.New("destination type not found for the destination ID")
	ErrDestDetail         = errors.New("error while getting destination details")
	ErrNoRunnableJob      = errors.New("no runnable job found")
	ErrDestNotImplemented = errors.New("job deletion not implemented for the destination")
	ErrInvalidDestination = errors.New("invalid destination")
	ErrRequestTimeout     = errors.New("request timeout")
)

type JobStatus string

const (
	JobStatusUndefined    JobStatus = ""
	JobStatusPending      JobStatus = "pending"
	JobStatusRunning      JobStatus = "running"
	JobStatusComplete     JobStatus = "complete"
	JobStatusFailed       JobStatus = "failed"
	JobStatusNotSupported JobStatus = "unsupported"
	JobStatusAborted      JobStatus = "aborted"
)

type Job struct {
	ID            int
	WorkspaceID   string
	DestinationID string
	Status        JobStatus
	Users         []User
	UpdatedAt     time.Time
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

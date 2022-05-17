package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
)

//go:generate mockgen -source=rsources.go -destination=mock_rsources.go -package=rsources github.com/rudderlabs/rudder-server/services/rsources JobService

type JobFilter struct {
	TaskRunId []string
	SourceId  []string
}

type JobTargetKey struct {
	taskRunId     string
	sourceId      string
	destinationId string
}

type Stats struct {
	In     uint `json:"in"`
	Out    uint `json:"out"`
	Failed uint `json:"failed"`
}

type JobStatus struct {
	ID          string       `json:"id"`
	TasksStatus []TaskStatus `json:"tasks"`
}

type TaskStatus struct {
	ID            string         `json:"id"`
	SourcesStatus []SourceStatus `json:"sources"`
}

type SourceStatus struct {
	ID                 string              `json:"id"`
	Completed          bool                `json:"completed"`
	Stats              Stats               `json:"stats"`
	DestinationsStatus []DestinationStatus `json:"destinations"`
}

type DestinationStatus struct {
	ID        string `json:"id"`
	Completed bool   `json:"completed"`
	Stats     Stats  `json:"stats"`
}

type FailedRecords struct{}

// JobService manages information about jobs created by rudder-sources
type JobService interface {

	// Delete deletes all relevant information for a given jobRunId
	Delete(ctx context.Context, jobRunId string) error

	// GetStatus gets the current status of a job
	GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error)

	// IncrementStats increments the existing statistic counters
	// for a specific job measurement.
	IncrementStats(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, stats Stats) error

	// TODO: future extension
	AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) error

	// TODO: future extension
	GetFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, filter JobFilter) (FailedRecords, error)
}

func NewNoOpService() JobService {
	return &noopService{}
}

type noopService struct {
}

func (*noopService) Delete(_ context.Context, _ string) error {
	return nil
}

func (*noopService) GetStatus(_ context.Context, _ string, _ JobFilter) (JobStatus, error) {
	return JobStatus{}, nil
}

func (*noopService) IncrementStats(_ context.Context, _ *sql.Tx, _ string, _ JobTargetKey, _ Stats) error {
	return nil
}

func (*noopService) AddFailedRecords(_ context.Context, _ *sql.Tx, _ string, _ JobTargetKey, _ []json.RawMessage) error {
	return nil
}

func (*noopService) GetFailedRecords(_ context.Context, _ *sql.Tx, _ string, _ JobFilter) (FailedRecords, error) {
	return FailedRecords{}, nil
}

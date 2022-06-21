package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

//go:generate mockgen -source=rsources.go -destination=mock_rsources.go -package=rsources github.com/rudderlabs/rudder-server/services/rsources JobService

type JobFilter struct {
	TaskRunID []string
	SourceID  []string
}

type JobTargetKey struct {
	TaskRunID     string `json:"source_task_run_id"`
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

type Stats struct {
	In     uint `json:"in"`
	Out    uint `json:"out"`
	Failed uint `json:"failed"`
}

func (r Stats) completed() bool {
	return r.In == r.Out+r.Failed
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

func (sourceStatus *SourceStatus) calculateCompleted() {
	if !sourceStatus.Stats.completed() {
		sourceStatus.Completed = false
		return
	}
	for _, destStatus := range sourceStatus.DestinationsStatus {
		if !destStatus.Completed {
			sourceStatus.Completed = false
			return
		}
	}
	sourceStatus.Completed = true
}

type DestinationStatus struct {
	ID        string `json:"id"`
	Completed bool   `json:"completed"`
	Stats     Stats  `json:"stats"`
}

type FailedRecords []FailedRecord

type FailedRecord struct {
	DestinationID string          `json:"destination_id"`
	RecordID      json.RawMessage `json:"record_id"`
	JobRunID      string          `json:"job_run_id"`
	TaskRunID     string          `json:"task_run_id"`
	SourceID      string          `json:"source_id"`
	CreatedAt     time.Time       `json:"createdAt"`
}

var StatusNotFoundError = errors.New("Status not found")

// StatsIncrementer increments stats
type StatsIncrementer interface {
	// IncrementStats increments the existing statistic counters
	// for a specific job measurement.
	IncrementStats(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, stats Stats) error
}

type JobServiceConfig struct {
	LocalHostname          string
	LocalConn              string
	MaxPoolSize            int
	SharedConn             string
	SubscriptionTargetConn string
}

// JobService manages information about jobs created by rudder-sources
type JobService interface {
	StatsIncrementer

	// Delete deletes all relevant information for a given jobRunId
	Delete(ctx context.Context, jobRunId string) error

	// GetStatus gets the current status of a job
	GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error)

	// TODO: future extension
	AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) error

	// TODO: future extension
	GetFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, filter JobFilter) (FailedRecords, error)

	// CleanupLoop starts the cleanup loop in the background which will stop upon context termination or in case of an error
	CleanupLoop(ctx context.Context) error
}

func NewJobService(config JobServiceConfig) (JobService, error) {
	var (
		localDB, sharedDB *sql.DB
		err               error
	)

	localDB, err = sql.Open("postgres", config.LocalConn)
	if err != nil {
		return nil, fmt.Errorf("failed to create local postgresql connection pool: %w", err)
	}
	localDB.SetMaxOpenConns(config.MaxPoolSize)

	if config.SharedConn != "" {
		sharedDB, err = sql.Open("postgres", config.SharedConn)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared postgresql connection pool: %w", err)
		}
		sharedDB.SetMaxOpenConns(config.MaxPoolSize)
	}
	handler := &sourcesHandler{
		config:   config,
		localDB:  localDB,
		sharedDB: sharedDB,
	}
	return handler, handler.init()
}

func NewNoOpService() JobService {
	return &noopService{}
}

type noopService struct{}

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

func (*noopService) CleanupLoop(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
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

func (k JobTargetKey) String() string {
	return fmt.Sprintf("%s:%s:%s", k.TaskRunID, k.SourceID, k.DestinationID)
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

type JobFailedRecords struct {
	ID    string              `json:"id"`
	Tasks []TaskFailedRecords `json:"tasks"`
}

type TaskFailedRecords struct {
	ID      string                `json:"id"`
	Sources []SourceFailedRecords `json:"sources"`
}

type SourceFailedRecords struct {
	ID           string                     `json:"id"`
	Records      FailedRecords              `json:"records"`
	Destinations []DestinationFailedRecords `json:"destinations"`
}

type DestinationFailedRecords struct {
	ID      string        `json:"id"`
	Records FailedRecords `json:"records"`
}
type FailedRecords []json.RawMessage

// ErrStatusNotFound sentinel error indicating that status cannot be found
var ErrStatusNotFound = errors.New("Status not found")

// ErrSourceNotCompleted sentinel error indicating that a source is not completed
var ErrSourceNotCompleted = errors.New("Source not completed")

// StatsIncrementer increments stats
type StatsIncrementer interface {
	// IncrementStats increments the existing statistic counters
	// for a specific job measurement.
	IncrementStats(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, stats Stats) error
}

type JobServiceConfig struct {
	LocalHostname               string
	LocalConn                   string
	MaxPoolSize                 int
	SharedConn                  string
	SubscriptionTargetConn      string
	SkipFailedRecordsCollection bool
	Log                         logger.Logger
}

// JobService manages information about jobs created by rudder-sources
type JobService interface {
	StatsIncrementer

	// Delete deletes all relevant information for a given jobRunId
	Delete(ctx context.Context, jobRunId string, filter JobFilter) error

	// GetStatus gets the current status of a job
	GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error)

	// AddFailedRecords adds failed records to the database as part of a transaction
	AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []json.RawMessage) error

	// GetFailedRecords gets the failed records for a jobRunID, with filters on taskRunId and sourceId
	GetFailedRecords(ctx context.Context, jobRunId string, filter JobFilter) (JobFailedRecords, error)

	// CleanupLoop starts the cleanup loop in the background which will stop upon context termination or in case of an error
	CleanupLoop(ctx context.Context) error

	// Monitor monitors the logical replication slot and lag when a shared database is configured
	Monitor(ctx context.Context, lagGauge, replicationSlotGauge Gauger)
}

type Gauger interface {
	Gauge(interface{})
}

func NewJobService(config JobServiceConfig) (JobService, error) {
	if config.Log == nil {
		config.Log = logger.NewLogger().Child("rsources")
	}
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
		log:      config.Log,
		config:   config,
		localDB:  localDB,
		sharedDB: sharedDB,
	}
	err = handler.init()
	return handler, err
}

func NewNoOpService() JobService {
	return &noopService{}
}

type noopService struct{}

func (*noopService) Delete(_ context.Context, _ string, _ JobFilter) error {
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

func (*noopService) GetFailedRecords(_ context.Context, _ string, _ JobFilter) (JobFailedRecords, error) {
	return JobFailedRecords{}, nil
}

func (*noopService) CleanupLoop(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (*noopService) Monitor(_ context.Context, _, _ Gauger) {}

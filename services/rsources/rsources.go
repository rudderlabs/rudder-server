package rsources

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
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

func (r *Stats) completed() bool {
	return r.In == r.Out+r.Failed
}

func (r *Stats) corrupted() bool {
	return r.In < r.Out+r.Failed
}

func (r *Stats) fixCorrupted() {
	if r.corrupted() {
		r.In = r.Out + r.Failed
	}
}

type JobStatus struct {
	ID          string       `json:"id"`
	TasksStatus []TaskStatus `json:"tasks"`
}

func (js *JobStatus) FixCorruptedStats(log logger.Logger) {
	isCorrupted := func() bool {
		for ti := range js.TasksStatus {
			for si := range js.TasksStatus[ti].SourcesStatus {
				if js.TasksStatus[ti].SourcesStatus[si].Stats.corrupted() {
					return true
				}
				for di := range js.TasksStatus[ti].SourcesStatus[si].DestinationsStatus {
					if js.TasksStatus[ti].SourcesStatus[si].DestinationsStatus[di].Stats.corrupted() {
						return true
					}
				}
			}
		}
		return false
	}
	fixCorrupted := func() {
		for ti := range js.TasksStatus {
			for si := range js.TasksStatus[ti].SourcesStatus {
				js.TasksStatus[ti].SourcesStatus[si].Stats.fixCorrupted()
				js.TasksStatus[ti].SourcesStatus[si].Completed = js.TasksStatus[ti].SourcesStatus[si].Stats.completed()
				for di := range js.TasksStatus[ti].SourcesStatus[si].DestinationsStatus {
					js.TasksStatus[ti].SourcesStatus[si].DestinationsStatus[di].Stats.fixCorrupted()
					js.TasksStatus[ti].SourcesStatus[si].DestinationsStatus[di].Completed = js.TasksStatus[ti].SourcesStatus[si].DestinationsStatus[di].Stats.completed()
				}
			}
		}
	}
	if isCorrupted() {
		corruptedJson, _ := json.Marshal(js)
		log.Warnw("Corrupted job status stats detected, fixing", "job_status", string(corruptedJson))
		fixCorrupted()
	}
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

type PagingInfo struct {
	Size          int    `json:"size"`
	NextPageToken string `json:"next"`
}

func NextPageTokenFromString(v string) (NextPageToken, error) {
	var npt NextPageToken
	if v == "" {
		return npt, nil
	}
	s, err := base64.URLEncoding.DecodeString(v)
	if err != nil {
		return npt, err
	}
	err = json.Unmarshal(s, &npt)
	return npt, err
}

type NextPageToken struct {
	ID       string `json:"id"`
	RecordID string `json:"record_id"`
}

func (npt *NextPageToken) String() string {
	s, _ := json.Marshal(npt)
	return base64.URLEncoding.EncodeToString(s)
}

type (
	JobFailedRecordsV2      JobFailedRecords[FailedRecord]
	JobFailedRecordsV1      JobFailedRecords[json.RawMessage]
	JobFailedRecords[R any] struct {
		ID     string                 `json:"id"`
		Tasks  []TaskFailedRecords[R] `json:"tasks"`
		Paging *PagingInfo            `json:"paging,omitempty"`
	}
)

type TaskFailedRecords[R any] struct {
	ID      string                   `json:"id"`
	Sources []SourceFailedRecords[R] `json:"sources"`
}

type SourceFailedRecords[R any] struct {
	ID           string                        `json:"id"`
	Records      []R                           `json:"records"`
	Destinations []DestinationFailedRecords[R] `json:"destinations"`
}

type DestinationFailedRecords[R any] struct {
	ID      string `json:"id"`
	Records []R    `json:"records"`
}
type FailedRecord struct {
	Record json.RawMessage `json:"record"`
	Code   int             `json:"code"`
}

// ErrStatusNotFound sentinel error indicating that status cannot be found
var ErrStatusNotFound = errors.New("Status not found")

// ErrSourceNotCompleted sentinel error indicating that a source is not completed
var ErrSourceNotCompleted = errors.New("Source not completed")

// ErrFailedRecordsNotFound sentinel error indicating that failed records cannot be found
var ErrFailedRecordsNotFound = errors.New("Failed records not found")

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
	MinPoolSize                 int
	SharedConn                  string
	SubscriptionTargetConn      string
	SkipFailedRecordsCollection bool
	Log                         logger.Logger
	ShouldSetupSharedDB         bool
}

// JobService manages information about jobs created by rudder-sources
type JobService interface {
	StatsIncrementer

	// Delete deletes all relevant information for a given jobRunId
	Delete(ctx context.Context, jobRunId string, filter JobFilter) error

	// DeleteJobStatus deletes the status for a given jobRunId
	DeleteJobStatus(ctx context.Context, jobRunId string, filter JobFilter) error

	// DeleteFailedRecords deletes all failed records for a given jobRunId
	DeleteFailedRecords(ctx context.Context, jobRunId string, filter JobFilter) error

	// GetStatus gets the current status of a job
	GetStatus(ctx context.Context, jobRunId string, filter JobFilter) (JobStatus, error)

	// AddFailedRecords adds failed records to the database as part of a transaction
	AddFailedRecords(ctx context.Context, tx *sql.Tx, jobRunId string, key JobTargetKey, records []FailedRecord) error

	// GetFailedRecords gets the failed records for a jobRunID, with filters on taskRunId and sourceId
	GetFailedRecords(ctx context.Context, jobRunId string, filter JobFilter, paging PagingInfo) (JobFailedRecordsV2, error)

	// GetFailedRecordsV1 gets the failed records for a jobRunID, with filters on taskRunId and sourceId
	GetFailedRecordsV1(ctx context.Context, jobRunId string, filter JobFilter, paging PagingInfo) (JobFailedRecordsV1, error)

	// CleanupLoop starts the cleanup loop in the background which will stop upon context termination or in case of an error
	CleanupLoop(ctx context.Context) error

	// Monitor monitors the logical replication slot and lag when a shared database is configured
	Monitor(ctx context.Context, lagGauge, replicationSlotGauge Gauger)
}

type Gauger interface {
	Gauge(interface{})
}

func NewJobService(config JobServiceConfig, stats stats.Stats) (JobService, error) {
	if config.Log == nil {
		config.Log = logger.NewLogger().Child("rsources")
	}
	if config.MaxPoolSize <= 2 {
		config.MaxPoolSize = 2 // minimum 2 connections in the pool for proper startup
	}
	if config.MinPoolSize <= 0 {
		config.MinPoolSize = 1
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
	err = stats.RegisterCollector(collectors.NewDatabaseSQLStats("rsources-local", localDB))
	if err != nil {
		return nil, fmt.Errorf("register local database stats collector: %w", err)
	}
	if config.SharedConn != "" {
		sharedDB, err = sql.Open("postgres", config.SharedConn)
		if err != nil {
			return nil, fmt.Errorf("failed to create shared postgresql connection pool: %w", err)
		}
		sharedDB.SetMaxOpenConns(config.MaxPoolSize)
		sharedDB.SetMaxIdleConns(config.MinPoolSize)
		err = stats.RegisterCollector(collectors.NewDatabaseSQLStats("rsources-shared", sharedDB))
		if err != nil {
			return nil, fmt.Errorf("register shared database stats collector: %w", err)
		}
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

func (*noopService) DeleteJobStatus(_ context.Context, _ string, _ JobFilter) error {
	return nil
}

func (*noopService) DeleteFailedRecords(_ context.Context, _ string, _ JobFilter) error {
	return nil
}

func (*noopService) GetStatus(_ context.Context, _ string, _ JobFilter) (JobStatus, error) {
	return JobStatus{}, nil
}

func (*noopService) IncrementStats(_ context.Context, _ *sql.Tx, _ string, _ JobTargetKey, _ Stats) error {
	return nil
}

func (*noopService) AddFailedRecords(_ context.Context, _ *sql.Tx, _ string, _ JobTargetKey, _ []FailedRecord) error {
	return nil
}

func (*noopService) GetFailedRecords(_ context.Context, _ string, _ JobFilter, _ PagingInfo) (JobFailedRecordsV2, error) {
	return JobFailedRecordsV2{}, nil
}

func (*noopService) GetFailedRecordsV1(_ context.Context, _ string, _ JobFilter, _ PagingInfo) (JobFailedRecordsV1, error) {
	return JobFailedRecordsV1{}, nil
}

func (*noopService) CleanupLoop(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (*noopService) Monitor(_ context.Context, _, _ Gauger) {}

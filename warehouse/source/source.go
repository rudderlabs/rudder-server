package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type Manager struct {
	logger           logger.Logger
	sourceRepo       sourceRepo
	tableUploadsRepo tableUploadsRepo
	publisher        publisher

	config struct {
		maxBatchSizeToProcess int64
		maxAttemptsPerJob     int
	}

	trigger struct {
		processingTimeout       func() <-chan time.Time
		processingSleepInterval func() <-chan time.Time
	}
}

func New(conf *config.Config, log logger.Logger, db *sqlmw.DB, publisher publisher) *Manager {
	m := &Manager{
		logger:           log.Child("source"),
		tableUploadsRepo: repo.NewTableUploads(db),
		sourceRepo:       repo.NewSource(db),
		publisher:        publisher,
	}

	m.config.maxBatchSizeToProcess = conf.GetInt64("Warehouse.jobs.maxBatchSizeToProcess", 10)
	m.config.maxAttemptsPerJob = conf.GetInt("Warehouse.jobs.maxAttemptsPerJob", 3)

	m.trigger.processingTimeout = func() <-chan time.Time {
		return time.After(conf.GetDuration("Warehouse.jobs.processingTimeout", 300, time.Second))
	}
	m.trigger.processingSleepInterval = func() <-chan time.Time {
		return time.After(conf.GetDuration("Warehouse.jobs.processingSleepInterval", 10, time.Second))
	}
	return m
}

func (m *Manager) InsertJobs(ctx context.Context, payload insertJobRequest) ([]int64, error) {
	jobType, err := model.FromSourceJobType(payload.JobType)
	if err != nil {
		return nil, fmt.Errorf("invalid job type %s", payload.JobType)
	}

	tableUploads, err := m.tableUploadsRepo.GetByJobRunTaskRun(
		ctx,
		payload.SourceID,
		payload.DestinationID,
		payload.JobRunID,
		payload.TaskRunID,
	)
	if err != nil {
		return nil, fmt.Errorf("getting table uploads: %w", err)
	}

	tableNames := lo.Map(tableUploads, func(item model.TableUpload, index int) string {
		return item.TableName
	})
	tableNames = lo.Filter(lo.Uniq(tableNames), func(tableName string, i int) bool {
		switch strings.ToLower(tableName) {
		case whutils.DiscardsTable, whutils.IdentityMappingsTable, whutils.IdentityMergeRulesTable:
			return false
		default:
			return true
		}
	})

	type metadata struct {
		JobRunID  string    `json:"job_run_id"`
		TaskRunID string    `json:"task_run_id"`
		JobType   string    `json:"jobtype"`
		StartTime time.Time `json:"start_time"`
	}
	metadataJson, err := json.Marshal(metadata{
		JobRunID:  payload.JobRunID,
		TaskRunID: payload.TaskRunID,
		StartTime: payload.StartTime.Time,
		JobType:   string(notifier.JobTypeAsync),
	})
	if err != nil {
		return nil, fmt.Errorf("marshalling metadata: %w", err)
	}
	jobIds, err := m.sourceRepo.Insert(ctx, lo.Map(tableNames, func(tableName string, _ int) model.SourceJob {
		return model.SourceJob{
			SourceID:      payload.SourceID,
			DestinationID: payload.DestinationID,
			WorkspaceID:   payload.WorkspaceID,
			TableName:     tableName,
			JobType:       jobType,
			Metadata:      metadataJson,
		}
	}))
	if err != nil {
		return nil, fmt.Errorf("inserting source jobs: %w", err)
	}
	return jobIds, nil
}

func (m *Manager) Run(ctx context.Context) error {
	if err := m.sourceRepo.Reset(ctx); err != nil {
		return fmt.Errorf("resetting source jobs with error %w", err)
	}

	if err := m.process(ctx); err != nil {
		var pqErr *pq.Error

		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.As(err, &pqErr) && pqErr.Code == "57014":
			return nil
		default:
			return fmt.Errorf("processing source jobs with error %w", err)
		}
	}
	return nil
}

func (m *Manager) process(ctx context.Context) error {
	m.logger.Infow("starting source jobs processing")

	for {
		pendingJobs, err := m.sourceRepo.GetToProcess(ctx, m.config.maxBatchSizeToProcess)
		if err != nil {
			return fmt.Errorf("getting pending source jobs with error %w", err)
		}

		if len(pendingJobs) > 0 {
			if err = m.processPendingJobs(ctx, pendingJobs); err != nil {
				return fmt.Errorf("process pending source jobs with error %w", err)
			}
		}

		select {
		case <-ctx.Done():
			m.logger.Infow("source jobs processing stopped due to context cancelled")
			return nil
		case <-m.trigger.processingSleepInterval():
		}
	}
}

// processPendingJobs
// 1. Prepare and publish claims to notifier
// 2. Mark source jobs as executing
// 3. Mark source jobs as failed if notifier returned timeout
// 4. Mark source jobs as failed if notifier returned error else mark as succeeded
func (m *Manager) processPendingJobs(ctx context.Context, pendingJobs []model.SourceJob) error {
	claims := make([]json.RawMessage, 0, len(pendingJobs))
	for _, job := range pendingJobs {
		message, err := json.Marshal(NotifierRequest{
			ID:            job.ID,
			SourceID:      job.SourceID,
			DestinationID: job.DestinationID,
			WorkspaceID:   job.WorkspaceID,
			TableName:     job.TableName,
			JobType:       job.JobType.String(),
			MetaData:      job.Metadata,
		})
		if err != nil {
			return fmt.Errorf("marshalling source job %d: %w", job.ID, err)
		}
		claims = append(claims, message)
	}

	ch, err := m.publisher.Publish(ctx, &notifier.PublishRequest{
		Payloads: claims,
		JobType:  notifier.JobTypeAsync,
		Priority: 100,
	})
	if err != nil {
		return fmt.Errorf("publishing source jobs: %w", err)
	}

	pendingJobsMap := lo.SliceToMap(pendingJobs, func(item model.SourceJob) (int64, *model.SourceJob) {
		return item.ID, &item
	})
	pendingJobIDs := lo.Map(pendingJobs, func(item model.SourceJob, index int) int64 {
		return item.ID
	})

	if err = m.sourceRepo.MarkExecuting(ctx, pendingJobIDs); err != nil {
		return fmt.Errorf("marking status executing: %w", err)
	}

	select {
	case <-ctx.Done():
		m.logger.Infow("pending jobs process stopped due to context cancelled", "ids", pendingJobIDs)
		return nil
	case responses, ok := <-ch:
		if !ok {
			if err := m.markFailed(ctx, pendingJobIDs, ErrReceivingChannelClosed); err != nil {
				return fmt.Errorf("marking status failed for receiving channel closed: %w", err)
			}
			return ErrReceivingChannelClosed
		}
		if responses.Err != nil {
			if err := m.markFailed(ctx, pendingJobIDs, responses.Err); err != nil {
				return fmt.Errorf("marking status failed for publishing source jobs: %w", err)
			}
			return fmt.Errorf("publishing source jobs: %w", responses.Err)
		}

		for _, job := range responses.Jobs {
			var response NotifierResponse
			var jobStatus model.SourceJobStatus

			if err = json.Unmarshal(job.Payload, &response); err != nil {
				return fmt.Errorf("unmarshalling notifier response for source job %d: %w", job.ID, err)
			}
			if jobStatus, err = model.FromSourceJobStatus(string(job.Status)); err != nil {
				return fmt.Errorf("invalid job status %s for source job %d: %w", job.Status, job.ID, err)
			}
			if pendingJob, ok := pendingJobsMap[response.ID]; ok {
				pendingJob.Status = jobStatus
				pendingJob.Error = job.Error
			}
		}

		for _, job := range pendingJobsMap {
			if job.Error != nil {
				err = m.sourceRepo.OnUpdateFailure(
					ctx,
					job.ID,
					job.Error,
					m.config.maxAttemptsPerJob,
				)
				if err != nil {
					return fmt.Errorf("on update failure for source job %d: %w", job.ID, err)
				}
				continue
			}

			if err = m.sourceRepo.OnUpdateSuccess(ctx, job.ID); err != nil {
				return fmt.Errorf("marking status success for source job %d: %w", job.ID, err)
			}
		}
	case <-m.trigger.processingTimeout():
		if err = m.markFailed(ctx, pendingJobIDs, ErrProcessingTimedOut); err != nil {
			return fmt.Errorf("marking status failed for processing timed out: %w", err)
		}
		return ErrProcessingTimedOut
	}
	return nil
}

func (m *Manager) markFailed(ctx context.Context, ids []int64, failError error) error {
	for _, id := range ids {
		err := m.sourceRepo.OnUpdateFailure(
			ctx,
			id,
			failError,
			m.config.maxAttemptsPerJob,
		)
		if err != nil {
			return fmt.Errorf("updating failure for source job %d: %w", id, err)
		}
	}
	return nil
}

type Uploader struct{}

func (*Uploader) IsWarehouseSchemaEmpty() bool                                      { return true }
func (*Uploader) UpdateLocalSchema(context.Context, model.Schema) error             { return nil }
func (*Uploader) GetTableSchemaInUpload(string) model.TableSchema                   { return model.TableSchema{} }
func (*Uploader) ShouldOnDedupUseNewRecord() bool                                   { return false }
func (*Uploader) UseRudderStorage() bool                                            { return false }
func (*Uploader) CanAppend() bool                                                   { return false }
func (*Uploader) GetLoadFileType() string                                           { return "" }
func (*Uploader) GetLocalSchema(context.Context) (model.Schema, error)              { return model.Schema{}, nil }
func (*Uploader) GetTableSchemaInWarehouse(string) model.TableSchema                { return model.TableSchema{} }
func (*Uploader) GetSampleLoadFileLocation(context.Context, string) (string, error) { return "", nil }
func (*Uploader) GetLoadFilesMetadata(context.Context, whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
	return []whutils.LoadFile{}, nil
}

func (*Uploader) GetSingleLoadFile(context.Context, string) (whutils.LoadFile, error) {
	return whutils.LoadFile{}, nil
}

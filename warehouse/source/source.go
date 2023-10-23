package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type notifierResponse struct {
	Id int64 `json:"id"`
}

type publisher interface {
	Publish(context.Context, *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error)
}

type Manager struct {
	logger           logger.Logger
	sourceRepo       *repo.Source
	tableUploadsRepo *repo.TableUploads
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
		return time.After(conf.GetDuration("Warehouse.jobs.processingSleepInterval", 5, time.Second))
	}
	return m
}

func (m *Manager) Run(ctx context.Context) error {
	if err := m.sourceRepo.Reset(ctx); err != nil {
		return fmt.Errorf("resetting source jobs with error %s", err.Error())
	}

	if err := m.process(ctx); err != nil && errors.Is(err, context.Canceled) {
		return fmt.Errorf("processing source jobs with error %s", err.Error())
	}
	return nil
}

func (m *Manager) process(ctx context.Context) error {
	for {
		pendingJobs, err := m.sourceRepo.GetToProcess(ctx, m.config.maxBatchSizeToProcess)
		if err != nil {
			return fmt.Errorf("getting pending source jobs with error %s", err.Error())
		}
		if len(pendingJobs) == 0 {
			continue
		}

		if err = m.processPendingJobs(ctx, pendingJobs); err != nil {
			return fmt.Errorf("process pending source jobs with error %s", err.Error())
		}

		select {
		case <-ctx.Done():
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
		message, err := json.Marshal(job)
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

	pendingJobsMap := lo.SliceToMap(pendingJobs, func(item model.SourceJob) (int64, model.SourceJob) {
		return item.ID, item
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
			return fmt.Errorf("receiving notifier channel closed")
		}
		if responses.Err != nil {
			return m.markFailed(ctx, pendingJobIDs, responses.Err)
		}

		for _, job := range responses.Jobs {
			var response notifierResponse

			if err = json.Unmarshal(job.Payload, &response); err != nil {
				return fmt.Errorf("unmarshalling notifier response for source job %d: %w", job.ID, err)
			}
			if pj, ok := pendingJobsMap[response.Id]; ok {
				pj.Status = string(job.Status)
				pj.Error = job.Error
			}
		}

		for _, job := range pendingJobsMap {
			if job.Error != nil {
				err = m.sourceRepo.OnUpdateFailure(
					ctx,
					job.ID,
					responses.Err,
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
		if err = m.markFailed(ctx, pendingJobIDs, context.Canceled); err != nil {
			return fmt.Errorf("marking status failed: %w", err)
		}
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
func (*Uploader) GetLoadFileGenStartTIme() time.Time                                { return time.Time{} }
func (*Uploader) GetLoadFileType() string                                           { return "" }
func (*Uploader) GetFirstLastEvent() (time.Time, time.Time)                         { return time.Now(), time.Now() }
func (*Uploader) GetLocalSchema(context.Context) (model.Schema, error)              { return model.Schema{}, nil }
func (*Uploader) GetTableSchemaInWarehouse(string) model.TableSchema                { return model.TableSchema{} }
func (*Uploader) GetSampleLoadFileLocation(context.Context, string) (string, error) { return "", nil }
func (*Uploader) GetLoadFilesMetadata(context.Context, whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
	return []whutils.LoadFile{}, nil
}

func (*Uploader) GetSingleLoadFile(context.Context, string) (whutils.LoadFile, error) {
	return whutils.LoadFile{}, nil
}

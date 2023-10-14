package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type notifierResponse struct {
	Id int64 `json:"id"`
}

type Manager struct {
	logger           logger.Logger
	sourceRepo       *repo.Source
	tableUploadsRepo *repo.TableUploads
	notifier         *notifier.Notifier

	config struct {
		maxBatchSizeToProcess   int64
		processingSleepInterval time.Duration
		maxAttemptsPerJob       int
		processingTimeout       time.Duration
	}
}

func New(
	conf *config.Config,
	log logger.Logger,
	db *sqlmw.DB,
	notifier *notifier.Notifier,
) *Manager {
	m := &Manager{
		logger:           log.Child("source-manager"),
		tableUploadsRepo: repo.NewTableUploads(db),
		sourceRepo:       repo.NewSource(db),
		notifier:         notifier,
	}

	m.config.maxBatchSizeToProcess = conf.GetInt64("Warehouse.jobs.maxBatchSizeToProcess", 10)
	m.config.maxAttemptsPerJob = conf.GetInt("Warehouse.jobs.maxAttemptsPerJob", 3)
	m.config.processingSleepInterval = conf.GetDuration("Warehouse.jobs.processingSleepInterval", 10, time.Second)
	m.config.processingTimeout = conf.GetDuration("Warehouse.jobs.processingTimeout", 300, time.Second)

	return m
}

func (a *Manager) Run(ctx context.Context) error {
	if err := a.sourceRepo.Reset(ctx); err != nil {
		return fmt.Errorf("unable to reset source table with error %s", err.Error())
	}

	return a.startProcessing(ctx)
}

/*
startProcessing is the main runner that
1) Periodically queries the db for any pending source jobs
2) Groups them together
3) Publishes them to the notifier
4) Spawns a subroutine that periodically checks for responses from Notifier/slave worker post trackBatch
*/
func (a *Manager) startProcessing(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.config.processingSleepInterval):
		}

		pendingJobs, err := a.sourceRepo.GetToProcess(ctx, a.config.maxBatchSizeToProcess)
		if err != nil {
			return fmt.Errorf("unable to get pending source jobs with error %s", err.Error())
		}
		if len(pendingJobs) == 0 {
			continue
		}

		notifierClaims := make([]json.RawMessage, 0, len(pendingJobs))
		for _, job := range pendingJobs {
			message, err := json.Marshal(job)
			if err != nil {
				return fmt.Errorf("unable to marshal source job payload with error %s", err.Error())
			}
			notifierClaims = append(notifierClaims, message)
		}

		ch, err := a.notifier.Publish(ctx, &notifier.PublishRequest{
			Payloads: notifierClaims,
			JobType:  notifier.JobTypeAsync,
			Priority: 100,
		})
		if err != nil {
			return fmt.Errorf("unable to publish source jobs to notifier with error %s", err.Error())
		}

		pendingJobsMap := make(map[int64]model.SourceJob)
		for _, job := range pendingJobs {
			pendingJobsMap[job.ID] = job
		}

		select {
		case <-ctx.Done():
			a.logger.Info("context cancelled, exiting")
			return nil
		case responses, ok := <-ch:
			if !ok {
				for _, job := range pendingJobsMap {
					err := a.sourceRepo.OnUpdateFailure(
						ctx,
						job.ID,
						errors.New("receiving channel closed"),
						a.config.maxAttemptsPerJob,
					)
					if err != nil {
						return fmt.Errorf("unable to update source job with error %s", err.Error())
					}
				}
				continue
			}
			if responses.Err != nil {
				for _, job := range pendingJobsMap {
					err := a.sourceRepo.OnUpdateFailure(
						ctx,
						job.ID,
						responses.Err,
						a.config.maxAttemptsPerJob,
					)
					if err != nil {
						return fmt.Errorf("unable to update source job with error %s", err.Error())
					}
				}
				continue
			}

			for _, job := range responses.Jobs {
				var response notifierResponse
				err := json.Unmarshal(job.Payload, &response)
				if err != nil {
					return fmt.Errorf("unable to unmarshal notifier response with error %s", err.Error())
				}

				if pj, ok := pendingJobsMap[response.Id]; ok {
					pj.Status = string(job.Status)
					pj.Error = job.Error
				}
			}

			for _, job := range pendingJobsMap {
				if job.Error != nil {
					err := a.sourceRepo.OnUpdateFailure(
						ctx,
						job.ID,
						responses.Err,
						a.config.maxAttemptsPerJob,
					)
					if err != nil {
						return fmt.Errorf("unable to update source job with error %s", err.Error())
					}
					continue
				}
				err := a.sourceRepo.OnUpdateSuccess(
					ctx,
					job.ID,
				)
				if err != nil {
					return fmt.Errorf("unable to update source job with error %s", err.Error())
				}
			}
		case <-time.After(a.config.processingTimeout):
			for _, job := range pendingJobsMap {
				err := a.sourceRepo.OnUpdateFailure(
					ctx,
					job.ID,
					errors.New("job timed out"),
					a.config.maxAttemptsPerJob,
				)
				if err != nil {
					return fmt.Errorf("unable to update source job with error %s", err.Error())
				}
			}
		}
	}
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
func (*Uploader) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
	return []warehouseutils.LoadFile{}
}

func (*Uploader) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

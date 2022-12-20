package service

// TODO: appropriate error handling via model.Errors
// TODO: appropriate status var update and handling via model.status
import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/stats"
)

//go:generate mockgen -source=service.go -destination=mock_service_test.go -package=service github.com/rudderlabs/rudder-server/regulation-worker/internal/service
type APIClient interface {
	Get(ctx context.Context) (model.Job, error)
	UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error
}

type destDetail interface {
	GetWorkspaceId(ctx context.Context) (string, error)
	GetDestDetails(ctx context.Context, destID string) (model.Destination, error)
}
type deleter interface {
	Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus
}

type JobSvc struct {
	API        APIClient
	Deleter    deleter
	DestDetail destDetail
}

func (js *JobSvc) Pickup(ctx context.Context) (*model.Job, error) {
	pkgLogger.Debugf("making API request to get job")
	job, err := js.API.Get(ctx)
	if err != nil {
		pkgLogger.Warnf("error while getting job: %v", err)
		return nil, err
	}

	// once job is successfully received, calling updatestatus API to update the status of job to running.
	status := model.JobStatusRunning
	err = js.updateStatus(ctx, status, job.ID)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (js *JobSvc) Process(ctx context.Context, job *model.Job) error {
	start := time.Now()
	var status model.JobStatus
	destDetail, err := js.DestDetail.GetDestDetails(ctx, job.DestinationID)
	if err != nil {
		pkgLogger.Errorf("error while getting destination details: %v", err)
		if err == model.ErrInvalidDestination {
			return js.updateStatus(ctx, model.JobStatusAborted, job.ID)
		}
		return js.updateStatus(ctx, model.JobStatusFailed, job.ID)
	}
	defer func() {
		stats.Default.NewTaggedStat("process_time", stats.TimerType, stats.Tags{
			"workspaceId":     job.WorkspaceID,
			"destinationId":   destDetail.DestinationID,
			"destinationType": destDetail.Name,
			"status":          string(status),
		}).Since(start)
	}()
	deletionStart := time.Now()
	status = js.Deleter.Delete(ctx, *job, destDetail)
	stats.Default.NewTaggedStat("delete_time", stats.TimerType, stats.Tags{
		"workspaceId":     job.WorkspaceID,
		"destinationid":   destDetail.DestinationID,
		"destinationType": destDetail.Name,
		"status":          string(status),
	}).Since(deletionStart)
	return js.updateStatus(ctx, status, job.ID)
}

func (js *JobSvc) updateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	pkgLogger.Debugf("updating job: %d status to: %v", jobID, status)

	maxWait := 10 * time.Minute
	var err error
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait

	if err = backoff.Retry(func() error {
		err := js.API.UpdateStatus(ctx, status, jobID)
		pkgLogger.Debugf("trying to update status...")
		return err
	}, boCtx); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			pkgLogger.Debugf("reached retry limit...")
			return err
		}
	}
	return nil
}

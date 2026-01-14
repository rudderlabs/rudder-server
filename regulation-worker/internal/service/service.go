package service

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
)

//go:generate mockgen -source=service.go -destination=mock_service.go -package=service github.com/rudderlabs/rudder-server/regulation-worker/internal/service
type APIClient interface {
	Get(ctx context.Context) (model.Job, error)
	UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error
}

type destDetail interface {
	GetDestDetails(destID string) (model.Destination, error)
}
type deleter interface {
	Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus
}

type JobSvc struct {
	API               APIClient
	Deleter           deleter
	DestDetail        destDetail
	MaxFailedAttempts int
}

// JobSvc called by looper
// calls api-client.getJob(workspaceID)
// calls api-client to get new job with workspaceID, which returns jobID.
func (js *JobSvc) JobSvc(ctx context.Context) error {
	loopStart := time.Now()
	// API request to get new job
	pkgLogger.Debugn("making API request to get job")
	job, err := js.API.Get(ctx)
	if err != nil {
		pkgLogger.Warnn("error while getting job", obskit.Error(err))
		return err
	}

	// once job is successfully received, calling update-status API to update the status of job to running.
	jobStatus := model.JobStatus{Status: model.JobStatusRunning}
	err = js.updateStatus(ctx, jobStatus, job.ID)
	if err != nil {
		return err
	}
	// executing deletion
	destDetail, err := js.DestDetail.GetDestDetails(job.DestinationID)
	if err != nil {
		pkgLogger.Errorn("error while getting destination details", obskit.Error(err))
		if errors.Is(err, model.ErrInvalidDestination) {
			return js.updateStatus(ctx, model.JobStatus{Status: model.JobStatusAborted, Error: model.ErrInvalidDestination}, job.ID)
		}
		return js.updateStatus(ctx, model.JobStatus{Status: model.JobStatusFailed, Error: err}, job.ID)
	}

	deletionStart := time.Now()

	jobStatus = js.Deleter.Delete(ctx, job, destDetail)
	if jobStatus.Status == model.JobStatusFailed && job.FailedAttempts >= js.MaxFailedAttempts {
		jobStatus.Status = model.JobStatusAborted
	}

	stats.Default.NewTaggedStat("regulation_worker_attempted_user_deletions_count", stats.CountType, stats.Tags{"workspaceId": job.WorkspaceID, "destinationid": destDetail.DestinationID, "destinationType": destDetail.Name, "status": string(jobStatus.Status)}).Count(len(job.Users))

	stats.Default.NewTaggedStat("regulation_worker_deletion_time", stats.TimerType, stats.Tags{"workspaceId": job.WorkspaceID, "destinationid": destDetail.DestinationID, "destinationType": destDetail.Name, "status": string(jobStatus.Status)}).Since(deletionStart)
	if jobStatus.Status == model.JobStatusComplete {
		stats.Default.NewTaggedStat("regulation_worker_deleted_user_count", stats.CountType, stats.Tags{"workspaceId": job.WorkspaceID, "destinationid": destDetail.DestinationID, "destinationType": destDetail.Name}).Count(len(job.Users))
	}
	stats.Default.NewTaggedStat("regulation_worker_loop_time", stats.TimerType, stats.Tags{"workspaceId": job.WorkspaceID, "destinationid": destDetail.DestinationID, "destinationType": destDetail.Name, "status": string(jobStatus.Status)}).Since(loopStart)

	return js.updateStatus(ctx, jobStatus, job.ID)
}

func (js *JobSvc) updateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	pkgLogger.Debugn("updating job status",
		logger.NewIntField("jobID", int64(jobID)),
		logger.NewStringField("status", string(status.Status)))

	maxWait := 10 * time.Minute
	var err error
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Minute

	if err = backoffvoid.Retry(ctx,
		func() error {
			err := js.API.UpdateStatus(ctx, status, jobID)
			pkgLogger.Debugn("trying to update status...")
			return err
		},
		backoff.WithBackOff(bo),
		backoff.WithMaxElapsedTime(maxWait),
	); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			pkgLogger.Debugn("reached retry limit...")
			return err
		}
	}
	return nil
}

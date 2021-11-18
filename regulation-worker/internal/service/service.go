package service

//TODO: appropriate error handling via model.Errors
//TODO: appropriate status var update and handling via model.status
import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

//go:generate mockgen -source=service.go -destination=mock_service_test.go -package=service github.com/rudderlabs/rudder-server/regulation-worker/internal/service
type APIClient interface {
	Get(ctx context.Context) (model.Job, error)
	UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error
}

type destDetail interface {
	GetDestDetails(destID, workspaceID string) (model.Destination, error)
}
type deleter interface {
	Delete(ctx context.Context, job model.Job, destDetail model.Destination) (model.JobStatus, error)
}

type JobSvc struct {
	API        APIClient
	Deleter    deleter
	DestDetail destDetail
}

//called by looper
//calls api-client.getJob(workspaceID)
//calls api-client to get new job with workspaceID, which returns jobID.
func (js *JobSvc) JobSvc(ctx context.Context) error {
	//API request to get new job
	job, err := js.API.Get(ctx)

	if err != nil {
		return err
	}

	//once job is successfully received, calling updatestatus API to update the status of job to running.
	status := model.JobStatusRunning
	err = js.updateStatus(ctx, status, job.ID)
	if err != nil {
		return err
	}

	//executing deletion
	destDetail, err := js.DestDetail.GetDestDetails(job.DestinationID)
	if err != nil {
		return fmt.Errorf("error while getting destination details: %w", err)
	}

	status, err = js.Deleter.Delete(ctx, job, destDetail)
	if err != nil {
		return err
	}

	err = js.updateStatus(ctx, status, job.ID)
	if err != nil {
		return err
	}

	return nil
}

func (js *JobSvc) updateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	maxWait := time.Minute * 10
	var err error
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait

	if err = backoff.Retry(func() error {
		err := js.API.UpdateStatus(ctx, status, jobID)
		return err
	}, boCtx); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			return err
		}

	}
	return err
}

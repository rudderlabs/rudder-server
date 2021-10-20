package service

//TODO: appropriate error handling via model.Errors
//TODO: appropriate status var update and handling via model.status
import (
	"context"
	"fmt"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type APIClient interface {
	Get(ctx context.Context) error
	UpdateStatus(ctx context.Context, status model.JobStatus) error
}

type deleter interface {
	DeleteJob(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error)
}
type JobSvc struct {
	API     client.JobAPI
	Deleter deleter
}

//called by looper
//calls api-client.getJob(workspaceID)
//calls api-client to get new job with workspaceID, which returns jobID.
//Doubt: context is for a flow. So, should we define new context for each sub-flow.
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
	dest, err := getDestDetails(job.DestinationID, job.WorkspaceID)
	if err != nil {
		return fmt.Errorf("error while getting destination details: %w", err)
	}
	status, err = js.Deleter.DeleteJob(ctx, job, dest)
	if err != nil {
		return err
	}

	err = js.updateStatus(ctx, status, job.ID)
	if err != nil {
		return err
	}
	return nil
}

//make api call to get json and then parse it to get destination related details
//like: dest_type, auth details,
//return destination Type enum{file, api}
func getDestDetails(destID, workspaceID string) (model.Destination, error) {
	return model.Destination{}, nil
}

func (js *JobSvc) updateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	maxWait := time.Minute * 10
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait
	if err := backoff.Retry(func() error {
		err := js.API.UpdateStatus(ctx, status, jobID)
		return err
	}, boCtx); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			return err
		}

	}
	return nil
}

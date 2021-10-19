package service

//TODO: appropriate error handling via model.Errors
//TODO: appropriate status var update and handling via model.status
import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/client"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type APIClient interface {
	Get(ctx context.Context) error
	UpdateStatus(ctx context.Context, status model.JobStatus) error
}

type Deleter interface {
	DeleteJob(ctx context.Context) (string, error)
}
type JobSvc struct {
	API     client.JobAPI
	Deleter delete.Deleter
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
	js.API.JobID = job.ID
	status := model.JobStatusRunning
	err = js.API.UpdateStatus(ctx, status)
	if err != nil {
		fmt.Println(err)
	}

	//executing deletion
	js.Deleter.Job = job
	dest, err := getDestDetails(job.DestinationID, job.WorkspaceID)
	if err != nil {
		return fmt.Errorf("error while getting destination details: %w", err)
	}
	js.Deleter.Destination = dest
	status, err = js.Deleter.DeleteJob(ctx)
	if err != nil {
		return err
	}

	//trying to update the status of job for 10 min, if failed, then panic.
	retryTimeout := time.After(time.Minute * 10)
	for {
		select {
		case <-retryTimeout:
			panic(err)
		default:
			err = js.API.UpdateStatus(ctx, status)
			if err == nil {
				return nil
			}
		}

	}
}

//make api call to get json and then parse it to get destination related details
//like: dest_type, auth details,
//return destination Type enum{file, api}
func getDestDetails(destID, workspaceID string) (model.Destination, error) {
	return model.Destination{}, nil
}

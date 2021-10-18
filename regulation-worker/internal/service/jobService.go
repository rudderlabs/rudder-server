package service

import (
	"context"
	"fmt"

	client "github.com/rudderlabs/rudder-server/regulation-worker/internal/api-client"
	apiDeleter "github.com/rudderlabs/rudder-server/regulation-worker/internal/deleter/api"
	batchDeleter "github.com/rudderlabs/rudder-server/regulation-worker/internal/deleter/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

var status string

type APIClient interface {
	GetJobAPI(ctx context.Context) error
	UpdateJobStatusAPI(ctx context.Context, status string) error
}

type JobSvc interface {
	GetJob() error
	UpdateJobStatus() error
	DeleteJob(ctx context.Context) (string, error)
}

type JobSvcImpl struct {
	Api client.JobAPI
	Job model.Job
}

//called by looper
//calls api-client.getJob(workspaceID)
//calls api-client to get new job with workspaceID, which returns jobID.
func (js *JobSvcImpl) GetJob(ctx context.Context) error {

	job, err := js.Api.GetJobAPI(ctx)
	if err != nil {
		return err
	}
	js.Api.JobID = job.ID
	js.Job = job
	status, err = js.DeleteJob(ctx)
	return err
}

func (js *JobSvcImpl) UpdateJobStatus() error {
	err := js.Api.UpdateJobStatusAPI(context.Background(), status)
	return err
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (js *JobSvcImpl) DeleteJob(ctx context.Context) (string, error) {
	var err error
	dest, err := getDestDetails(js.Job.DestinationID, js.Job.WorkspaceID)
	if err != nil {
		return status, err
	}
	if dest.Type == "api" {

		deleter := apiDeleter.Deleter{Dest: dest}
		return deleter.CallTransformer()

	} else if dest.Type == "batch" {

		deleter := batchDeleter.Deleter{Dest: dest}
		err = deleter.GetData()
		if err != nil {
			status = "failed"
			return status, fmt.Errorf("error while getting deletion data: %w", err)
		}

		err = deleter.DeleteData()
		if err != nil {
			status = "failed"
			return status, fmt.Errorf("error while deleting users from destination data: %w", err)
		}

		err = deleter.UploadData()
		if err != nil {
			status = "failed"
			return status, fmt.Errorf("error while uploading deleted data: %w", err)
		}
		status = "successful"
		return status, nil

	} else {
		return status, fmt.Errorf("unknown destination type")
	}
}

//make api call to get json and then parse it to get destination related details
//like: dest_type, auth details,
//return destination Type enum{file, api}
func getDestDetails(destID, workspaceID string) (model.Destination, error) {
	return model.Destination{}, nil
}

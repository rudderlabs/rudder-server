package service

//TODO: appropriate error handling via model.Errors
//TODO: appropriate status var update and handling via model.status
import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//go:generate mockgen -source=service.go -destination=mock_deleter_test.go -package=service github.com/rudderlabs/rudder-server/regulation-worker/internal/service deleter,APIClient
type APIClient interface {
	Get(ctx context.Context) (model.Job, error)
	UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error
}

type deleter interface {
	DeleteJob(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error)
}
type JobSvc struct {
	API     APIClient
	Deleter deleter
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
	dest, err := GetDestDetails(job.DestinationID, job.WorkspaceID)
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
func GetDestDetails(destID, workspaceID string) (model.Destination, error) {
	workspaceConfig := backendconfig.WorkspaceConfig{}
	config, notErr := workspaceConfig.Get()
	if !notErr {
		return model.Destination{}, fmt.Errorf("error while getting destination details")
	}
	destDetail := model.Destination{}
	for _, source := range config.Sources {
		for _, dest := range source.Destinations {
			if dest.ID == destID {
				destDetail.Config.BucketName = dest.Config["bucketName"]
				destDetail.Config.Prefix = dest.Config["prefix"]
				destDetail.Config.AccessKeyID = dest.Config["accessKeyID"]
				destDetail.Config.AccessKey = dest.Config["accessKey"]
				destDetail.Config.EnableSSE = dest.Config["enableSSE"]
				destDetail.DestinationID = dest.ID
				destDetail.Name = dest.DestinationDefinition.Name
			}
		}
	}
	batchDestinations, _ := misc.LoadDestinations()
	if misc.Contains(batchDestinations, destDetail.Name) {
		destDetail.Type = "batch"
	} else {
		destDetail.Type = "API"
	}
	return destDetail, nil
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

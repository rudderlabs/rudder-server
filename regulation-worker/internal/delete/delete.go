package delete

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Deleter struct {
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *Deleter) DeleteJob(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {

	if dest.Type == "api" {

		deleter := MockAPIDeleter{}
		return deleter.CallTransformer(job, dest)

	} else if dest.Type == "batch" {

		deleter := MockBatchDeleter{}
		data, err := deleter.GetData(job, dest)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while getting deletion data: %w", err)
		}
		cleanedData, err := deleter.DeleteData(job, dest, data)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while deleting users from destination data: %w", err)
		}

		status, err := deleter.UploadData(job, dest, cleanedData)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while uploading deleted data: %w", err)
		}
		return status, nil

	} else {
		return model.JobStatusFailed, fmt.Errorf("deletion feature not available for %s destination type", dest.Type)
	}
}

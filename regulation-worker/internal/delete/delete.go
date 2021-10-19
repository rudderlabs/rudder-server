package delete

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Deleter struct {
	Job         model.Job
	Destination model.Destination
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *Deleter) DeleteJob(ctx context.Context) (model.JobStatus, error) {
	if d.Destination.Type == "api" {

		deleter := MockAPIDeleter{}
		return deleter.CallTransformer()

	} else if d.Destination.Type == "batch" {

		deleter := MockBatchDeleter{}
		data, err := deleter.GetData()
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while getting deletion data: %w", err)
		}
		cleanedData, err := deleter.DeleteData(data)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while deleting users from destination data: %w", err)
		}

		status, err := deleter.UploadData(cleanedData)
		if err != nil {
			return model.JobStatusFailed, fmt.Errorf("error while uploading deleted data: %w", err)
		}
		return status, nil

	} else {
		return model.JobStatusFailed, fmt.Errorf("deletion feature not available for %s destination type", d.Destination.Type)
	}
}

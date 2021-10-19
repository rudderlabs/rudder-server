package deleter

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Delete struct {
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *Delete) DeleteJob(ctx context.Context) (model.JobStatus, error) {
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

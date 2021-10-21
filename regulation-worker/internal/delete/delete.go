package delete

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type deleter interface {
	Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error)
}
type Deleter struct {
	API   deleter
	Batch deleter
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *Deleter) DeleteJob(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	switch dest.Type {
	case "api":
		return d.API.Delete(ctx, job, dest)
	case "batch":
		return d.Batch.Delete(ctx, job, dest)
	default:
		return model.JobStatusFailed, fmt.Errorf("deletion feature not available for %s destination type", dest.Type)

	}
}

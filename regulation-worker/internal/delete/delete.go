package delete

//This is going to declare appropriate struct based on destination type & call `Deleter` method of it.
//to get deletion done.
//called by JobSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type deleter interface {
	Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error)
}
type DeleteSvc struct {
	API   deleter
	Batch deleter
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *DeleteSvc) DeleteSvc(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	switch dest.Type {
	case "api":
		delAPI := MockAPIDeleter{}
		return delAPI.Delete(ctx, job, dest)
	case "batch":
		delBatch := MockBatchDeleter{}
		return delBatch.Delete(ctx, job, dest)
	default:
		fmt.Println("default called")
		return model.JobStatusFailed, fmt.Errorf("deletion feature not available for %s destination type", dest.Type)

	}
}
// }

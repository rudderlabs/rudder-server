package delete

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type apiManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
}

type batchManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
}

type kvManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
}

type DeleteFacade struct {
	AM apiManager
	BM batchManager
	CM kvManager
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *DeleteFacade) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {
	switch destDetail.Type {
	case "api":
		return d.AM.Delete(ctx, job, destDetail.Config, destDetail.Name)
	case "batch":
		return d.BM.Delete(ctx, job, destDetail.Config, destDetail.Name)
	case "kvstore":
		return d.CM.Delete(ctx, job, destDetail.Config, destDetail.Name)
	default:
		return model.JobStatusFailed
	}
}

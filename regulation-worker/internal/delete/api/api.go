package api

//This is simply going to make any API call to transfomer as per the API spec here: https://www.notion.so/rudderstacks/GDPR-Transformer-API-Spec-c3303b5b70c64225815d56c7767f8d22
//to get deletion done.
//called by delete/deleteSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)
import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type deleteManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
}
type API struct {
	DeleteManager deleteManager
}

//prepares payload based on (job,destDetail) & make an API call to transformer.
//gets (status, failure_reason) which is converted to appropriate model.Error & returned to caller.
func (api *API) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {

	return api.DeleteManager.Delete(ctx, job, destConfig, destName)
}

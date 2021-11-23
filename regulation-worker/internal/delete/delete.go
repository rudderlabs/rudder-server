package delete

//This is going to declare appropriate struct based on destination type & call `Deleter` method of it.
//to get deletion done.
//called by JobSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kv_store"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

// type deleter interface {
// 	Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error)
// }
type DeleteFacade struct {
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *DeleteFacade) Delete(ctx context.Context, job model.Job, destDetail model.Destination) (model.JobStatus, error) {
	switch destDetail.Type {
	case "api":
		err:=api.Delete(ctx, job, destDetail.Config, destDetail.Name)
		if err!=nil{
			return model.JobStatusFailed, err
		}
		return model.JobStatusComplete, nil
	case "batch":
		fmt.Println("it's batch type")
		err := batch.Delete(ctx, job, destDetail.Config, destDetail.Name)
		if err != nil {
			fmt.Println("DELETE RETURNED WITH ERROR FACADE: ", err)
			return model.JobStatusFailed, err
		} else {
			return model.JobStatusComplete, nil
		}
	case "kv_store":
		delKVStore := kv_store.KVStore{
			DeleteManager: &kv_store.Mock_KVStoreWorker{},
		}
		return delKVStore.DeleteManager.Delete(ctx, job, destDetail)

	default:
		fmt.Println("default called")
		return model.JobStatusFailed, fmt.Errorf("deletion feature not available for %s destination type", destDetail.Type)

	}
}

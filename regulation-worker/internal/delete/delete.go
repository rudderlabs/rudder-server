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
	// Deleter deleter
}

//get destType & access credentials from workspaceID & destID
//call appropriate struct file type or api type based on destType.
func (d *DeleteFacade) Delete(ctx context.Context, job model.Job, destDetail model.Destination) (model.JobStatus, error) {
	switch destDetail.Type {
	case "api":
		delAPI := api.API{
			DeleteManager: &api.Mock_apiWorker{},
		}
		return delAPI.DeleteManager.Delete(ctx, job, destDetail)
	case "batch":
		batch.Batch{}
		// fmFactory := filemanager.FileManagerFactoryT{}
		// fm, err := fmFactory.New(&filemanager.SettingsT{
		// 	Provider: dest.Name,
		// 	Config:   dest.Config,
		// })
		// if err != nil {
		// 	return model.JobStatusFailed, fmt.Errorf("error while creating file manager: %w", err)
		// }

		// delBatch := batch.Batch{
		// 	FileManager:   fm,
		// 	DeleteManager: &batch.Mock_batchWorker{},
		// }
		return delBatch.DeleteManager.Delete(ctx, job, dest)
	case "kv_store":
		delKVStore := kv_store.KVStore{
			DeleteManager: &kv_store.Mock_KVStoreWorker{},
		}
		return delKVStore.DeleteManager.Delete(ctx, job, dest)

	default:
		fmt.Println("default called")
		return model.JobStatusFailed, fmt.Errorf("deletion feature not available for %s destination type", dest.Type)

	}
}

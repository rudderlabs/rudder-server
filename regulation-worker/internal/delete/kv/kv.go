package batch

//This is going to call appropriate method of KVStoreManager & DeleteManager
//to get deletion done.
//called by delete/deleteSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)
import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
)

type deleteManager interface {
}

type KVStore struct {
	KVStoreManager kvstoremanager.KVStoreManager
	DeleteManager  deleteManager
}

//calls KVStoreManager to download data
//calls deletemanager to delete users from downloaded data
//calls KVStoreManager to upload data
func (b *KVStore) Delete(ctx context.Context, job model.Job, destDetail model.Destination) (status string, err error) {

	return "successful", nil
}

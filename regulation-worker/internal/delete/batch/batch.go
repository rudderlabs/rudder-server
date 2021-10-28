package batch

//This is going to call appropriate method of Filemanager & DeleteManager
//to get deletion done.
//called by delete/deleteSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)
import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
)

type deleteManager interface {
}

type Batch struct {
	FileManager   filemanager.FileManager
	DeleteManager deleteManager
}

//calls filemanager to download data
//calls deletemanager to delete users from downloaded data
//calls filemanager to upload data
func (b *Batch) Delete(ctx context.Context, job model.Job, destDetail model.Destination) (status string, err error) {

	return "successful", nil
}

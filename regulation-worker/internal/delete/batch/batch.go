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
	Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error)
}

type Batch struct {
	FileManager   filemanager.FileManager
	DeleteManager deleteManager
}

//calls filemanager to download data
//calls deletemanager to delete users from downloaded data
//calls filemanager to upload data
func (b *Batch) Delete(ctx context.Context, job model.Job, destDetail model.Destination) (status model.JobStatus, err error) {
	// data, err := getData(job, dest)
	// if err != nil {
	// 	return model.JobStatusFailed, fmt.Errorf("error while getting deletion data: %w", err)
	// }
	// cleanedData, err := deleteData(job, dest, data)
	// if err != nil {
	// 	return model.JobStatusFailed, fmt.Errorf("error while deleting users from destination data: %w", err)
	// }

	// status, err := uploadData(job, dest, cleanedData)
	// if err != nil {
	// 	return model.JobStatusFailed, fmt.Errorf("error while uploading deleted data: %w", err)
	// }
	return b.DeleteManager.Delete(ctx, job, destDetail)

}

func getData(job model.Job, dest model.Destination) (interface{}, error) {
	return nil, nil
}

func deleteData(job model.Job, dest model.Destination, data interface{}) (interface{}, error) {
	return nil, nil
}

func uploadData(job model.Job, dest model.Destination, data interface{}) (model.JobStatus, error) {
	return model.JobStatusComplete, nil
}

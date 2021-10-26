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

// func getDestSpecDetails(){
// 	destDetail := model.Destination{}
// 	for _, source := range config.Sources {
// 		for _, dest := range source.Destinations {
// 			if dest.ID == destID {
// 				destDetail.Config.BucketName = dest.Config["bucketName"]
// 				destDetail.Config.Prefix = dest.Config["prefix"]
// 				destDetail.Config.AccessKeyID = dest.Config["accessKeyID"]
// 				destDetail.Config.AccessKey = dest.Config["accessKey"]
// 				destDetail.Config.EnableSSE = dest.Config["enableSSE"]
// 				destDetail.DestinationID = dest.ID
// 				destDetail.Name = dest.DestinationDefinition.Name
// 			}
// 		}
// 	}

// }

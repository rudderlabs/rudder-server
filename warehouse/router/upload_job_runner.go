package router

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/uploadjob"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
)

// RouterUploadJobRunner implements the UploadJobRunner interface for the router
type RouterUploadJobRunner struct {
	uploadJobFactory *uploadjob.UploadJobFactory
}

// NewRouterUploadJobRunner creates a new router upload job runner
func NewRouterUploadJobRunner(factory *uploadjob.UploadJobFactory) *RouterUploadJobRunner {
	return &RouterUploadJobRunner{
		uploadJobFactory: factory,
	}
}

// RunUploadJob runs an upload job in the router
func (r *RouterUploadJobRunner) RunUploadJob(ctx context.Context, uploadJob *model.UploadJob) error {
	whManager := manager.New(uploadJob.Warehouse.Destination.DestinationDefinition.Name)
	job := r.uploadJobFactory.NewUploadJob(ctx, uploadJob, whManager)
	return job.Run()
}

package slave

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/uploadjob"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
)

// SlaveUploadJobRunner implements the UploadJobRunner interface for the slave
type SlaveUploadJobRunner struct {
	uploadJobFactory *uploadjob.UploadJobFactory
}

// NewSlaveUploadJobRunner creates a new slave upload job runner
func NewSlaveUploadJobRunner(factory *uploadjob.UploadJobFactory) *SlaveUploadJobRunner {
	return &SlaveUploadJobRunner{
		uploadJobFactory: factory,
	}
}

// RunUploadJob runs an upload job in the slave
func (s *SlaveUploadJobRunner) RunUploadJob(ctx context.Context, uploadJob *model.UploadJob) error {
	whManager := manager.New(uploadJob.Warehouse.Destination.DestinationDefinition.Name)
	job := s.uploadJobFactory.NewUploadJob(ctx, uploadJob, whManager)
	return job.Run()
}

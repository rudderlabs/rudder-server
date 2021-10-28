package batch

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Mock_batchWorker struct {
}

func (d *Mock_batchWorker) Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	return model.JobStatusComplete, nil
}

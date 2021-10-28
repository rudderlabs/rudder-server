package api

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Mock_apiWorker struct {
}

func (d *Mock_apiWorker) Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	return model.JobStatusComplete, nil
}

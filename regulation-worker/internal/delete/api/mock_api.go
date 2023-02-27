package api

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Mock_apiWorker struct{}

func (d *Mock_apiWorker) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.Status {
	return model.JobStatusComplete
}

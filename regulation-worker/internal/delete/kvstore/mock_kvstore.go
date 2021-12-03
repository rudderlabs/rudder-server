package kvstore

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Mock_KVStoreWorker struct {
}

func (d *Mock_KVStoreWorker) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {
	return model.JobStatusComplete
}

package kv_store

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type Mock_KVStoreWorker struct {
}

func (d *Mock_KVStoreWorker) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {
	return model.JobStatusComplete
}

package delete

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type MockAPIDeleter struct {
}

func (d *MockAPIDeleter) Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	return callTransformer(ctx, job, dest)
}

func callTransformer(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	//make API call to transformer and get response
	//based on the response set appropriate status string and return
	status := model.JobStatusComplete
	return status, nil
}

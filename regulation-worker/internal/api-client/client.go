package client

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type JobAPI struct {
	WorkspaceID string
	JobID       int
}

func (j *JobAPI) Get(ctx context.Context) (model.Job, error) {
	return model.Job{}, nil
}

func (j *JobAPI) UpdateStatus(ctx context.Context, status string) error {

	return nil
}

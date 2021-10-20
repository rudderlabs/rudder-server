package client

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type JobAPI struct {
	WorkspaceID string
}

//send http request with workspaceID in the url and receives a json payload
//which is decoded using schema and then mapped from schema to internal model.Job struct,
//which is actually returned.
func (j *JobAPI) Get(ctx context.Context) (model.Job, error) {
	return model.Job{}, nil
}

//marshals status into appropriate status schema, and sent as payload
//checked for returned status code.
func (j *JobAPI) UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error {

	return nil
}

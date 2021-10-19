package delete

import "github.com/rudderlabs/rudder-server/regulation-worker/internal/model"

type MockAPIDeleter struct {
	Job         model.Job
	Destination model.Destination
}

func (d *MockAPIDeleter) CallTransformer(job model.Job, dest model.Destination) (model.JobStatus, error) {
	//make API call to transformer and get response
	//based on the response set appropriate status string and return
	status := model.JobStatusComplete
	return status, nil
}

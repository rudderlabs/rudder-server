package delete

import "github.com/rudderlabs/rudder-server/regulation-worker/internal/model"

type MockBatchDeleter struct {
	Job         model.Job
	Destination model.Destination
}

func (d *MockBatchDeleter) GetData(job model.Job, dest model.Destination) (interface{}, error) {
	return nil, nil
}

func (d *MockBatchDeleter) DeleteData(job model.Job, dest model.Destination, data interface{}) (interface{}, error) {
	return nil, nil
}

func (d *MockBatchDeleter) UploadData(job model.Job, dest model.Destination, data interface{}) (model.JobStatus, error) {
	return model.JobStatusComplete, nil
}

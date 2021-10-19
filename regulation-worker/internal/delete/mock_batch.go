package delete

import "github.com/rudderlabs/rudder-server/regulation-worker/internal/model"

type MockBatchDeleter struct {
	Job         model.Job
	Destination model.Destination
}

func (d *MockBatchDeleter) GetData() (interface{}, error) {
	return nil, nil
}

func (d *MockBatchDeleter) DeleteData(data interface{}) (interface{}, error) {
	return nil, nil
}

func (d *MockBatchDeleter) UploadData(data interface{}) (model.JobStatus, error) {
	return model.JobStatusComplete, nil
}

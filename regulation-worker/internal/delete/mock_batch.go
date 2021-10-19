package delete

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type MockBatchDeleter struct {
}

func (d *MockBatchDeleter) Delete(ctx context.Context, job model.Job, dest model.Destination) (model.JobStatus, error) {
	data, err := getData(job, dest)
	if err != nil {
		return model.JobStatusFailed, fmt.Errorf("error while getting deletion data: %w", err)
	}
	cleanedData, err := deleteData(job, dest, data)
	if err != nil {
		return model.JobStatusFailed, fmt.Errorf("error while deleting users from destination data: %w", err)
	}

	status, err := uploadData(job, dest, cleanedData)
	if err != nil {
		return model.JobStatusFailed, fmt.Errorf("error while uploading deleted data: %w", err)
	}
	return status, nil

}

func getData(job model.Job, dest model.Destination) (interface{}, error) {
	return nil, nil
}

func deleteData(job model.Job, dest model.Destination, data interface{}) (interface{}, error) {
	return nil, nil
}

func uploadData(job model.Job, dest model.Destination, data interface{}) (model.JobStatus, error) {
	return model.JobStatusComplete, nil
}

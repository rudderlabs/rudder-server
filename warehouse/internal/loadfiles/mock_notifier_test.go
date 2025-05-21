package loadfiles_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
)

type mockNotifier struct {
	t *testing.T

	requests   []loadfiles.WorkerJobRequest
	requestsV2 []loadfiles.WorkerJobRequestV2
	tables     []string
}

func (n *mockNotifier) Publish(_ context.Context, payload *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error) {
	if payload.JobType == notifier.JobTypeUploadV2 {
		return n.publishV2(payload)
	}
	return n.publish(payload)
}

func (n *mockNotifier) publish(payload *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error) {
	var responses notifier.PublishResponse
	for _, p := range payload.Payloads {
		var req loadfiles.WorkerJobRequest
		err := jsonrs.Unmarshal(p, &req)
		require.NoError(n.t, err)

		var loadFileUploads []loadfiles.LoadFileUpload
		for _, tableName := range n.tables {
			destinationRevisionID := req.DestinationRevisionID

			n.requests = append(n.requests, req)

			loadFileUploads = append(loadFileUploads, loadfiles.LoadFileUpload{
				TableName:             tableName,
				Location:              req.StagingFileLocation + "/" + req.UniqueLoadGenID + "/" + tableName,
				TotalRows:             10,
				ContentLength:         1000,
				StagingFileID:         req.StagingFileID,
				DestinationRevisionID: destinationRevisionID,
				UseRudderStorage:      req.UseRudderStorage,
			})
		}
		jobResponse := loadfiles.WorkerJobResponse{
			StagingFileID: req.StagingFileID,
			Output:        loadFileUploads,
		}
		out, err := jsonrs.Marshal(jobResponse)

		errString := ""
		if err != nil {
			errString = err.Error()
		}

		status := notifier.Succeeded
		if req.StagingFileLocation == "" {
			errString = "staging file location is empty"
			status = notifier.Aborted
		}

		responses.Jobs = append(responses.Jobs, notifier.Job{
			Payload: out,
			Error:   errors.New(errString),
			Status:  status,
		})
	}

	ch := make(chan *notifier.PublishResponse, 1)
	ch <- &responses
	return ch, nil
}

func (n *mockNotifier) publishV2(payload *notifier.PublishRequest) (<-chan *notifier.PublishResponse, error) {
	var responses notifier.PublishResponse
	for _, p := range payload.Payloads {
		var req loadfiles.WorkerJobRequestV2
		err := jsonrs.Unmarshal(p, &req)
		require.NoError(n.t, err)

		var loadFileUploads []loadfiles.LoadFileUpload
		for _, tableName := range n.tables {
			destinationRevisionID := req.DestinationRevisionID

			n.requestsV2 = append(n.requestsV2, req)

			loadFileUploads = append(loadFileUploads, loadfiles.LoadFileUpload{
				TableName:             tableName,
				Location:              req.UniqueLoadGenID + "/" + tableName,
				TotalRows:             len(req.StagingFiles),
				DestinationRevisionID: destinationRevisionID,
				UseRudderStorage:      req.UseRudderStorage,
			})
		}
		jobResponse := loadfiles.WorkerJobResponseV2{
			Output: loadFileUploads,
		}
		out, err := jsonrs.Marshal(jobResponse)

		errString := ""
		if err != nil {
			errString = err.Error()
		}

		status := notifier.Succeeded
		if req.StagingFiles[0].Location == "abort" {
			errString = "staging file location is abort"
			status = notifier.Aborted
		}

		responses.Jobs = append(responses.Jobs, notifier.Job{
			Payload: out,
			Error:   errors.New(errString),
			Status:  status,
		})
	}

	ch := make(chan *notifier.PublishResponse, 1)
	ch <- &responses
	return ch, nil
}

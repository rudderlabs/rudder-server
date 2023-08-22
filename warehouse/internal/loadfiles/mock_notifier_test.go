package loadfiles_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	notifierModel "github.com/rudderlabs/rudder-server/services/notifier/model"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
)

type mockNotifier struct {
	t *testing.T

	requests []loadfiles.WorkerJobRequest
	tables   []string
}

func (n *mockNotifier) Publish(_ context.Context, payload *notifierModel.PublishRequest) (chan notifierModel.PublishResponse, error) {
	var responses notifierModel.PublishResponse
	for _, p := range payload.Jobs {
		var req loadfiles.WorkerJobRequest
		err := json.Unmarshal(p, &req)
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
		out, err := json.Marshal(jobResponse)

		errString := ""
		if err != nil {
			errString = err.Error()
		}

		status := "ok"
		if req.StagingFileLocation == "" {
			errString = "staging file location is empty"
			status = "aborted"
		}

		responses.Notifiers = append(responses.Notifiers, notifierModel.Notifier{
			Payload: out,
			Error:   errors.New(errString),
			Status:  status,
		})
	}

	ch := make(chan notifierModel.PublishResponse, 1)
	ch <- responses
	return ch, nil
}

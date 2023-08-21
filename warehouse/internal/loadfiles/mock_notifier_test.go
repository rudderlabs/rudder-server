package loadfiles_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rudderlabs/rudder-server/services/notifier"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockNotifier struct {
	t *testing.T

	requests []loadfiles.WorkerJobRequest
	tables   []string
}

func (n *mockNotifier) Publish(_ context.Context, payload notifier.MessagePayload, _ *warehouseutils.Schema, _ int) (chan []notifier.Response, error) {
	var responses []notifier.Response
	for _, p := range payload.Jobs {
		var req loadfiles.WorkerJobRequest
		err := json.Unmarshal(p, &req)
		require.NoError(n.t, err)

		var resps []loadfiles.WorkerJobResponse
		for _, tableName := range n.tables {
			destinationRevisionID := req.DestinationRevisionID

			n.requests = append(n.requests, req)

			resps = append(resps, loadfiles.WorkerJobResponse{
				TableName:             tableName,
				Location:              req.StagingFileLocation + "/" + req.UniqueLoadGenID + "/" + tableName,
				TotalRows:             10,
				ContentLength:         1000,
				StagingFileID:         req.StagingFileID,
				DestinationRevisionID: destinationRevisionID,
				UseRudderStorage:      req.UseRudderStorage,
			})
		}
		out, err := json.Marshal(resps)

		errString := ""
		if err != nil {
			errString = err.Error()
		}

		status := "ok"
		if req.StagingFileLocation == "" {
			errString = "staging file location is empty"
			status = "aborted"
		}

		responses = append(responses, notifier.Response{
			JobID:  req.StagingFileID,
			Output: out,
			Error:  errString,
			Status: status,
		})
	}

	ch := make(chan []notifier.Response, 1)
	ch <- responses
	return ch, nil
}

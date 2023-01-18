package loadfiles_test

import (
	"encoding/json"
	"testing"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockNotifier struct {
	t *testing.T

	requests []loadfiles.WorkerJobRequest
	tables   []string
}

func (n *mockNotifier) Publish(payload pgnotifier.MessagePayload, schema *warehouseutils.SchemaT, priority int) (chan []pgnotifier.ResponseT, error) {
	var responses []pgnotifier.ResponseT
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

		responses = append(responses, pgnotifier.ResponseT{
			JobID:  req.StagingFileID,
			Output: json.RawMessage(out),
			Error:  errString,
			Status: status,
		})
	}

	ch := make(chan []pgnotifier.ResponseT, 1)
	ch <- responses
	return ch, nil
}

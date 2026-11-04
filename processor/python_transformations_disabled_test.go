package processor

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// TestPythonTransformationsDisabledAbortsJobs feeds the real user transformer's answer for a Python transformation on
// a deployment that does not offer Python (both PyT flags off) to the processor's failure handling, and checks the
// events end up as aborted jobs with an aborted user_transformer report, with no panic on the way.
func TestPythonTransformationsDisabledAbortsJobs(t *testing.T) {
	processor, _, c, _ := newVisibilityProcessor(t, true)
	defer c.Finish()

	conf := config.New()
	conf.Set("Processor.UserTransformer.pythonTransformationsEnabled", false)
	conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", false)
	userTransformer := user_transformer.New(conf, logger.NOP, stats.NOP)

	commonMetadata := types.Metadata{SourceID: SourceIDEnabled, DestinationID: DestinationIDEnabledA}
	var inputEvents []types.TransformerEvent
	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt)
	for i := range 3 {
		msgID := fmt.Sprintf("msg%d", i+1)
		event := types.SingularEventT{"messageId": msgID}
		metadata := commonMetadata
		metadata.MessageID = msgID
		metadata.WorkspaceID = "ws-1"
		eventsByMessageID[msgID] = types.SingularEventWithReceivedAt{SingularEvent: event, ReceivedAt: time.Now()}
		inputEvents = append(inputEvents, types.TransformerEvent{
			Metadata: metadata,
			Message:  event,
			Destination: backendconfig.DestinationT{
				ID: DestinationIDEnabledA,
				Transformations: []backendconfig.TransformationT{
					{ID: "transformation-1", VersionID: "version-1", Language: "pythonfaas"},
				},
			},
		})
	}

	var response types.Response
	require.NotPanics(t, func() { response = userTransformer.Transform(context.Background(), inputEvents) })
	require.Empty(t, response.Events)

	m := processor.getNonSuccessfulMetrics(
		response, inputEvents, &commonMetadata, eventsByMessageID, reportingtypes.USER_TRANSFORMER, responseStatusCode,
	)

	require.Empty(t, m.filteredJobs, "an abort is a failure, not a filter")
	require.Len(t, m.failedJobs, len(inputEvents))
	for _, j := range m.failedJobs {
		var params map[string]any
		require.NoError(t, jsonrs.Unmarshal(j.Parameters, &params))
		require.EqualValues(t, http.StatusBadRequest, params["status_code"])
		require.Equal(t, user_transformer.ErrPythonTransformationsDisabled.Error(), params["error"])
		require.Equal(t, reportingtypes.USER_TRANSFORMER, params["stage"])
	}

	require.Len(t, m.failedMetrics, 1)
	require.Equal(t, reportingtypes.USER_TRANSFORMER, m.failedMetrics[0].PU)
	require.Equal(t, jobsdb.Aborted.State, m.failedMetrics[0].StatusDetail.Status)
	require.Equal(t, http.StatusBadRequest, m.failedMetrics[0].StatusDetail.StatusCode)
	require.EqualValues(t, len(inputEvents), m.failedMetrics[0].StatusDetail.Count)
}

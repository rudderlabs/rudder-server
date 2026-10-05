package router

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockdestinationdebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/router/types"
)

func newLiveEventsTestWorker(t *testing.T, flag, captureOn bool) *worker {
	t.Helper()
	ctrl := gomock.NewController(t)
	dbg := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
	dbg.EXPECT().HasUploadEnabled("dest-1").Return(captureOn).AnyTimes()
	w := &worker{rt: &Handle{
		destType:                        "CUSTOM_AUDIENCE",
		debugger:                        dbg,
		saveDestinationResponseOverride: config.SingleValueLoader(false),
		liveEventsSuccessResponse:       config.SingleValueLoader(flag),
		logger:                          logger.NOP,
	}}
	w.rt.supportsDeliveredWithWarnings.Store(true)
	return w
}

func liveEventsDestinationJob(jobIDs ...int64) types.DestinationJobT {
	metadata := make([]types.JobMetadataT, 0, len(jobIDs))
	for _, id := range jobIDs {
		metadata = append(metadata, types.JobMetadataT{JobID: id, DestinationID: "dest-1", WorkspaceID: "ws-1"})
	}
	return types.DestinationJobT{Destination: backendconfig.DestinationT{ID: "dest-1"}, JobMetadataArray: metadata}
}

func TestPrepareRouterJobResponsesLiveEventsKeepsBody(t *testing.T) {
	w := newLiveEventsTestWorker(t, true, true)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1, 2, 3),
		map[int64]int{1: 200, 2: 200, 3: 200},
		map[int64]string{1: `{"handles":["h1"]}`, 2: `{"handles":["h1"]}`, 3: `{"handles":["h1"]}`}, "")
	require.Len(t, responses, 3)
	for _, r := range responses {
		require.Equal(t, "", r.respBody, "the jobs database body stays blank")
		require.Equal(t, `{"handles":["h1"]}`, r.liveEventsRespBody)
	}
}

func TestPrepareRouterJobResponsesLiveEventsFlagOff(t *testing.T) {
	w := newLiveEventsTestWorker(t, false, true)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
		map[int64]int{1: 200}, map[int64]string{1: `{"handles":["h1"]}`}, "")
	require.Equal(t, "", responses[0].respBody)
	require.Equal(t, "", responses[0].liveEventsRespBody)
}

func TestPrepareRouterJobResponsesLiveEventsCaptureOff(t *testing.T) {
	w := newLiveEventsTestWorker(t, true, false)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
		map[int64]int{1: 200}, map[int64]string{1: `{"handles":["h1"]}`}, "")
	require.Equal(t, "", responses[0].liveEventsRespBody)
}

func TestPrepareRouterJobResponsesLiveEventsEmptyBody(t *testing.T) {
	w := newLiveEventsTestWorker(t, true, true)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
		map[int64]int{1: 200}, map[int64]string{1: ""}, "")
	require.Equal(t, "", responses[0].liveEventsRespBody)
}

func TestPrepareRouterJobResponsesLiveEventsFailureNotCopied(t *testing.T) {
	w := newLiveEventsTestWorker(t, true, true)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
		map[int64]int{1: 400}, map[int64]string{1: `{"error":"bad"}`}, "")
	require.Equal(t, `{"error":"bad"}`, responses[0].respBody, "failures keep their body as today")
	require.Equal(t, "", responses[0].liveEventsRespBody, "no second copy for a body the status already carries")
}

func TestPrepareRouterJobResponsesLiveEventsSaveOverrideWins(t *testing.T) {
	w := newLiveEventsTestWorker(t, false, true)
	w.rt.saveDestinationResponseOverride = config.SingleValueLoader(true)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
		map[int64]int{1: 200}, map[int64]string{1: "ok"}, "")
	require.Equal(t, "ok", responses[0].respBody)
	require.Equal(t, "", responses[0].liveEventsRespBody)
}

func TestLiveEventsSuccessResponseConfigKeys(t *testing.T) {
	require.Equal(t,
		[]string{"Router.CUSTOM_AUDIENCE.liveEventsSuccessResponse", "Router.liveEventsSuccessResponse"},
		getRouterConfigKeys("liveEventsSuccessResponse", "CUSTOM_AUDIENCE"))
}

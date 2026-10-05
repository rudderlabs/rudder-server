package router

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mockdestinationdebugger "github.com/rudderlabs/rudder-server/mocks/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
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

func liveEventsJobResponses(t *testing.T, jobIDs []int64, statusCode int, statusBody, liveBody string) []*JobResponse {
	t.Helper()
	destinationJob := liveEventsDestinationJob(jobIDs...)
	destinationJob.Message = json.RawMessage(`{"item_type":"HOME_LISTING"}`)
	responses := make([]*JobResponse, 0, len(jobIDs))
	for i := range destinationJob.JobMetadataArray {
		md := destinationJob.JobMetadataArray[i]
		md.JobT = &jobsdb.JobT{JobID: md.JobID, Parameters: json.RawMessage(`{}`)}
		status := &jobsdb.JobStatusT{
			JobID:         md.JobID,
			JobState:      jobsdb.Succeeded.State,
			ErrorCode:     strconv.Itoa(statusCode),
			ErrorResponse: routerutils.EnhanceJSON(routerutils.EmptyPayload, "response", statusBody),
			WorkspaceId:   "ws-1",
		}
		responses = append(responses, &JobResponse{
			jobID: md.JobID, destinationJob: &destinationJob, destinationJobMetadata: &md,
			respStatusCode: statusCode, liveEventsRespBody: liveBody, status: status,
		})
	}
	return responses
}

func newRecordingWorker(t *testing.T, records *[]*destinationdebugger.DeliveryStatusT) *worker {
	t.Helper()
	ctrl := gomock.NewController(t)
	dbg := mockdestinationdebugger.NewMockDestinationDebugger(ctrl)
	dbg.EXPECT().RecordEventDeliveryStatus("dest-1", gomock.Any()).
		DoAndReturn(func(_ string, s *destinationdebugger.DeliveryStatusT) bool {
			*records = append(*records, s)
			return true
		}).AnyTimes()
	return &worker{rt: &Handle{destType: "CUSTOM_AUDIENCE", debugger: dbg, logger: logger.NOP}}
}

func TestSendLiveEventsCarriesKeptBody(t *testing.T) {
	var records []*destinationdebugger.DeliveryStatusT
	w := newRecordingWorker(t, &records)
	responses := liveEventsJobResponses(t, []int64{1}, 200, "", `{"handles":["h1"]}`)
	statusBefore := string(responses[0].status.ErrorResponse)

	w.sendLiveEvents(responses)

	require.Len(t, records, 1)
	require.Equal(t, `{"handles":["h1"]}`, gjson.GetBytes(records[0].ErrorResponse, "response").String())
	require.Equal(t, statusBefore, string(responses[0].status.ErrorResponse), "the job status is never modified")
	require.Equal(t, "", gjson.GetBytes(responses[0].status.ErrorResponse, "response").String())
}

func TestSendLiveEventsWithoutKeptBodyUnchanged(t *testing.T) {
	var records []*destinationdebugger.DeliveryStatusT
	w := newRecordingWorker(t, &records)
	w.sendLiveEvents(liveEventsJobResponses(t, []int64{1}, 200, "", ""))
	require.Len(t, records, 1)
	require.Equal(t, "", gjson.GetBytes(records[0].ErrorResponse, "response").String())
}

func TestSendLiveEventsFailureUnchanged(t *testing.T) {
	var records []*destinationdebugger.DeliveryStatusT
	w := newRecordingWorker(t, &records)
	w.sendLiveEvents(liveEventsJobResponses(t, []int64{1}, 400, `{"error":"bad"}`, ""))
	require.Len(t, records, 1)
	require.Equal(t, `{"error":"bad"}`, gjson.GetBytes(records[0].ErrorResponse, "response").String())
}

func TestSendLiveEventsTrimsTo10KB(t *testing.T) {
	var records []*destinationdebugger.DeliveryStatusT
	w := newRecordingWorker(t, &records)
	big := strings.Repeat("a", 12*1024)
	w.sendLiveEvents(liveEventsJobResponses(t, []int64{1}, 200, "", big))
	require.Len(t, gjson.GetBytes(records[0].ErrorResponse, "response").String(), 10*1024)
}

func TestSendLiveEventsKeepsPlainTextBody(t *testing.T) {
	var records []*destinationdebugger.DeliveryStatusT
	w := newRecordingWorker(t, &records)
	w.sendLiveEvents(liveEventsJobResponses(t, []int64{1}, 200, "", "<html>OK</html>"))
	require.Equal(t, "<html>OK</html>", gjson.GetBytes(records[0].ErrorResponse, "response").String())
}

func TestSendLiveEventsOneRecordPerCall(t *testing.T) {
	var records []*destinationdebugger.DeliveryStatusT
	w := newRecordingWorker(t, &records)
	responses := liveEventsJobResponses(t, []int64{1, 2, 3}, 200, "", `{"handles":["h1"]}`)
	w.sendLiveEvents(responses)
	require.Len(t, records, 1, "one record per destination job (= one HTTP call)")
	require.Equal(t, `{"handles":["h1"]}`, gjson.GetBytes(records[0].ErrorResponse, "response").String())
	for _, r := range responses {
		require.Equal(t, "", gjson.GetBytes(r.status.ErrorResponse, "response").String())
	}
}

func TestPrepareRouterJobResponsesLiveEventsFlagOffCaptureOff(t *testing.T) {
	w := newLiveEventsTestWorker(t, false, false)
	responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
		map[int64]int{1: 200}, map[int64]string{1: `{"handles":["h1"]}`}, "")
	require.Equal(t, "", responses[0].respBody)
	require.Equal(t, "", responses[0].liveEventsRespBody)
}

func TestPrepareRouterJobResponsesLiveEventsNoCopyWithoutDelivery(t *testing.T) {
	// 298 (filtered) and 299 (suppressed) make no HTTP call; their bodies are router text, not a reply.
	for _, code := range []int{utilTypes.FilterEventCode, utilTypes.SuppressEventCode} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			w := newLiveEventsTestWorker(t, true, true)
			responses := w.prepareRouterJobResponses(liveEventsDestinationJob(1),
				map[int64]int{1: code}, map[int64]string{1: "Event filtered"}, "")
			require.Equal(t, "", responses[0].liveEventsRespBody)
		})
	}
}

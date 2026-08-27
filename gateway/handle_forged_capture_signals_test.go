package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

// newJobDataTestGateway builds a fully Setup Handle backed by mocks, mirroring the
// "jobDataFromRequest" harness in gateway_test.go. getJobDataFromRequest is exercised directly
// (bypassing the HTTP/auth layer and the DB write path entirely), so job.Parameters/EventPayload can
// be asserted on without mocking jobsdb.Store/WithStoreSafeTx.
func newJobDataTestGateway(t *testing.T) *Handle {
	t.Helper()

	config.Reset()
	admin.Init()
	logger.Reset()
	misc.Init()

	mockCtrl := gomock.NewController(t)

	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{}).AnyTimes()

	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{}, Topic: string(topic)}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch
		}).AnyTimes()

	conf := config.New()
	conf.Set("Gateway.enableRateLimit", false)

	gw := &Handle{}
	err := gw.Setup(
		context.Background(),
		conf,
		logger.NOP,
		stats.NOP,
		mockApp,
		mockBackendConfig,
		mocksJobsDB.NewMockJobsDB(mockCtrl),
		nil,
		func(http.ResponseWriter, *http.Request) {},
		rsources.NewNoOpService(),
		transformer.NewNoOpService(),
		sourcedebugger.NewNoOpService(),
		nil,
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		select {
		case <-gw.backendConfigInitialisedChan:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "backend config never initialised")

	t.Cleanup(func() {
		require.NoError(t, gw.Shutdown())
		mockCtrl.Finish()
	})

	return gw
}

// TestGetJobDataFromRequest_ForgedCaptureSignalsAreInert covers the gateway half of A3.5.
//
// The rETL "capture the final recorded error" opt-in is resolved server-side at the processor, from
// the connection's backend config. The gateway has no intake for it: an earlier revision parsed an
// X-Rudder-Capture-Error-Detail header, and the one before that a context.sources.capture_error
// payload field. Neither is read any more, so a request carrying both must produce no capture
// parameter - and must leave the payload exactly as it arrived, since nothing strips a field nobody
// consumes.
func TestGetJobDataFromRequest_ForgedCaptureSignalsAreInert(t *testing.T) {
	gw := newJobDataTestGateway(t)

	r := httptest.NewRequest(http.MethodPost, "/internal/v1/retl", http.NoBody)
	r.Header.Set("X-Rudder-Job-Run-Id", "run-1")
	r.Header.Set("X-Rudder-Task-Run-Id", "task-1")
	r.Header.Set("X-Rudder-Capture-Error-Detail", "true")

	arctx := &gwtypes.AuthRequestContext{
		WriteKey:      "capture-error-write-key",
		SourceID:      "capture-error-source",
		WorkspaceID:   "capture-error-workspace",
		DestinationID: "capture-error-destination",
	}
	augmentAuthRequestContext(arctx, r)
	require.Equal(t, "run-1", arctx.SourceJobRunID, "the job run id header is still parsed")

	payload, err := jsonrs.Marshal(map[string]any{"batch": []map[string]any{{
		"type":      "track",
		"event":     "purchase",
		"userId":    "user-1",
		"messageId": "message-1",
		"context": map[string]any{
			"sources": map[string]any{
				"job_run_id":    "run-1",
				"task_run_id":   "task-1",
				"capture_error": true,
			},
		},
	}}})
	require.NoError(t, err)

	jobData, err := gw.getJobDataFromRequest(&webRequestT{
		reqType:        "batch",
		authContext:    arctx,
		done:           make(chan<- string),
		userIDHeader:   "userIDHeader",
		requestPayload: payload,
	})
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	params := jobData.jobs[0].Parameters
	require.False(t, gjson.GetBytes(params, "capture_error").Exists(),
		"neither the header nor the payload may produce a capture parameter, got: %s", params)

	keys := make([]string, 0, 6)
	gjson.ParseBytes(params).ForEach(func(key, _ gjson.Result) bool {
		keys = append(keys, key.String())
		return true
	})
	require.ElementsMatch(t, []string{
		"source_id", "source_job_run_id", "source_task_run_id", "traceparent", "source_category", "destination_id",
	}, keys, "the gateway must not add any capture-related parameter")

	require.True(t, gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context.sources.capture_error").Bool(),
		"a forged payload field is inert, not stripped")
}

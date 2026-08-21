package gateway

import (
	"bytes"
	"context"
	"net/http"
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

// newCaptureErrorTestGateway builds a fully Setup Handle backed by mocks, mirroring the
// "jobDataFromRequest" harness in gateway_test.go. getJobDataFromRequest is exercised directly
// (bypassing the HTTP/auth layer and the DB write path entirely), so job.Parameters/EventPayload
// can be asserted on without needing to mock jobsdb.Store/WithStoreSafeTx.
func newCaptureErrorTestGateway(t *testing.T) *Handle {
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

	mockJobsDB := mocksJobsDB.NewMockJobsDB(mockCtrl)

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
		mockJobsDB,
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

// captureErrorAuthContext returns an AuthRequestContext with no pre-authenticated job run id, so
// tests control rETL-ness purely through the payload's context.sources.job_run_id.
func captureErrorAuthContext() *gwtypes.AuthRequestContext {
	return &gwtypes.AuthRequestContext{
		WriteKey:    "capture-error-write-key",
		SourceID:    "capture-error-source",
		WorkspaceID: "capture-error-workspace",
	}
}

// buildCaptureErrorRequest marshals events into a {"batch": [...]} payload and wraps it in a
// webRequestT, exactly as the existing "jobDataFromRequest" Ginkgo tests do.
func buildCaptureErrorRequest(t *testing.T, authCtx *gwtypes.AuthRequestContext, events []map[string]any) *webRequestT {
	t.Helper()

	payload, err := jsonrs.Marshal(map[string]any{"batch": events})
	require.NoError(t, err)

	return &webRequestT{
		reqType:        "batch",
		authContext:    authCtx,
		done:           make(chan<- string),
		userIDHeader:   "userIDHeader",
		requestPayload: payload,
	}
}

// A3.1: rETL batch (context.sources.job_run_id set) whose first event has
// context.sources.capture_error: true. params["capture_error"] must be a real JSON boolean true.
func TestGetJobDataFromRequest_CaptureError_HonoredOnRETL(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-1",
			"context": map[string]any{
				"sources": map[string]any{
					"job_run_id":    "run-1",
					"capture_error": true,
				},
			},
		},
	}
	req := buildCaptureErrorRequest(t, captureErrorAuthContext(), events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	captureError := gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error")
	require.True(t, captureError.Exists(), "capture_error should be present in params")
	require.Equal(t, "true", captureError.Raw, "capture_error must be encoded as a JSON boolean literal, not a string")
	require.True(t, captureError.Bool())
}

// A3.2: same shape, >=2 events, EVERY event carries capture_error. The stored EventPayload must
// contain no capture_error anywhere, while job_run_id (a sibling key) must survive untouched.
func TestGetJobDataFromRequest_CaptureError_StrippedFromEveryEvent(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-1",
			"context": map[string]any{
				"sources": map[string]any{
					"job_run_id":    "run-2",
					"capture_error": true,
				},
			},
		},
		{
			"type":   "track",
			"userId": "user-2",
			"context": map[string]any{
				"sources": map[string]any{
					"capture_error": true,
				},
			},
		},
	}
	req := buildCaptureErrorRequest(t, captureErrorAuthContext(), events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 2)

	for i, job := range jobData.jobs {
		require.Falsef(t, gjson.GetBytes(job.EventPayload, "batch.0.context.sources.capture_error").Exists(),
			"job %d: capture_error must be stripped from the stored payload", i)
		require.Falsef(t, bytes.Contains(job.EventPayload, []byte("capture_error")),
			"job %d: capture_error string must not appear anywhere in the payload bytes", i)
	}

	// job_run_id is a sibling key under context.sources on the first event only; it must survive
	// the strip, proving delete() removed a single key rather than clobbering the whole map.
	jobRunID := gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context.sources.job_run_id")
	require.True(t, jobRunID.Exists())
	require.Equal(t, "run-2", jobRunID.String())
}

// A3.3: capture_error: true but no job_run_id anywhere (not on the event, not pre-authenticated) -
// non-rETL traffic. The key must be absent from params even though the flag was requested.
func TestGetJobDataFromRequest_CaptureError_AbsentWithoutJobRunID(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-3",
			"context": map[string]any{
				"sources": map[string]any{
					"capture_error": true,
				},
			},
		},
	}
	authCtx := captureErrorAuthContext()
	require.Empty(t, authCtx.SourceJobRunID, "test assumes no pre-authenticated job run id")
	req := buildCaptureErrorRequest(t, authCtx, events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	require.False(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists(),
		"capture_error must not be honored on non-rETL traffic")
	require.False(t, gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context.sources.capture_error").Exists(),
		"capture_error must still be stripped from the payload even when not honored")
}

// A3.4: capture_error delivered as the JSON string "true" (not a boolean) must be rejected - only
// a real JSON boolean true counts.
func TestGetJobDataFromRequest_CaptureError_RejectsStringTrue(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-4",
			"context": map[string]any{
				"sources": map[string]any{
					"job_run_id":    "run-4",
					"capture_error": "true",
				},
			},
		},
	}
	req := buildCaptureErrorRequest(t, captureErrorAuthContext(), events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	require.False(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists(),
		"a string \"true\" must not enable capture_error")
	require.False(t, gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context.sources.capture_error").Exists(),
		"capture_error must still be stripped from the payload even when rejected")
}

// Regression: requests that never mention capture_error, or whose context.sources isn't shaped as
// expected, must not panic and must never gain a capture_error parameter.
func TestGetJobDataFromRequest_CaptureError_RegressionNoPanic(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	tests := []struct {
		name  string
		event map[string]any
	}{
		{
			name: "no context key at all",
			event: map[string]any{
				"type":   "track",
				"userId": "user-5",
			},
		},
		{
			name: "context present without sources",
			event: map[string]any{
				"type":   "track",
				"userId": "user-6",
				"context": map[string]any{
					"library": map[string]any{"name": "sdk", "version": "1.0.0"},
				},
			},
		},
		{
			name: "context.sources is a string, not a map",
			event: map[string]any{
				"type":   "track",
				"userId": "user-7",
				"context": map[string]any{
					"sources": "not-a-map",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildCaptureErrorRequest(t, captureErrorAuthContext(), []map[string]any{tt.event})

			jobData, err := gw.getJobDataFromRequest(req)
			require.NoError(t, err)
			require.Len(t, jobData.jobs, 1)
			require.False(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists())
		})
	}
}

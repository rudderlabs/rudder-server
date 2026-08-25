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

// captureErrorAuthContext returns an AuthRequestContext with no pre-authenticated job run id and
// no capture opt-in; tests opt in by setting CaptureErrorDetail (the value
// augmentAuthRequestContext seeds from the X-Rudder-Capture-Error-Detail header).
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

// A3.1: rETL request whose auth context carries the header opt-in (CaptureErrorDetail) and a job
// run id. params["capture_error"] must be a real JSON boolean true.
func TestGetJobDataFromRequest_CaptureError_HonoredOnRETL(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	authCtx := captureErrorAuthContext()
	authCtx.SourceJobRunID = "run-1"
	authCtx.SourceTaskRunID = "task-run-1"
	authCtx.CaptureErrorDetail = true

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-1",
		},
	}
	req := buildCaptureErrorRequest(t, authCtx, events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	captureError := gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error")
	require.True(t, captureError.Exists(), "capture_error should be present in params")
	require.Equal(t, "true", captureError.Raw, "capture_error must be encoded as a JSON boolean literal, not a string")
	require.True(t, captureError.Bool())
}

// A3.1b: the opt-in also combines with a job run id read from the first event's payload (the
// legacy job_run_id delivery), not only the pre-authenticated header value.
func TestGetJobDataFromRequest_CaptureError_HonoredWithPayloadJobRunID(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	authCtx := captureErrorAuthContext()
	authCtx.CaptureErrorDetail = true

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-1",
			"context": map[string]any{
				"sources": map[string]any{
					"job_run_id": "run-1",
				},
			},
		},
	}
	req := buildCaptureErrorRequest(t, authCtx, events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	require.True(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Bool())
}

// A3.2: the opt-in rides the request header only. A context.sources.capture_error field inside the
// payload is user data: it must not switch capture on, and the gateway must not mutate it away --
// the stored payload keeps the field byte-for-byte.
func TestGetJobDataFromRequest_CaptureError_PayloadFlagIgnoredAndUntouched(t *testing.T) {
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
		require.Falsef(t, gjson.GetBytes(job.Parameters, "capture_error").Exists(),
			"job %d: a payload capture_error must not be honored", i)
		require.Truef(t, gjson.GetBytes(job.EventPayload, "batch.0.context.sources.capture_error").Exists(),
			"job %d: the payload field is user data and must pass through unmodified", i)
	}

	jobRunID := gjson.GetBytes(jobData.jobs[0].EventPayload, "batch.0.context.sources.job_run_id")
	require.True(t, jobRunID.Exists())
	require.Equal(t, "run-2", jobRunID.String())
}

// A3.3: header opt-in set but no job run id anywhere (neither pre-authenticated nor in the
// payload) - non-rETL traffic. The key must be absent from params even though the flag was
// requested.
func TestGetJobDataFromRequest_CaptureError_AbsentWithoutJobRunID(t *testing.T) {
	gw := newCaptureErrorTestGateway(t)

	authCtx := captureErrorAuthContext()
	authCtx.CaptureErrorDetail = true
	require.Empty(t, authCtx.SourceJobRunID, "test assumes no pre-authenticated job run id")

	events := []map[string]any{
		{
			"type":   "track",
			"userId": "user-3",
		},
	}
	req := buildCaptureErrorRequest(t, authCtx, events)

	jobData, err := gw.getJobDataFromRequest(req)
	require.NoError(t, err)
	require.Len(t, jobData.jobs, 1)

	require.False(t, gjson.GetBytes(jobData.jobs[0].Parameters, "capture_error").Exists(),
		"capture_error must not be honored on non-rETL traffic")
}

// A3.4: only the exact header value "true" opts in - case variants, "1", garbage and absence all
// fall back to false.
func TestAugmentAuthRequestContext_CaptureErrorDetail(t *testing.T) {
	tests := []struct {
		name   string
		header *string
		want   bool
	}{
		{name: "exact true", header: strPtr("true"), want: true},
		{name: "uppercase TRUE", header: strPtr("TRUE"), want: false},
		{name: "numeric 1", header: strPtr("1"), want: false},
		{name: "empty value", header: strPtr(""), want: false},
		{name: "garbage", header: strPtr("yes"), want: false},
		{name: "absent", header: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/internal/v1/retl", nil)
			r.Header.Set("X-Rudder-Job-Run-Id", "run-4")
			r.Header.Set("X-Rudder-Task-Run-Id", "task-run-4")
			if tt.header != nil {
				r.Header.Set("X-Rudder-Capture-Error-Detail", *tt.header)
			}

			arctx := &gwtypes.AuthRequestContext{}
			augmentAuthRequestContext(arctx, r)

			require.Equal(t, tt.want, arctx.CaptureErrorDetail)
			require.Equal(t, "run-4", arctx.SourceJobRunID, "job run id seeding must be unaffected")
			require.Equal(t, "task-run-4", arctx.SourceTaskRunID, "task run id seeding must be unaffected")
		})
	}
}

func strPtr(s string) *string { return &s }

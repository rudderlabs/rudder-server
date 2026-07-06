package cpservice

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/processor/internal/pytscaler"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

func TestForward(t *testing.T) {
	t.Run("dispatches each op to its endpoint method and passes status/body through", func(t *testing.T) {
		cases := []struct {
			op           proto.Op
			wantEndpoint string
		}{
			{proto.Op_OP_TEST, "test"},
			{proto.Op_OP_TEST_RUN, "testRun"},
			{proto.Op_OP_TEST_LIBRARY, "testLibrary"},
			{proto.Op_OP_EXTRACT_LIBS, "extractLibs"},
		}
		for _, tc := range cases {
			t.Run(tc.op.String(), func(t *testing.T) {
				fwd := &fakeForwarder{statusCode: 201, body: []byte(`{"ok":true}`)}
				svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), fwd)

				resp, err := svc.Forward(context.Background(), &proto.ForwardRequest{
					Op: tc.op, WorkspaceId: "ws-1", Payload: []byte(`{"a":1}`),
				})
				require.NoError(t, err)
				require.Equal(t, int32(201), resp.StatusCode)
				require.Equal(t, []byte(`{"ok":true}`), resp.Body)
				require.Equal(t, tc.wantEndpoint, fwd.gotEndpoint)
				require.Equal(t, "ws-1", fwd.gotWorkspaceID)
				require.Equal(t, []byte(`{"a":1}`), fwd.gotPayload)
			})
		}
	})

	t.Run("rejects an unknown or unspecified op with InvalidArgument", func(t *testing.T) {
		svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), &fakeForwarder{})
		for _, op := range []proto.Op{proto.Op_OP_UNSPECIFIED, proto.Op(99)} {
			_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: op, WorkspaceId: "ws-1"})
			require.Equalf(t, codes.InvalidArgument, status.Code(err), "op %v", op)
		}
	})

	t.Run("rejects workspaceIds that could escape the pyt URL template", func(t *testing.T) {
		cases := []struct {
			name        string
			workspaceID string
		}{
			{"empty", ""},
			{"userinfo and fragment (SSRF)", "a@evil.com#"},
			{"path separator", "a/b"},
			{"port separator", "a:8080"},
			{"query separator", "a?x=1"},
			{"path traversal", "../etc"},
			{"whitespace", "ws 1"},
			{"longer than a DNS label allows", strings.Repeat("a", 60)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fwd := &fakeForwarder{statusCode: 200}
				svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), fwd)
				_, err := svc.Forward(context.Background(),
					&proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: tc.workspaceID})
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.Empty(t, fwd.gotEndpoint, "an invalid workspaceId must never reach the forwarder")
			})
		}
	})

	t.Run("accepts well-formed workspaceIds", func(t *testing.T) {
		for _, id := range []string{"ws-1", "2CJhY0aBcDeFgHiJkLmNoPqRsTu", strings.Repeat("a", 59)} {
			fwd := &fakeForwarder{statusCode: 200}
			svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), fwd)
			_, err := svc.Forward(context.Background(),
				&proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: id})
			require.NoErrorf(t, err, "workspaceId %q", id)
			require.Equal(t, id, fwd.gotWorkspaceID)
		}
	})

	t.Run("asks the scaler for the configured replica target", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.UserTransformer.pytTestScaleReplicas", 3)
		scaler := &fakeScaler{}
		svc := newService(t, conf, scaler, &fakeForwarder{statusCode: 200})

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.NoError(t, err)
		require.True(t, scaler.called)
		require.Equal(t, 3, scaler.count)
	})

	t.Run("returns FailedPrecondition when the pyt deployment is missing", func(t *testing.T) {
		scaler := &fakeScaler{err: pytscaler.ErrDeploymentNotFound}
		fwd := &fakeForwarder{}
		svc := newService(t, nil, scaler, fwd)

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Empty(t, fwd.gotEndpoint, "a failed scale must not be forwarded")
	})

	t.Run("returns Internal when scaling fails", func(t *testing.T) {
		scaler := &fakeScaler{err: errors.New("k8s api unavailable")}
		fwd := &fakeForwarder{}
		svc := newService(t, nil, scaler, fwd)

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.Internal, status.Code(err))
		require.Empty(t, fwd.gotEndpoint, "a failed scale must not be forwarded")
	})

	t.Run("returns FailedPrecondition when per-workspace PyT is not enabled", func(t *testing.T) {
		svc := newService(t, nil, pytscaler.NewNoop(logger.NOP),
			&fakeForwarder{err: user_transformer.ErrPerWorkspacePyTNotEnabled})
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("returns Unavailable when the forward fails", func(t *testing.T) {
		svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), &fakeForwarder{err: errors.New("connection refused")})
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	// Run with -race to catch unsynchronised access to Service state under
	// concurrent Forwards. singleflight collapses same-workspace calls onto the
	// in-flight EnsureScaled, whose first execution scales and leaves the replicas
	// non-zero, so a burst scales the workspace up exactly once. The first
	// EnsureScaled is held open so, without singleflight, every request would
	// see zero replicas and scale — making this a real regression test for the
	// collapse.
	t.Run("scales a workspace up exactly once under a concurrent burst", func(t *testing.T) {
		scaler := &countingScaler{block: make(chan struct{})}
		svc := newService(t, nil, scaler, stubForwarder{statusCode: 200})

		const n = 20
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				_, err := svc.Forward(context.Background(),
					&proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1", Payload: []byte(`{}`)})
				assert.NoError(t, err)
			})
		}
		// Let the goroutines pile onto the in-flight scale check before releasing.
		require.Eventually(t, func() bool { return scaler.waiting.Load() > 0 }, time.Second, time.Millisecond)
		close(scaler.block)
		wg.Wait()

		require.Equal(t, int32(1), scaler.scales.Load(), "a concurrent burst must scale the workspace up exactly once")
	})
}

// fakeForwarder records which endpoint method was called and returns a canned
// response, so tests can assert Forward dispatches each op to the right method.
type fakeForwarder struct {
	gotEndpoint    string
	gotWorkspaceID string
	gotPayload     []byte

	statusCode int
	body       []byte
	err        error
}

func (f *fakeForwarder) record(endpoint, workspaceID string, payload []byte) (int, []byte, error) {
	f.gotEndpoint, f.gotWorkspaceID, f.gotPayload = endpoint, workspaceID, payload
	return f.statusCode, f.body, f.err
}

func (f *fakeForwarder) Test(_ context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("test", workspaceID, payload)
}

func (f *fakeForwarder) TestRun(_ context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("testRun", workspaceID, payload)
}

func (f *fakeForwarder) TestLibrary(_ context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("testLibrary", workspaceID, payload)
}

func (f *fakeForwarder) ExtractLibs(_ context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("extractLibs", workspaceID, payload)
}

// fakeScaler records the EnsureScaled call and returns err.
type fakeScaler struct {
	called bool
	count  int
	err    error
}

func (s *fakeScaler) EnsureScaled(_ context.Context, _ string, count int) error {
	s.called, s.count = true, count
	return s.err
}

func newService(t *testing.T, conf *config.Config, scaler pytscaler.Scaler, fwd Forwarder) *Service {
	t.Helper()
	if conf == nil {
		conf = config.New()
	}
	return NewService(conf, logger.NOP, nil, WithScaler(scaler), WithForwarder(fwd))
}

// stubForwarder is a stateless, concurrency-safe forwarder returning a canned
// response, for tests that forward from many goroutines at once.
type stubForwarder struct {
	statusCode int
	body       []byte
	err        error
}

func (f stubForwarder) forward() (int, []byte, error) { return f.statusCode, f.body, f.err }

func (f stubForwarder) Test(context.Context, string, []byte) (int, []byte, error) { return f.forward() }

func (f stubForwarder) TestRun(context.Context, string, []byte) (int, []byte, error) {
	return f.forward()
}

func (f stubForwarder) TestLibrary(context.Context, string, []byte) (int, []byte, error) {
	return f.forward()
}

func (f stubForwarder) ExtractLibs(context.Context, string, []byte) (int, []byte, error) {
	return f.forward()
}

// countingScaler counts scale-ups (atomically, for -race) and blocks the first
// EnsureScaled on block so concurrent callers coalesce onto it via singleflight.
// Like the real scaler it tracks replicas and scales only from zero, so later
// executions see the workspace as already scaled — mirroring real scaling and
// keeping EnsureScaled idempotent.
type countingScaler struct {
	replicas atomic.Int32
	block    chan struct{}
	waiting  atomic.Int32
	scales   atomic.Int32
	released atomic.Bool
}

func (s *countingScaler) EnsureScaled(_ context.Context, _ string, count int) error {
	if s.released.CompareAndSwap(false, true) {
		s.waiting.Add(1)
		<-s.block // hold the first call open so a burst can coalesce onto it
	}
	if s.replicas.Load() == 0 {
		s.scales.Add(1)
		s.replicas.Store(int32(count))
	}
	return nil
}

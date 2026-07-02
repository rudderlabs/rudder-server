package cpservice

import (
	"context"
	"errors"
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
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

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

// fakeScaler records scaler calls; getReplicas is the count GetReplicaCount reports.
type fakeScaler struct {
	getReplicas int
	getErr      error

	setCalled bool
	setCount  int
}

func (s *fakeScaler) GetReplicaCount(context.Context, string) (int, error) {
	return s.getReplicas, s.getErr
}

func (s *fakeScaler) SetReplicaCount(_ context.Context, _ string, count int) error {
	s.setCalled, s.setCount = true, count
	return nil
}

func newService(t *testing.T, conf *config.Config, scaler pytscaler.Scaler, fwd Forwarder) *Service {
	t.Helper()
	if conf == nil {
		conf = config.New()
	}
	return NewService(conf, logger.NOP, nil, WithScaler(scaler), WithForwarder(fwd))
}

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

	t.Run("rejects an empty workspaceId with InvalidArgument", func(t *testing.T) {
		svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), &fakeForwarder{})
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("scales up to the configured target when the deployment is at zero", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.UserTransformer.pytTestScaleReplicas", 3)
		scaler := &fakeScaler{getReplicas: 0}
		svc := newService(t, conf, scaler, &fakeForwarder{statusCode: 200})

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.NoError(t, err)
		require.True(t, scaler.setCalled)
		require.Equal(t, 3, scaler.setCount)
	})

	t.Run("does not scale when the deployment is already running", func(t *testing.T) {
		scaler := &fakeScaler{getReplicas: 1}
		svc := newService(t, nil, scaler, &fakeForwarder{statusCode: 200})

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.NoError(t, err)
		require.False(t, scaler.setCalled)
	})

	t.Run("returns FailedPrecondition when the pyt deployment is missing", func(t *testing.T) {
		scaler := &fakeScaler{getErr: pytscaler.ErrDeploymentNotFound}
		svc := newService(t, nil, scaler, &fakeForwarder{})

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("returns Unavailable when the forward fails", func(t *testing.T) {
		svc := newService(t, nil, pytscaler.NewNoop(logger.NOP), &fakeForwarder{err: errors.New("connection refused")})
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	// Run with -race to catch unsynchronised access to Service state under
	// concurrent Forwards. singleflight serializes same-workspace scale checks, so
	// combined with get-before-set on the (now non-zero) replicas only the first
	// execution scales: a burst scales the workspace up exactly once. The first
	// GetReplicaCount is held open so, without singleflight, every request would
	// read zero and scale — making this a real regression test for the collapse.
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

		require.Equal(t, int32(1), scaler.sets.Load(), "a concurrent burst must scale the workspace up exactly once")
	})
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

// countingScaler counts scaler calls (atomically, for -race) and blocks the first
// GetReplicaCount on block so concurrent callers coalesce onto it via singleflight.
// SetReplicaCount records the new count so later checks see the workspace as
// already scaled — mirroring real scaling and making the scale-up idempotent.
type countingScaler struct {
	replicas atomic.Int32
	block    chan struct{}
	waiting  atomic.Int32
	sets     atomic.Int32
	released atomic.Bool
}

func (s *countingScaler) GetReplicaCount(context.Context, string) (int, error) {
	if s.released.CompareAndSwap(false, true) {
		s.waiting.Add(1)
		<-s.block // hold the first check open so a burst can coalesce onto it
	}
	return int(s.replicas.Load()), nil
}

func (s *countingScaler) SetReplicaCount(_ context.Context, _ string, count int) error {
	s.sets.Add(1)
	s.replicas.Store(int32(count))
	return nil
}

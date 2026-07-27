package cpservice

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

const staticASTURL = "http://ast-static:8080"

func TestForward(t *testing.T) {
	t.Run("dispatches each op to its endpoint method and passes status/body through", func(t *testing.T) {
		cases := []struct {
			op              proto.Op
			wantEndpoint    string
			wantIsExecution bool
		}{
			{proto.Op_OP_TEST, "test", true},
			{proto.Op_OP_TEST_RUN, "testRun", true},
			{proto.Op_OP_TEST_LIBRARY, "testLibrary", false},
			{proto.Op_OP_EXTRACT_LIBS, "extractLibs", false},
		}
		for _, tc := range cases {
			t.Run(tc.op.String(), func(t *testing.T) {
				fwd := &fakeForwarder{statusCode: 201, body: []byte(`{"ok":true}`)}
				deployer := &fakeDeployer{baseURL: "http://pyt-test-abc123.ns.svc:8080"}
				svc := newService(t, nil, deployer, fwd)

				resp, err := svc.Forward(context.Background(), &proto.ForwardRequest{
					Op: tc.op, WorkspaceId: "ws-1", Payload: []byte(`{"a":1}`),
				})
				require.NoError(t, err)
				require.Equal(t, int32(201), resp.StatusCode)
				require.Equal(t, []byte(`{"ok":true}`), resp.Body)
				require.Equal(t, tc.wantEndpoint, fwd.gotEndpoint)
				require.Equal(t, "ws-1", fwd.gotWorkspaceID)
				require.Equal(t, []byte(`{"a":1}`), fwd.gotPayload)

				if tc.wantIsExecution {
					require.Equal(t, int32(1), deployer.calls.Load(), "an execution op must run on the ephemeral deployer")
					require.Equal(t, deployer.baseURL, fwd.gotBaseURL, "the forwarder must be called with the ephemeral base URL")
				} else {
					require.Zero(t, deployer.calls.Load(), "an AST op must never touch the deployer")
					require.Equal(t, staticASTURL, fwd.gotBaseURL, "the forwarder must be called with the static AST URL")
				}
			})
		}
	})

	t.Run("rejects an unknown or unspecified op with InvalidArgument", func(t *testing.T) {
		svc := newService(t, nil, &fakeDeployer{}, &fakeForwarder{})
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
			{"leading hyphen (invalid k8s label value)", "-ws-1"},
			{"trailing hyphen (invalid k8s label value)", "ws-1-"},
			{"a lone hyphen", "-"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fwd := &fakeForwarder{statusCode: 200}
				deployer := &fakeDeployer{baseURL: "http://pyt-test-abc.ns.svc:8080"}
				svc := newService(t, nil, deployer, fwd)
				_, err := svc.Forward(context.Background(),
					&proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: tc.workspaceID})
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.Empty(t, fwd.gotEndpoint, "an invalid workspaceId must never reach the forwarder")
				require.Zero(t, deployer.calls.Load(), "an invalid workspaceId must never reach the deployer")
			})
		}
	})

	t.Run("accepts well-formed workspaceIds", func(t *testing.T) {
		for _, id := range []string{"ws-1", "2CJhY0aBcDeFgHiJkLmNoPqRsTu", "a", strings.Repeat("a", 59)} {
			fwd := &fakeForwarder{statusCode: 200}
			deployer := &fakeDeployer{baseURL: "http://pyt-test-abc.ns.svc:8080"}
			svc := newService(t, nil, deployer, fwd)
			_, err := svc.Forward(context.Background(),
				&proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: id})
			require.NoErrorf(t, err, "workspaceId %q", id)
			require.Equal(t, id, fwd.gotWorkspaceID)
		}
	})

	t.Run("AST ops never touch the deployer and hit the static AST URL even with no deployer configured", func(t *testing.T) {
		deployer := &fakeDeployer{err: errors.New("must never be called")}
		fwd := &fakeForwarder{statusCode: 200}
		svc := newService(t, nil, deployer, fwd)
		for _, op := range []proto.Op{proto.Op_OP_TEST_LIBRARY, proto.Op_OP_EXTRACT_LIBS} {
			_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: op, WorkspaceId: "ws-1"})
			require.NoErrorf(t, err, "op %v", op)
			require.Zero(t, deployer.calls.Load())
			require.Equal(t, staticASTURL, fwd.gotBaseURL)
		}
	})

	t.Run("routes a config-flagged workspace's execution ops to its production pyt deployment, bypassing the deployer", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.pytTestOverrides.routeToProduction", map[string]any{"WS-Prod-Routed": true})
		deployer := &fakeDeployer{err: errors.New("must never be called")}
		fwd := &fakeForwarder{statusCode: 200}
		svc := newService(t, conf, deployer, fwd)

		for _, op := range []proto.Op{proto.Op_OP_TEST, proto.Op_OP_TEST_RUN} {
			// Flag match is case-insensitive, like the deployment name convention.
			_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: op, WorkspaceId: "ws-prod-routed"})
			require.NoErrorf(t, err, "op %v", op)
			require.Zero(t, deployer.calls.Load(), "a prod-routed workspace must never touch the deployer")
			require.Equal(t, "http://pyt-ws-prod-routed:8080", fwd.gotBaseURL,
				"the forward must target the workspace's production pyt deployment")
		}

		// AST ops stay on the static AST deployment even for flagged workspaces.
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST_LIBRARY, WorkspaceId: "ws-prod-routed"})
		require.NoError(t, err)
		require.Equal(t, staticASTURL, fwd.gotBaseURL)
	})

	t.Run("an unflagged workspace still runs on the ephemeral deployer", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.pytTestOverrides.routeToProduction", map[string]any{"ws-other": true})
		deployer := &fakeDeployer{baseURL: "http://pyt-test-abc.ns.svc:8080"}
		fwd := &fakeForwarder{statusCode: 200}
		svc := newService(t, conf, deployer, fwd)

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.NoError(t, err)
		require.Equal(t, int32(1), deployer.calls.Load())
		require.Equal(t, deployer.baseURL, fwd.gotBaseURL)
	})

	t.Run("the \"*\" routeToProduction entry flags every workspace at once", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.pytTestOverrides.routeToProduction", map[string]any{"*": true})
		deployer := &fakeDeployer{err: errors.New("must never be called")}
		fwd := &fakeForwarder{statusCode: 200}
		svc := newService(t, conf, deployer, fwd)

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "any-ws"})
		require.NoError(t, err)
		require.Zero(t, deployer.calls.Load())
		require.Equal(t, "http://pyt-any-ws:8080", fwd.gotBaseURL)
	})

	t.Run("a workspace's own false entry wins over the \"*\" entry", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.pytTestOverrides.routeToProduction", map[string]any{"*": true, "ws-exempt": false})
		deployer := &fakeDeployer{baseURL: "http://pyt-test-abc.ns.svc:8080"}
		fwd := &fakeForwarder{statusCode: 200}
		svc := newService(t, conf, deployer, fwd)

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-exempt"})
		require.NoError(t, err)
		require.Equal(t, int32(1), deployer.calls.Load(), "the exempted workspace must stay on the ephemeral deployer")
		require.Equal(t, deployer.baseURL, fwd.gotBaseURL)
	})

	t.Run("returns Unavailable when the ephemeral deployer fails", func(t *testing.T) {
		deployer := &fakeDeployer{err: errors.New("creating ephemeral pyt deployment: quota exceeded")}
		fwd := &fakeForwarder{}
		svc := newService(t, nil, deployer, fwd)

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Empty(t, fwd.gotEndpoint, "a failed deploy must not reach the forwarder")
	})

	t.Run("k8s client unavailable: execution ops fail with FailedPrecondition, AST ops still work", func(t *testing.T) {
		conf := config.New()
		conf.Set("Processor.pytTestStaticASTURL", staticASTURL)
		fwd := &fakeForwarder{statusCode: 200}
		// No WithDeployer override: NewService tries to build a real k8s-backed
		// deployer, which fails outside a cluster/without a kubeconfig, and
		// falls back to unavailableDeployer.
		svc := NewService(conf, logger.NOP, stats.NOP, WithForwarder(fwd))

		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.FailedPrecondition, status.Code(err),
			"a missing deployer is misconfiguration, not a transient outage")

		_, err = svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST_LIBRARY, WorkspaceId: "ws-1"})
		require.NoError(t, err, "AST ops must keep working without a k8s-backed deployer")
	})

	t.Run("returns FailedPrecondition when the static AST URL is not configured", func(t *testing.T) {
		deployer := &fakeDeployer{err: errors.New("must never be called")}
		// Real user_transformer forwarder + no pytTestStaticASTURL: the AST op
		// forwards to an empty baseURL, which the forwarder rejects with
		// ErrEmptyForwardBaseURL.
		svc := NewService(config.New(), logger.NOP, stats.NOP, WithDeployer(deployer))
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST_LIBRARY, WorkspaceId: "ws-1"})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("returns Unavailable when the forward fails", func(t *testing.T) {
		deployer := &fakeDeployer{baseURL: "http://pyt-test-abc.ns.svc:8080"}
		svc := newService(t, nil, deployer, &fakeForwarder{err: errors.New("connection refused")})
		_, err := svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("emits total and request timers tagged by op, route and workspace", func(t *testing.T) {
		cases := []struct {
			name      string
			op        proto.Op
			prodRoute bool
			fwdErr    error
			wantRoute string
		}{
			{"execution op on an ephemeral deployment", proto.Op_OP_TEST, false, nil, "ephemeral"},
			{"execution op routed to production", proto.Op_OP_TEST_RUN, true, nil, "production"},
			{"AST op on the static deployment", proto.Op_OP_TEST_LIBRARY, false, nil, "static"},
			{"emitted on a failed forward too", proto.Op_OP_TEST, false, errors.New("connection refused"), "ephemeral"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				statsStore, err := memstats.New()
				require.NoError(t, err)
				conf := config.New()
				conf.Set("Processor.pytTestStaticASTURL", staticASTURL)
				if tc.prodRoute {
					conf.Set("Processor.pytTestOverrides.routeToProduction", map[string]any{"ws-1": true})
				}
				// The real forwarder returns no status code alongside an error.
				fwdStatus := 200
				if tc.fwdErr != nil {
					fwdStatus = 0
				}
				svc := NewService(conf, logger.NOP, statsStore,
					WithDeployer(&fakeDeployer{baseURL: "http://pyt-test-abc.ns.svc:8080"}),
					WithForwarder(&fakeForwarder{statusCode: fwdStatus, err: tc.fwdErr}))

				_, err = svc.Forward(context.Background(), &proto.ForwardRequest{Op: tc.op, WorkspaceId: "ws-1"})
				if tc.fwdErr != nil {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				wantTags := stats.Tags{
					"op":          tc.op.String(),
					"route":       tc.wantRoute,
					"workspaceId": "ws-1",
					"success":     strconv.FormatBool(tc.fwdErr == nil),
				}
				metrics := statsStore.GetByName("processor_pyt_forward_rpc_time")
				require.Len(t, metrics, 1)
				require.Equal(t, wantTags, metrics[0].Tags)

				wantReqTags := stats.Tags{
					"op":          tc.op.String(),
					"route":       tc.wantRoute,
					"workspaceId": "ws-1",
					"success":     strconv.FormatBool(tc.fwdErr == nil),
				}
				execMetrics := statsStore.GetByName("processor_pyt_request_handle_time")
				require.Len(t, execMetrics, 1, "the forward ran, so the request timer must have a sample too")
				require.Equal(t, wantReqTags, execMetrics[0].Tags)
			})
		}
	})

	t.Run("no request timer when the deployer fails before forwarding", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)
		conf := config.New()
		conf.Set("Processor.pytTestStaticASTURL", staticASTURL)
		svc := NewService(conf, logger.NOP, statsStore,
			WithDeployer(&fakeDeployer{err: errors.New("create failed")}),
			WithForwarder(&fakeForwarder{statusCode: 200}))

		_, err = svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "ws-1"})
		require.Error(t, err)

		require.Empty(t, statsStore.GetByName("processor_pyt_request_handle_time"),
			"no request was made, so no request sample must be recorded")
		rpcMetrics := statsStore.GetByName("processor_pyt_forward_rpc_time")
		require.Len(t, rpcMetrics, 1, "the total timer still records the failed attempt")
		require.Equal(t, "false", rpcMetrics[0].Tags["success"],
			"a deployer failure must be distinguishable from a slow success")
	})

	t.Run("no timers when the request is rejected before forwarding", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)
		conf := config.New()
		conf.Set("Processor.pytTestStaticASTURL", staticASTURL)
		svc := NewService(conf, logger.NOP, statsStore,
			WithDeployer(&fakeDeployer{}), WithForwarder(&fakeForwarder{}))

		_, err = svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_UNSPECIFIED, WorkspaceId: "ws-1"})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		_, err = svc.Forward(context.Background(), &proto.ForwardRequest{Op: proto.Op_OP_TEST, WorkspaceId: "a@evil.com#"})
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		require.Empty(t, statsStore.GetByName("processor_pyt_forward_rpc_time"))
	})
}

// fakeForwarder records which endpoint method was called (and the baseURL it
// was called with) and returns a canned response, so tests can assert Forward
// dispatches each op to the right method and target.
type fakeForwarder struct {
	gotEndpoint    string
	gotBaseURL     string
	gotWorkspaceID string
	gotPayload     []byte

	statusCode int
	body       []byte
	err        error
}

func (f *fakeForwarder) record(endpoint, baseURL, workspaceID string, payload []byte) (int, []byte, error) {
	f.gotEndpoint, f.gotBaseURL, f.gotWorkspaceID, f.gotPayload = endpoint, baseURL, workspaceID, payload
	return f.statusCode, f.body, f.err
}

func (f *fakeForwarder) Test(_ context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("test", baseURL, workspaceID, payload)
}

func (f *fakeForwarder) TestRun(_ context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("testRun", baseURL, workspaceID, payload)
}

func (f *fakeForwarder) TestLibrary(_ context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("testLibrary", baseURL, workspaceID, payload)
}

func (f *fakeForwarder) ExtractLibs(_ context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error) {
	return f.record("extractLibs", baseURL, workspaceID, payload)
}

// fakeDeployer stubs pytdeployer.Deployer: on success it invokes forward
// against baseURL directly (skipping any real k8s work); when err is set it
// fails before forward is ever called, simulating a create/readiness failure.
type fakeDeployer struct {
	baseURL string
	err     error
	calls   atomic.Int32
}

func (d *fakeDeployer) RunOnEphemeral(
	ctx context.Context, _ string, forward func(ctx context.Context, baseURL string) (int, []byte, error),
) (int, []byte, error) {
	d.calls.Add(1)
	if d.err != nil {
		return 0, nil, d.err
	}
	return forward(ctx, d.baseURL)
}

func newService(t *testing.T, conf *config.Config, deployer *fakeDeployer, fwd Forwarder) *Service {
	t.Helper()
	if conf == nil {
		conf = config.New()
	}
	conf.Set("Processor.pytTestStaticASTURL", staticASTURL)
	return NewService(conf, logger.NOP, stats.NOP, WithDeployer(deployer), WithForwarder(fwd))
}

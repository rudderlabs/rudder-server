package cpservice

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/internal/pytscaler"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

func TestShouldConnect(t *testing.T) {
	cases := []struct {
		name       string
		enabled    bool
		instanceID string
		want       bool
	}{
		{name: "enabled on node-0", enabled: true, instanceID: "rs-proc-0", want: true},
		{name: "disabled on node-0", enabled: false, instanceID: "rs-proc-0", want: false},
		{name: "enabled on node-1", enabled: true, instanceID: "rs-proc-1", want: false},
		{name: "enabled with unset instance id", enabled: true, instanceID: "", want: false},
		{name: "enabled on HA node-0", enabled: true, instanceID: "rs-gw-ha-0-abc-def", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("INSTANCE_ID", tc.instanceID)
			conf := config.New()
			conf.Set("Processor.cpRouterConnection.enabled", tc.enabled)
			require.Equal(t, tc.want, ShouldConnect(conf))
		})
	}
}

func TestServiceRegisters(t *testing.T) {
	srv := grpc.NewServer()
	proto.RegisterProcessorServiceServer(srv, NewService(config.New(), logger.NOP, stats.NOP,
		WithScaler(pytscaler.NewNoop(logger.NOP))))

	info := srv.GetServiceInfo()
	_, ok := info["proto.ProcessorService"]
	require.True(t, ok, "ProcessorService should be registered on the gRPC server")
}

// TestForwardOverConnection drives the full path: the processor (dataplane) dials
// a mock cp-router, registers ProcessorService over the dataplane-initiated
// yamux/gRPC connection, and the mock — acting as the control-plane gRPC client —
// calls Forward. The service scales (no-op here) and forwards to an httptest pyt
// stub, returning its status and body unchanged.
func TestForwardOverConnection(t *testing.T) {
	// A dedicated dataplane deployment so deployment.GetConnectionToken resolves.
	t.Setenv("DEPLOYMENT_TYPE", "DEDICATED")
	t.Setenv("WORKSPACE_TOKEN", "test-token")

	var gotPath string
	var gotBody []byte
	pyt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(pyt.Close)

	cpRouter := newMockCPRouter(t)

	conf := config.New()
	conf.Set("CP_ROUTER_USE_TLS", false) // mock cp-router is a plain TCP listener
	conf.Set("Processor.cpRouterConnection.retryInterval", "10ms")
	conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
	conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", pyt.URL)

	svc := NewService(conf, logger.NOP, stats.NOP, WithScaler(pytscaler.NewNoop(logger.NOP)))
	c, err := NewConnector(conf, logger.NOP, nil, svc)
	require.NoError(t, err)
	require.Equal(t, ServiceName, c.cm.AuthInfo.Service)
	t.Cleanup(func() { c.cm.Apply(cpRouter.addr, false) }) // stop the reconnect loop

	// The dataplane initiates the connection to cp-router.
	c.cm.Apply(cpRouter.addr, true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := cpRouter.forward(ctx, &proto.ForwardRequest{
		Op:          proto.Op_OP_TEST_RUN,
		WorkspaceId: "ws-1",
		Payload:     []byte(`{"code":"x"}`),
	})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusCreated), resp.StatusCode)
	require.JSONEq(t, `{"ok":true}`, string(resp.Body))
	require.Equal(t, "/testRun", gotPath)
	require.JSONEq(t, `{"code":"x"}`, string(gotBody))
}

// mockCPRouter is a stand-in for cp-router: it accepts the dataplane-initiated
// TCP connection and, mirroring cp-router's connection model, wraps it as the
// yamux client and runs a gRPC client over it to call the processor's service.
type mockCPRouter struct {
	addr     string
	clientCh chan proto.ProcessorServiceClient
	errCh    chan error

	mu       sync.Mutex
	grpcConn *grpc.ClientConn
}

func newMockCPRouter(t *testing.T) *mockCPRouter {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	m := &mockCPRouter{
		addr:     ln.Addr().String(),
		clientCh: make(chan proto.ProcessorServiceClient, 1),
		errCh:    make(chan error, 1),
	}
	go m.accept(ln)

	t.Cleanup(func() {
		_ = ln.Close()
		m.mu.Lock()
		if m.grpcConn != nil {
			_ = m.grpcConn.Close()
		}
		m.mu.Unlock()
	})
	return m
}

func (m *mockCPRouter) accept(ln net.Listener) {
	conn, err := ln.Accept()
	if err != nil {
		m.errCh <- err
		return
	}
	// cp-router is the yamux client; the dataplane is the yamux server.
	session, err := yamux.Client(conn, yamux.DefaultConfig())
	if err != nil {
		m.errCh <- err
		return
	}
	grpcConn, err := grpc.NewClient(
		"passthrough:///cprouter",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return session.Open()
		}),
	)
	if err != nil {
		m.errCh <- err
		return
	}
	m.mu.Lock()
	m.grpcConn = grpcConn
	m.mu.Unlock()
	m.clientCh <- proto.NewProcessorServiceClient(grpcConn)
}

// forward waits for the dataplane to connect, then calls Forward over the
// dataplane-initiated channel.
func (m *mockCPRouter) forward(ctx context.Context, req *proto.ForwardRequest) (*proto.ForwardResponse, error) {
	select {
	case client := <-m.clientCh:
		return client.Forward(ctx, req)
	case err := <-m.errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

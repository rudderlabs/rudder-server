package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/rudderlabs/rudder-server/utils/types/workspace"
)

type mockModeProvider struct {
	modeCh      chan servermode.ChangeEvent
	workspaceCh chan workspace.ChangeEvent
}

func (m *mockModeProvider) ServerMode(context.Context) <-chan servermode.ChangeEvent {
	return m.modeCh
}

func (m *mockModeProvider) WorkspaceIDs(_ context.Context) <-chan workspace.ChangeEvent {
	return m.workspaceCh
}

func (m *mockModeProvider) SendMode(newMode servermode.ChangeEvent) {
	m.modeCh <- newMode
}

func (m *mockModeProvider) SendWorkspaceIDs(ws workspace.ChangeEvent) {
	m.workspaceCh <- ws
}

type mockLifecycle struct {
	status    string
	callOrder uint64
	callCount *uint64
}

func (m *mockLifecycle) Start() {
	m.callOrder = atomic.AddUint64(m.callCount, 1)
	m.status = "start"
}

func (m *mockLifecycle) Stop() {
	m.callOrder = atomic.AddUint64(m.callCount, 1)
	m.status = "stop"
}

type configMock struct {
	workspaces          string
	stopPollingCalled   bool
	waitForConfigCalled bool
}

func (s *configMock) StartWithIDs(_ context.Context, workspaces string) {
	s.workspaces = workspaces
}

func (s *configMock) Stop() {
	s.stopPollingCalled = true
}

func (s *configMock) WaitForConfig(_ context.Context) error {
	s.waitForConfigCalled = true
	return nil
}

func Init() {
	config.Load()
	stats.Setup()
	logger.Init()
}

func TestDynamicCluster(t *testing.T) {
	Init()

	provider := &mockModeProvider{
		modeCh:      make(chan servermode.ChangeEvent),
		workspaceCh: make(chan workspace.ChangeEvent),
	}

	callCount := uint64(0)

	gatewayDB := &mockLifecycle{status: "", callCount: &callCount}
	routerDB := &mockLifecycle{status: "", callCount: &callCount}
	batchRouterDB := &mockLifecycle{status: "", callCount: &callCount}
	errorDB := &mockLifecycle{status: "", callCount: &callCount}

	processor := &mockLifecycle{status: "", callCount: &callCount}
	router := &mockLifecycle{status: "", callCount: &callCount}

	mtStat := &multitenant.Stats{
		RouterDBs: map[string]jobsdb.MultiTenantJobsDB{},
	}

	backendConfig := configMock{}
	dc := cluster.Dynamic{
		Provider: provider,

		GatewayDB:     gatewayDB,
		RouterDB:      routerDB,
		BatchRouterDB: batchRouterDB,
		ErrorDB:       errorDB,

		Processor: processor,
		Router:    router,

		MultiTenantStat: mtStat,
		BackendConfig:   &backendConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wait := make(chan struct{})
	go func() {
		_ = dc.Run(ctx)
		close(wait)
	}()

	t.Run("DEGRADED -> NORMAL", func(t *testing.T) {
		chACK := make(chan struct{})
		provider.SendMode(servermode.NewChangeEvent(servermode.NormalMode, func(_ context.Context) error {
			close(chACK)
			return nil
		}))

		require.Eventually(t, func() bool {
			<-chACK
			return true
		}, time.Second, time.Millisecond)

		require.Equal(t, "start", gatewayDB.status)
		require.Equal(t, "start", routerDB.status)
		require.Equal(t, "start", batchRouterDB.status)
		require.Equal(t, "start", errorDB.status)

		require.Equal(t, "start", processor.status)
		require.Equal(t, "start", router.status)

		t.Log("dbs should be started before processor")
		require.True(t, gatewayDB.callOrder < processor.callOrder)
		require.True(t, routerDB.callOrder < processor.callOrder)
		require.True(t, batchRouterDB.callOrder < processor.callOrder)
		require.True(t, errorDB.callOrder < processor.callOrder)

		t.Log("dbs should be started before router")
		require.True(t, gatewayDB.callOrder < router.callOrder)
		require.True(t, routerDB.callOrder < router.callOrder)
		require.True(t, batchRouterDB.callOrder < router.callOrder)
		require.True(t, errorDB.callOrder < router.callOrder)
	})

	t.Run("NORMAL -> DEGRADED", func(t *testing.T) {
		chACK := make(chan struct{})
		provider.SendMode(servermode.NewChangeEvent(servermode.DegradedMode, func(_ context.Context) error {
			close(chACK)
			return nil
		}))

		require.Eventually(t, func() bool {
			<-chACK
			return true
		}, time.Second, time.Millisecond)

		require.Equal(t, "stop", gatewayDB.status)
		require.Equal(t, "stop", routerDB.status)
		require.Equal(t, "stop", batchRouterDB.status)
		require.Equal(t, "stop", errorDB.status)

		require.Equal(t, "stop", processor.status)
		require.Equal(t, "stop", router.status)

		t.Log("dbs should be stopped after processor")
		require.True(t, gatewayDB.callOrder > processor.callOrder)
		require.True(t, routerDB.callOrder > processor.callOrder)
		require.True(t, batchRouterDB.callOrder > processor.callOrder)
		require.True(t, errorDB.callOrder > processor.callOrder)

		t.Log("dbs should be stopped after router")
		require.True(t, gatewayDB.callOrder > router.callOrder)
		require.True(t, routerDB.callOrder > router.callOrder)
		require.True(t, batchRouterDB.callOrder > router.callOrder)
		require.True(t, errorDB.callOrder > router.callOrder)
	})

	t.Run("Update workspaceIDs", func(t *testing.T) {
		chACK := make(chan struct{})
		provider.SendWorkspaceIDs(
			workspace.NewWorkspacesRequest([]string{"a", "b", "c"},
				func(_ context.Context) error {
					close(chACK)
					return nil
				},
				func(ctx context.Context, err error) error {
					t.Fatalf("unexpected error: %v", err)
					return nil
				}),
		)

		select {
		case <-chACK:
		case <-time.After(time.Second):
			t.Fatal("Did not get acknowledgement within 1 second")
		}

		require.True(t, backendConfig.stopPollingCalled)
		require.True(t, backendConfig.waitForConfigCalled)
		require.Equal(t, "a,b,c", backendConfig.workspaces)
	})

	t.Run("Shutdown from Normal ", func(t *testing.T) {
		cancel()
		<-wait

		require.Equal(t, "stop", gatewayDB.status)
		require.Equal(t, "stop", routerDB.status)
		require.Equal(t, "stop", batchRouterDB.status)
		require.Equal(t, "stop", errorDB.status)

		require.Equal(t, "stop", processor.status)
		require.Equal(t, "stop", router.status)

		t.Log("dbs should be stopped after processor")
		require.True(t, gatewayDB.callOrder > processor.callOrder)
		require.True(t, routerDB.callOrder > processor.callOrder)
		require.True(t, batchRouterDB.callOrder > processor.callOrder)
		require.True(t, errorDB.callOrder > processor.callOrder)

		t.Log("dbs should be stopped after router")
		require.True(t, gatewayDB.callOrder > router.callOrder)
		require.True(t, routerDB.callOrder > router.callOrder)
		require.True(t, batchRouterDB.callOrder > router.callOrder)
		require.True(t, errorDB.callOrder > router.callOrder)
	})
}

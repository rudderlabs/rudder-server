package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/multitenant"
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

func (m *mockModeProvider) sendMode(newMode servermode.ChangeEvent) {
	m.modeCh <- newMode
}

func (m *mockModeProvider) sendWorkspaceIDs(ws workspace.ChangeEvent) {
	m.workspaceCh <- ws
}

type mockLifecycle struct {
	status    string
	callOrder uint64
	callCount *uint64
}

func (m *mockLifecycle) Start() error {
	m.callOrder = atomic.AddUint64(m.callCount, 1)
	m.status = "start"
	return nil
}

func (m *mockLifecycle) Stop() {
	m.callOrder = atomic.AddUint64(m.callCount, 1)
	m.status = "stop"
}

func Init() {
	config.Reset()
	logger.Reset()
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

	ctrl := gomock.NewController(t)
	backendConfig := NewMockconfigLifecycle(ctrl)
	dc := cluster.Dynamic{
		Provider: provider,

		GatewayDB:     gatewayDB,
		RouterDB:      routerDB,
		BatchRouterDB: batchRouterDB,
		ErrorDB:       errorDB,

		Processor: processor,
		Router:    router,

		MultiTenantStat: mtStat,
		BackendConfig:   backendConfig,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wait := make(chan struct{})
	go func() {
		_ = dc.Run(ctx)
		close(wait)
	}()

	t.Run("DEGRADED -> NORMAL", func(t *testing.T) {
		chACK := make(chan struct{})
		provider.sendMode(servermode.NewChangeEvent(servermode.NormalMode, func(_ context.Context) error {
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
		provider.sendMode(servermode.NewChangeEvent(servermode.DegradedMode, func(_ context.Context) error {
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
		backendConfig.EXPECT().Stop().Times(1)
		backendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
		backendConfig.EXPECT().StartWithIDs(gomock.Any(), "a,b,c").Times(1)

		provider.sendWorkspaceIDs(
			workspace.NewWorkspacesRequest([]string{"a", "b", "c"},
				func(_ context.Context, err error) error {
					close(chACK)
					require.NoError(t, err)
					return nil
				},
			),
		)

		select {
		case <-chACK:
		case <-time.After(time.Second):
			t.Fatal("Did not get acknowledgement within 1 second")
		}
	})

	t.Run("Empty workspaces triggers a reload", func(t *testing.T) {
		chACK := make(chan struct{})
		backendConfig.EXPECT().Stop().Times(1)
		backendConfig.EXPECT().StartWithIDs(gomock.Any(), gomock.Any()).Times(1)
		backendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)

		provider.sendWorkspaceIDs(
			workspace.NewWorkspacesRequest([]string{},
				func(ctx context.Context, err error) error {
					require.NoError(t, err)
					close(chACK)
					return nil
				}),
		)

		select {
		case <-chACK:
		case <-time.After(time.Second):
			t.Fatal("Did not get acknowledgement error within 1 second")
		}
	})

	t.Run("Shutdown from Normal", func(t *testing.T) {
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

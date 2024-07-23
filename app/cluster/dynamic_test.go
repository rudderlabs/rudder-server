package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/require"

	mockjobsforwarder "github.com/rudderlabs/rudder-server/mocks/jobs-forwarder"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

type mockModeProvider struct {
	modeCh chan servermode.ChangeEvent
}

func (m *mockModeProvider) ServerMode(context.Context) <-chan servermode.ChangeEvent {
	return m.modeCh
}

func (m *mockModeProvider) sendMode(newMode servermode.ChangeEvent) {
	m.modeCh <- newMode
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
		modeCh: make(chan servermode.ChangeEvent),
	}

	callCount := uint64(0)

	gatewayDB := &mockLifecycle{status: "", callCount: &callCount}
	routerDB := &mockLifecycle{status: "", callCount: &callCount}
	batchRouterDB := &mockLifecycle{status: "", callCount: &callCount}
	errorDB := &mockLifecycle{status: "", callCount: &callCount}
	schemaForwarder := mockjobsforwarder.NewMockForwarder(gomock.NewController(t))
	eschDB := &mockLifecycle{status: "", callCount: &callCount}
	archDB := &mockLifecycle{status: "", callCount: &callCount}
	archiver := &mockLifecycle{status: "", callCount: &callCount}

	processor := &mockLifecycle{status: "", callCount: &callCount}
	router := &mockLifecycle{status: "", callCount: &callCount}
	dc := cluster.Dynamic{
		Provider: provider,

		GatewayDB:     gatewayDB,
		RouterDB:      routerDB,
		BatchRouterDB: batchRouterDB,
		ErrorDB:       errorDB,
		EventSchemaDB: eschDB,
		ArchivalDB:    archDB,

		Processor:       processor,
		Router:          router,
		SchemaForwarder: schemaForwarder,
		Archiver:        archiver,
	}

	ctx, cancel := context.WithCancel(context.Background())

	schemaForwarder.EXPECT().Start().Return(nil).AnyTimes()
	schemaForwarder.EXPECT().Stop().AnyTimes()
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

	t.Run("server should start in NORMAL mode by default when there is no instruction by scheduler", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return gatewayDB.status == "start"
		}, time.Second, time.Millisecond)

		require.Eventually(t, func() bool {
			return routerDB.status == "start"
		}, time.Second, time.Millisecond)

		require.Eventually(t, func() bool {
			return errorDB.status == "start"
		}, time.Second, time.Millisecond)

		require.Eventually(t, func() bool {
			return batchRouterDB.status == "start"
		}, time.Second, time.Millisecond)

		require.Eventually(t, func() bool {
			return processor.status == "start"
		}, time.Second, time.Millisecond)

		require.Eventually(t, func() bool {
			return router.status == "start"
		}, time.Second, time.Millisecond)
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

package state_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/stretchr/testify/require"
)

func TestStatic_ServerMode(t *testing.T) {
	s := state.NewStaticProvider(servermode.DegradedMode)

	ctx, cancel := context.WithCancel(context.Background())

	ch := s.ServerMode(ctx)
	req, ok := <-ch
	require.True(t, ok)
	require.NoError(t, req.Err())
	require.Equal(t, servermode.DegradedMode, req.Mode())

	require.NoError(t, req.Ack(ctx))

	t.Log("cancel context should close channel")
	cancel()
	_, ok = <-ch
	require.False(t, ok)
}

func TestStatic_WorkspaceIDs(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	backendconfig.DefaultBackendConfig = mockBackendConfig
	mockBackendConfig.EXPECT().AccessToken().Times(2)
	s := state.NewStaticProvider(servermode.DegradedMode)

	ctx, cancel := context.WithCancel(context.Background())

	ch := s.WorkspaceIDs(ctx)
	chEvent := <-ch
	wIds := chEvent.WorkspaceIDs()
	require.Equal(t, []string{mockBackendConfig.AccessToken()}, wIds)

	t.Log("cancel context should close channel")
	cancel()
	_, ok := <-ch
	require.False(t, ok)
}

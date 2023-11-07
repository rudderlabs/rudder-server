package state_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
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

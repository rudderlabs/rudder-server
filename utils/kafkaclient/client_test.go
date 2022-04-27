package kafkaclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

func TestClient_Ping(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaContainer, err := destination.SetupKafka(pool, &testCleanup{t}, t)
	require.NoError(t, err)

	kafkaHost := fmt.Sprintf("localhost:%s", kafkaContainer.Port)
	c, err := New("tcp", kafkaHost)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, c.Ping(ctx))

	require.NoError(t, kafkaContainer.Destroy())
	err = c.Ping(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
}

type testCleanup struct{ *testing.T }

func (t *testCleanup) Defer(fn func() error) {
	t.Cleanup(func() {
		if err := fn(); err != nil {
			t.Log(err)
		}
	})
}

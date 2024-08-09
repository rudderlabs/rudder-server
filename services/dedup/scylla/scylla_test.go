package scylla

import (
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/scylla"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

func Test_Scylla(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	keySpace := rand.String(5)
	scyllaContainer, err := scylla.Setup(pool, t, scylla.WithKeyspace(keySpace))
	require.NoError(t, err)
	require.NotNil(t, scyllaContainer)
	conf := config.New()
	conf.Set("Scylla.Hosts", scyllaContainer.URL)
	conf.Set("Scylla.Keyspace", keySpace)
	scylla, err := New(conf, stats.NOP)
	require.NoError(t, err)
	require.NotNil(t, scylla)
	defer scylla.Close()
	t.Run("Same messageID should not be deduped for different workspace", func(t *testing.T) {
		key1 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test1"}
		key2 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test2"}
		found, _, err := scylla.Get(key1)
		require.Nil(t, err)
		require.True(t, found)
		err = scylla.Commit([]string{key1.Key})
		require.NoError(t, err)
		found, _, err = scylla.Get(key2)
		require.Nil(t, err)
		require.True(t, found)
		err = scylla.Commit([]string{key2.Key})
		require.NoError(t, err)
	})
	t.Run("Same messageID should be deduped for same workspace", func(t *testing.T) {
		key1 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test"}
		key2 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test"}
		found, _, err := scylla.Get(key1)
		require.Nil(t, err)
		require.True(t, found)
		err = scylla.Commit([]string{key1.Key})
		require.NoError(t, err)
		found, _, err = scylla.Get(key2)
		require.Nil(t, err)
		require.False(t, found)
	})
	t.Run("Same messageID should be deduped for same workspace from cache", func(t *testing.T) {
		key1 := types.KeyValue{Key: "b", Value: 1, WorkspaceId: "test"}
		key2 := types.KeyValue{Key: "b", Value: 1, WorkspaceId: "test"}
		found, _, err := scylla.Get(key1)
		require.Nil(t, err)
		require.True(t, found)
		found, _, err = scylla.Get(key2)
		require.Nil(t, err)
		require.False(t, found)
	})
}

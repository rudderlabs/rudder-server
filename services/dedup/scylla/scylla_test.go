package scylla

import (
	"strings"
	"testing"

	"github.com/google/uuid"
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
	table := rand.String(5)
	scyllaContainer, err := scylla.Setup(pool, t, scylla.WithKeyspace(keySpace))
	require.NoError(t, err)
	require.NotNil(t, scyllaContainer)
	conf := config.New()
	conf.Set("Scylla.Hosts", scyllaContainer.URL)
	conf.Set("Scylla.Keyspace", keySpace)
	conf.Set("Scylla.TableName", table)
	scylla, err := New(conf, stats.NOP)
	require.NoError(t, err)
	require.NotNil(t, scylla)
	defer scylla.Close()
	t.Run("Same messageID should not be deduped for different workspace", func(t *testing.T) {
		key1 := types.KeyValue{Key: "a", WorkspaceID: "test1"}
		key2 := types.KeyValue{Key: "a", WorkspaceID: "test2"}
		found, err := scylla.Get(key1)
		require.Nil(t, err)
		require.True(t, found)
		err = scylla.Commit([]string{key1.Key})
		require.NoError(t, err)
		found, err = scylla.Get(key2)
		require.Nil(t, err)
		require.True(t, found)
		err = scylla.Commit([]string{key2.Key})
		require.NoError(t, err)
	})
	t.Run("Same messageID should be deduped for same workspace", func(t *testing.T) {
		key1 := types.KeyValue{Key: "a", WorkspaceID: "test"}
		key2 := types.KeyValue{Key: "a", WorkspaceID: "test"}
		found, err := scylla.Get(key1)
		require.NoError(t, err)
		require.True(t, found)
		err = scylla.Commit([]string{key1.Key})
		require.NoError(t, err)
		found, err = scylla.Get(key2)
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("Same messageID should be deduped for same workspace from cache", func(t *testing.T) {
		key1 := types.KeyValue{Key: "b", WorkspaceID: "test"}
		key2 := types.KeyValue{Key: "b", WorkspaceID: "test"}
		found, err := scylla.Get(key1)
		require.NoError(t, err)
		require.True(t, found)
		found, err = scylla.Get(key2)
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("Same messageID should be deduped for same workspace from cache for Batch call", func(t *testing.T) {
		keys := []types.KeyValue{
			{Key: "c", WorkspaceID: "test", JobID: 1},
			{Key: "c", WorkspaceID: "test", JobID: 2},
			{Key: "d", WorkspaceID: "test", JobID: 3},
		}
		expected := map[types.KeyValue]bool{
			keys[0]: true,
			keys[1]: false,
			keys[2]: true,
		}
		found, err := scylla.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, found, 3)
		for _, key := range keys {
			require.Equal(t, expected[key], found[key])
		}
		err = scylla.Commit([]string{"c", "d"})
		require.NoError(t, err)
	})
	t.Run("Different messageID should not be deduped for same workspace", func(t *testing.T) {
		keys := []types.KeyValue{
			{Key: "e", WorkspaceID: "test", JobID: 1},
			{Key: "f", WorkspaceID: "test", JobID: 2},
			{Key: "g", WorkspaceID: "test", JobID: 3},
		}
		found, err := scylla.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, found, 3)
		for _, key := range keys {
			require.True(t, found[key])
		}
	})
	t.Run("Same messageID should not be deduped for different workspace", func(t *testing.T) {
		keys := []types.KeyValue{
			{Key: "h", WorkspaceID: "test1", JobID: 1},
		}
		found, err := scylla.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, found, 1)
		for _, key := range keys {
			require.True(t, found[key])
		}
		err = scylla.Commit([]string{"h"})
		require.NoError(t, err)
		keys = []types.KeyValue{
			{Key: "h", WorkspaceID: "test2", JobID: 1},
		}
		found, err = scylla.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, found, 1)
		for _, key := range keys {
			require.True(t, found[key])
		}
		err = scylla.Commit([]string{"h"})
		require.NoError(t, err)
	})
}

// Benchmark_ScyllaGet/Get-12         	      20	  71365577 ns/op
// Benchmark_ScyllaGet/GetBatch-12    	     266	   4006687 ns/op
func Benchmark_ScyllaGet(b *testing.B) {
	pool, err := dockertest.NewPool("")
	require.NoError(b, err)
	keySpace := strings.ToUpper(rand.String(5))
	table := rand.String(5)
	resource, err := scylla.Setup(pool, b, scylla.WithKeyspace(keySpace))
	require.NoError(b, err)
	conf := config.New()
	conf.Set("Scylla.Hosts", resource.URL)
	conf.Set("Scylla.Keyspace", keySpace)
	conf.Set("Scylla.TableName", table)
	scylla, err := New(conf, stats.NOP)
	require.NoError(b, err)
	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var keys []string
			for j := 0; j < 100; j++ {
				key := uuid.New().String()
				keys = append(keys, key)
				_, err = scylla.Get(types.KeyValue{Key: key, WorkspaceID: "test", JobID: int64(i*100 + j)})
				require.NoError(b, err)
			}
			err = scylla.Commit(keys)
			require.NoError(b, err)
		}
	})
	b.Run("GetBatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var keys []types.KeyValue
			var commitKeys []string
			for j := 0; j < 100; j++ {
				key := uuid.New().String()
				keys = append(keys, types.KeyValue{Key: key, WorkspaceID: "test", JobID: int64(i*100 + j)})
				commitKeys = append(commitKeys, key)
			}
			_, err = scylla.GetBatch(keys)
			require.NoError(b, err)
			err = scylla.Commit(commitKeys)
			require.NoError(b, err)
		}
	})
}

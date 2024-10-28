package badger

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Test_Badger(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := t.TempDir()
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)
	badger := NewBadgerDB(conf, stats.NOP, DefaultPath())
	require.NotNil(t, badger)
	defer badger.Close()
	t.Run("Same messageID should be deduped from badger", func(t *testing.T) {
		key1 := types.KeyValue{Key: "a", Value: 1, WorkspaceID: "test"}
		key2 := types.KeyValue{Key: "a", Value: 1, WorkspaceID: "test"}
		notAvailable, _, err := badger.Get(key1)
		require.NoError(t, err)
		require.True(t, notAvailable)
		err = badger.Commit([]string{key1.Key})
		require.NoError(t, err)
		notAvailable, _, err = badger.Get(key2)
		require.NoError(t, err)
		require.False(t, notAvailable)
	})
	t.Run("Same messageID should be deduped from cache", func(t *testing.T) {
		key1 := types.KeyValue{Key: "b", Value: 1, WorkspaceID: "test"}
		key2 := types.KeyValue{Key: "b", Value: 1, WorkspaceID: "test"}
		found, _, err := badger.Get(key1)
		require.NoError(t, err)
		require.True(t, found)
		found, _, err = badger.Get(key2)
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("different messageID should not be deduped for batch", func(t *testing.T) {
		keys := []types.KeyValue{
			{Key: "c", Value: 1, WorkspaceID: "test"},
			{Key: "d", Value: 1, WorkspaceID: "test"},
			{Key: "e", Value: 1, WorkspaceID: "test"},
		}
		found, _, err := badger.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, found, 3)
		for _, key := range keys {
			require.True(t, found[key])
		}
	})
	t.Run("same messageID should be deduped for batch", func(t *testing.T) {
		keys := []types.KeyValue{
			{Key: "f", Value: 1, WorkspaceID: "test", JobID: 3},
			{Key: "f", Value: 1, WorkspaceID: "test", JobID: 4},
			{Key: "g", Value: 1, WorkspaceID: "test", JobID: 5},
		}
		expected := map[types.KeyValue]bool{
			keys[0]: true,
			keys[1]: false,
			keys[2]: true,
		}
		found, _, err := badger.GetBatch(keys)
		require.NoError(t, err)
		require.Len(t, found, 3)
		for _, key := range keys {
			require.Equal(t, expected[key], found[key])
		}
	})
}

func TestBadgerClose(t *testing.T) {
	badger := NewBadgerDB(config.New(), stats.NOP, t.TempDir())
	require.NotNil(t, badger)

	t.Log("close badger without any other operation")
	badger.Close()
}

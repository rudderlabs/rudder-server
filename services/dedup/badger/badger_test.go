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
		key1 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test"}
		key2 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test"}
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
		key1 := types.KeyValue{Key: "b", Value: 1, WorkspaceId: "test"}
		key2 := types.KeyValue{Key: "b", Value: 1, WorkspaceId: "test"}
		found, _, err := badger.Get(key1)
		require.NoError(t, err)
		require.True(t, found)
		found, _, err = badger.Get(key2)
		require.NoError(t, err)
		require.False(t, found)
	})
}

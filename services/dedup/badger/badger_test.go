package badger

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
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
		key := types.BatchKey{Key: "a"}
		notAvailable, err := badger.Allowed(key)
		require.NoError(t, err)
		require.True(t, notAvailable[key])
		err = badger.Commit([]string{key.Key})
		require.NoError(t, err)
		notAvailable, err = badger.Allowed(key)
		require.NoError(t, err)
		require.False(t, notAvailable[key])
	})
	t.Run("Same messageID should be deduped from cache", func(t *testing.T) {
		key := types.BatchKey{Key: "b"}
		found, err := badger.Allowed(key)
		require.NoError(t, err)
		require.True(t, found[key])
		found, err = badger.Allowed(key)
		require.NoError(t, err)
		require.False(t, found[key])
	})
	t.Run("different messageID should not be deduped for batch", func(t *testing.T) {
		keys := []types.BatchKey{
			{Index: 0, Key: "c"},
			{Index: 1, Key: "d"},
			{Index: 2, Key: "e"},
		}
		found, err := badger.Allowed(keys...)
		require.NoError(t, err)
		for _, key := range keys {
			require.True(t, found[key])
		}
	})
	t.Run("same messageID should be deduped for batch", func(t *testing.T) {
		keys := []types.BatchKey{
			{Index: 0, Key: "f"},
			{Index: 1, Key: "f"},
			{Index: 2, Key: "g"},
		}
		expected := map[types.BatchKey]bool{
			keys[0]: true,
			keys[1]: false,
			keys[2]: true,
		}
		found, err := badger.Allowed(keys...)
		require.NoError(t, err)
		for _, key := range keys {
			require.Equal(t, expected[key], found[key])
		}
	})
}

func TestBadgerDirCleanup(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := t.TempDir()
	conf := config.New()
	conf.Set("BadgerDB.memTableSize", 1*bytesize.MB)
	t.Setenv("RUDDER_TMPDIR", dbPath)
	badger := NewBadgerDB(conf, stats.NOP, DefaultPath())
	allowed, err := badger.Allowed(types.BatchKey{Key: "a"})
	require.NoError(t, err)
	require.True(t, allowed[types.BatchKey{Key: "a"}])
	err = badger.Commit([]string{"a"})
	require.NoError(t, err)
	allowed, err = badger.Allowed(types.BatchKey{Key: "a"})
	require.NoError(t, err)
	require.False(t, allowed[types.BatchKey{Key: "a"}])
	badger.Close()

	// reopen
	badger = NewBadgerDB(conf, stats.NOP, DefaultPath())
	allowed, err = badger.Allowed(types.BatchKey{Key: "a"})
	require.NoError(t, err)
	require.False(t, allowed[types.BatchKey{Key: "a"}], "since the directory wasn't cleaned up, the key should be present")
	badger.Close()

	// corrupt KEYREGISTRY file
	require.NoError(t, os.WriteFile(path.Join(dbPath, "badgerdbv4", "KEYREGISTRY"), []byte("corrupted"), 0o644))
	badger = NewBadgerDB(conf, stats.NOP, DefaultPath())
	allowed, err = badger.Allowed(types.BatchKey{Key: "a"})
	require.NoError(t, err)
	require.True(t, allowed[types.BatchKey{Key: "a"}], "since the directory was cleaned up, the key should not be present")
	err = badger.Commit([]string{"a"})
	require.NoError(t, err)
	badger.Close()

	// cleanup on startup
	conf.Set("BadgerDB.cleanupOnStartup", true)
	badger = NewBadgerDB(conf, stats.NOP, DefaultPath())
	allowed, err = badger.Allowed(types.BatchKey{Key: "a"})
	require.NoError(t, err)
	require.True(t, allowed[types.BatchKey{Key: "a"}], "since the directory was cleaned up, the key should not be present")
	err = badger.Commit([]string{"a"})
	require.NoError(t, err)
	badger.Close()
}

func TestBadgerClose(t *testing.T) {
	badger := NewBadgerDB(config.New(), stats.NOP, t.TempDir())
	require.NotNil(t, badger)

	t.Log("close badger without any other operation")
	badger.Close()
}

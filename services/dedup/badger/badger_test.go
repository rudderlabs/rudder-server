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
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Test_Badger(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := t.TempDir()
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)

	// Create BadgerDB directly
	db, err := NewBadgerDB(conf, stats.NOP, DefaultPath())
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	t.Run("Set and Get keys", func(t *testing.T) {
		keys := []string{"a", "b", "c"}

		// Initially keys should not exist
		result, err := db.Get(keys)
		require.NoError(t, err)
		require.Empty(t, result)

		// Set keys
		err = db.Set(keys)
		require.NoError(t, err)

		// Keys should now exist
		result, err = db.Get(keys)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.True(t, result["a"])
		require.True(t, result["b"])
		require.True(t, result["c"])
	})

	t.Run("Mixed existing and non-existing keys", func(t *testing.T) {
		existingKeys := []string{"d", "e"}
		newKeys := []string{"f"}
		allKeys := append(existingKeys, newKeys...)

		// Set only some keys
		err := db.Set(existingKeys)
		require.NoError(t, err)

		// Get all keys
		result, err := db.Get(allKeys)
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.True(t, result["d"])
		require.True(t, result["e"])
		_, exists := result["f"]
		require.False(t, exists)
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

	// Create BadgerDB directly
	db, err := NewBadgerDB(conf, stats.NOP, DefaultPath())
	require.NoError(t, err)

	// Test setting and getting a key
	err = db.Set([]string{"a"})
	require.NoError(t, err)

	result, err := db.Get([]string{"a"})
	require.NoError(t, err)
	require.True(t, result["a"])
	db.Close()

	// reopen
	db, err = NewBadgerDB(conf, stats.NOP, DefaultPath())
	require.NoError(t, err)

	result, err = db.Get([]string{"a"})
	require.NoError(t, err)
	require.True(t, result["a"], "since the directory wasn't cleaned up, the key should be present")
	db.Close()

	// corrupt KEYREGISTRY file
	require.NoError(t, os.WriteFile(path.Join(dbPath, "badgerdbv4", "KEYREGISTRY"), []byte("corrupted"), 0o644))
	db, err = NewBadgerDB(conf, stats.NOP, DefaultPath())
	require.NoError(t, err)

	// After corruption and cleanup, key should not be present
	result, err = db.Get([]string{"a"})
	require.NoError(t, err)
	require.False(t, result["a"], "since the directory was cleaned up, the key should not be present")

	err = db.Set([]string{"a"})
	require.NoError(t, err)
	db.Close()

	// cleanup on startup
	conf.Set("BadgerDB.cleanupOnStartup", true)
	db, err = NewBadgerDB(conf, stats.NOP, DefaultPath())
	require.NoError(t, err)

	result, err = db.Get([]string{"a"})
	require.NoError(t, err)
	require.False(t, result["a"], "since the directory was cleaned up, the key should not be present")

	err = db.Set([]string{"a"})
	require.NoError(t, err)
	db.Close()
}

func TestBadgerClose(t *testing.T) {
	conf := config.New()
	db, err := NewBadgerDB(conf, stats.NOP, t.TempDir())
	require.NoError(t, err)
	require.NotNil(t, db)

	t.Log("close badger without any other operation")
	db.Close()
}

package mirrorBadger

import (
	"os"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/scylla"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Test_MirrorBadger(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	keySpace := strings.ToUpper(rand.String(5))
	resource, err := scylla.Setup(pool, t, scylla.WithKeyspace(keySpace))
	require.NoError(t, err)
	conf.Set("Scylla.Hosts", resource.URL)
	conf.Set("Scylla.Keyspace", keySpace)
	mirrorBadger, err := NewMirrorBadger(conf, stats.NOP)
	require.Nil(t, err)
	require.NotNil(t, mirrorBadger)
	defer mirrorBadger.Close()
	t.Run("Same messageID should be deduped from badger", func(t *testing.T) {
		key1 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test"}
		key2 := types.KeyValue{Key: "a", Value: 1, WorkspaceId: "test"}
		found, _, err := mirrorBadger.Get(key1)
		require.Nil(t, err)
		require.True(t, found)
		err = mirrorBadger.Commit([]string{key1.Key})
		require.NoError(t, err)
		found, _, err = mirrorBadger.Get(key2)
		require.Nil(t, err)
		require.False(t, found)
	})
	t.Run("Same messageID should be deduped from cache", func(t *testing.T) {
		key1 := types.KeyValue{Key: "b", Value: 1, WorkspaceId: "test"}
		key2 := types.KeyValue{Key: "b", Value: 1, WorkspaceId: "test"}
		found, _, err := mirrorBadger.Get(key1)
		require.Nil(t, err)
		require.True(t, found)
		found, _, err = mirrorBadger.Get(key2)
		require.Nil(t, err)
		require.False(t, found)
	})
}

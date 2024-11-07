package dedup_test

import (
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/scylla"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Test_Dedup(t *testing.T) {
	testCases := []struct {
		name string
	}{
		{
			name: "Badger",
		},
		{
			name: "Scylla",
		},
		{
			name: "MirrorScylla",
		},
		{
			name: "MirrorBadger",
		},
		{
			name: "Random",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config.Reset()
			logger.Reset()
			misc.Init()

			dbPath := t.TempDir()
			conf := config.New()
			t.Setenv("RUDDER_TMPDIR", dbPath)
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			keySpace := strings.ToUpper(rand.String(5))
			table := rand.String(5)
			resource, err := scylla.Setup(pool, t, scylla.WithKeyspace(keySpace))
			require.NoError(t, err)
			conf.Set("Scylla.Hosts", resource.URL)
			conf.Set("Scylla.Keyspace", keySpace)
			conf.Set("Scylla.TableName", table)
			conf.Set("Dedup.Mode", tc.name)
			d, err := dedup.New(conf, stats.Default)
			require.Nil(t, err)
			defer d.Close()

			t.Run("if message id is not present in cache and badger db", func(t *testing.T) {
				found, err := d.Get(types.KeyValue{Key: "a", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
				require.NoError(t, err)
				require.Equal(t, true, found)

				// Checking it again should give us the previous value from the cache
				found, err = d.Get(types.KeyValue{Key: "a", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
				require.Nil(t, err)
				require.Equal(t, false, found)
			})

			t.Run("if message is committed, previous value should always return", func(t *testing.T) {
				found, err := d.Get(types.KeyValue{Key: "b", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
				require.Nil(t, err)
				require.Equal(t, true, found)

				err = d.Commit([]string{"a"})
				require.NoError(t, err)

				found, err = d.Get(types.KeyValue{Key: "b", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
				require.Nil(t, err)
				require.Equal(t, false, found)
			})

			t.Run("committing a messageid not present in cache", func(t *testing.T) {
				found, err := d.Get(types.KeyValue{Key: "c", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
				require.Nil(t, err)
				require.Equal(t, true, found)

				err = d.Commit([]string{"d"})
				require.NotNil(t, err)
			})

			t.Run("test GetBatch with unique keys", func(t *testing.T) {
				kvs := []types.KeyValue{
					{Key: "e", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"},
					{Key: "f", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"},
					{Key: "g", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"},
				}
				found, err := d.GetBatch(kvs)
				require.Nil(t, err)
				require.Len(t, found, 3)
				for _, kv := range kvs {
					require.Equal(t, true, found[kv])
				}
				err = d.Commit([]string{"e", "f", "g"})
				require.NoError(t, err)
			})

			t.Run("test GetBatch with non-unique keys", func(t *testing.T) {
				kvs := []types.KeyValue{
					{Key: "g", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh", JobID: 3},
					{Key: "h", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh", JobID: 4},
					{Key: "h", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh", JobID: 5},
				}
				expected := map[types.KeyValue]bool{
					kvs[0]: false,
					kvs[1]: true,
					kvs[2]: false,
				}
				found, err := d.GetBatch(kvs)
				require.Nil(t, err)
				require.Len(t, found, 3)
				for _, kv := range kvs {
					require.Equal(t, expected[kv], found[kv])
				}
				err = d.Commit([]string{"h"})
				require.NoError(t, err)
			})
		})
	}
}

func Test_Dedup_Window(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test"
	conf := config.New()
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)
	conf.Set("Dedup.dedupWindow", "1s")
	t.Setenv("RUDDER_TMPDIR", dbPath)
	d, err := dedup.New(conf, stats.Default)
	require.Nil(t, err)
	defer d.Close()

	found, err := d.Get(types.KeyValue{Key: "to be deleted", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
	require.Nil(t, err)
	require.Equal(t, true, found)

	err = d.Commit([]string{"to be deleted"})
	require.NoError(t, err)

	found, err = d.Get(types.KeyValue{Key: "to be deleted", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
	require.Nil(t, err)
	require.Equal(t, false, found)

	require.Eventually(t, func() bool {
		found, err = d.Get(types.KeyValue{Key: "to be deleted", WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
		require.Nil(t, err)
		return found
	}, 2*time.Second, 100*time.Millisecond)
}

func Test_Dedup_ErrTxnTooBig(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test_errtxntoobig"
	defer func() { _ = os.RemoveAll(dbPath) }()
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)
	d, err := dedup.New(conf, stats.Default)
	require.Nil(t, err)
	defer d.Close()

	size := 105_000
	messages := make([]string, size)
	for i := 0; i < size; i++ {
		key := uuid.New().String()
		messages[i] = key
		_, _ = d.Get(types.KeyValue{Key: key, WorkspaceID: "test"})
	}
	err = d.Commit(messages)
	require.NoError(t, err)
}

func Benchmark_Dedup(b *testing.B) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := path.Join("./testdata", "tmp", rand.String(10), "/DB_Benchmark_Dedup")
	b.Logf("using path %s, since tmpDir has issues in macOS\n", dbPath)
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.MkdirAll(dbPath, 0o750)
	conf := config.New()
	b.Setenv("RUDDER_TMPDIR", dbPath)
	d, err := dedup.New(conf, stats.Default)
	require.NoError(b, err)

	b.Run("no duplicates 1000 batch unique", func(b *testing.B) {
		batchSize := 1000

		msgIDs := make([]types.KeyValue, batchSize)
		keys := make([]string, 0)
		for i := 0; i < b.N; i++ {
			key := uuid.New().String()
			msgIDs[i%batchSize] = types.KeyValue{
				Key:         key,
				WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh",
			}
			keys = append(keys, key)
			if i%batchSize == batchSize-1 || i == b.N-1 {
				for _, msgID := range msgIDs[:i%batchSize] {
					_, _ = d.Get(msgID)
				}
				err := d.Commit(keys)
				require.NoError(b, err)
				keys = nil
			}
		}
		b.ReportMetric(float64(b.N), "events")
		b.ReportMetric(float64(b.N*len(uuid.New().String())), "bytes")
	})
	d.Close()

	cmd := exec.Command("du", "-sh", dbPath)
	out, err := cmd.Output()
	if err != nil {
		b.Log(err)
	}

	b.Log("db size:", string(out))
}

// Benchmark_DedupModes/MirrorBadger-12   	    1072	   1101878 ns/op
// Benchmark_DedupModes/MirrorScylla-12   	     566	   1986533 ns/op
// Benchmark_DedupModes/Scylla-12         	     990	   1525086 ns/op
// Benchmark_DedupModes/Badger-12         	  108246	      9981 ns/op

func Benchmark_DedupModes(b *testing.B) {
	testCases := []struct {
		name string
	}{
		{
			name: "Badger",
		},
		{
			name: "Scylla",
		},
		{
			name: "MirrorScylla",
		},
		{
			name: "MirrorBadger",
		},
		{
			name: "Random",
		},
	}
	pool, err := dockertest.NewPool("")
	require.NoError(b, err)
	keySpace := strings.ToUpper(rand.String(5))
	table := rand.String(5)
	resource, err := scylla.Setup(pool, b, scylla.WithKeyspace(keySpace))
	require.NoError(b, err)
	for _, tc := range testCases {
		config.Reset()
		logger.Reset()
		misc.Init()
		dbPath := b.TempDir()
		conf := config.New()
		conf.Set("Scylla.Hosts", resource.URL)
		conf.Set("Scylla.Keyspace", keySpace)
		conf.Set("Scylla.TableName", table)
		b.Setenv("RUDDER_TMPDIR", dbPath)
		conf.Set("Dedup.Mode", tc.name)
		d, err := dedup.New(conf, stats.Default)
		require.Nil(b, err)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				key := uuid.New().String()
				_, err = d.Get(types.KeyValue{Key: key, WorkspaceID: "2DAZvjf8PEMrAkbVm6smqEJnh"})
				require.NoError(b, err)
				err = d.Commit([]string{key})
				require.NoError(b, err)
			}
		})
		d.Close()
	}
}

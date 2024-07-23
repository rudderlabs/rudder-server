package dedup_test

import (
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

func Test_Dedup(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)
	d := dedup.New()
	defer d.Close()

	t.Run("if message id is not present in cache and badger db", func(t *testing.T) {
		found, _, err := d.Set(types.KeyValue{Key: "a", Value: 1})
		require.Nil(t, err)
		require.Equal(t, true, found)

		// Checking it again should give us the previous value from the cache
		found, value, err := d.Set(types.KeyValue{Key: "a", Value: 2})
		require.Nil(t, err)
		require.Equal(t, false, found)
		require.Equal(t, int64(1), value)
	})

	t.Run("if message is committed, previous value should always return", func(t *testing.T) {
		found, _, err := d.Set(types.KeyValue{Key: "b", Value: 1})
		require.Nil(t, err)
		require.Equal(t, true, found)

		err = d.Commit([]string{"a"})
		require.NoError(t, err)

		found, value, err := d.Set(types.KeyValue{Key: "b", Value: 2})
		require.Nil(t, err)
		require.Equal(t, false, found)
		require.Equal(t, int64(1), value)
	})

	t.Run("committing a messageid not present in cache", func(t *testing.T) {
		found, _, err := d.Set(types.KeyValue{Key: "c", Value: 1})
		require.Nil(t, err)
		require.Equal(t, true, found)

		err = d.Commit([]string{"d"})
		require.NotNil(t, err)
	})
}

func Test_Dedup_Window(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)
	config.Set("Dedup.dedupWindow", "1s")
	d := dedup.New()
	defer d.Close()

	found, _, err := d.Set(types.KeyValue{Key: "to be deleted", Value: 1})
	require.Nil(t, err)
	require.Equal(t, true, found)

	err = d.Commit([]string{"to be deleted"})
	require.NoError(t, err)

	found, _, err = d.Set(types.KeyValue{Key: "to be deleted", Value: 2})
	require.Nil(t, err)
	require.Equal(t, false, found)

	require.Eventually(t, func() bool {
		found, _, err = d.Set(types.KeyValue{Key: "to be deleted", Value: 3})
		require.Nil(t, err)
		return found
	}, 2*time.Second, 100*time.Millisecond)
}

func Test_Dedup_ErrTxnTooBig(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test_errtxntoobig"
	defer os.RemoveAll(dbPath)
	os.RemoveAll(dbPath)
	d := dedup.New()
	defer d.Close()

	size := 105_000
	messages := make([]string, size)
	for i := 0; i < size; i++ {
		messages[i] = uuid.New().String()
		_, _, _ = d.Set(types.KeyValue{Key: messages[i], Value: int64(i + 1)})
	}
	err := d.Commit(messages)
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
	d := dedup.New()

	b.Run("no duplicates 1000 batch unique", func(b *testing.B) {
		batchSize := 1000

		msgIDs := make([]types.KeyValue, batchSize)
		keys := make([]string, 0)

		for i := 0; i < b.N; i++ {
			msgIDs[i%batchSize] = types.KeyValue{
				Key:   uuid.New().String(),
				Value: int64(i + 1),
			}

			if i%batchSize == batchSize-1 || i == b.N-1 {
				for _, msgID := range msgIDs[:i%batchSize] {
					_, _, _ = d.Set(msgID)
					keys = append(keys, msgID.Key)
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

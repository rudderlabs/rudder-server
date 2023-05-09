package dedup_test

import (
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/dedup"
)

func Test_Dedup(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
	defer d.Close()

	t.Run("if message id is not present in cache and badger db", func(t *testing.T) {
		found, _ := d.Set(dedup.KeyValue{Key: "a", Value: 1})
		require.Equal(t, true, found)

		// Checking it again should give us the previous value from the cache
		found, value := d.Set(dedup.KeyValue{Key: "a", Value: 2})
		require.Equal(t, false, found)
		require.Equal(t, int64(1), value)
	})

	t.Run("if message is committed, previous value should always return", func(t *testing.T) {
		found, _ := d.Set(dedup.KeyValue{Key: "b", Value: 1})
		require.Equal(t, true, found)

		err := d.Commit([]string{"a"})
		require.NoError(t, err)

		found, value := d.Set(dedup.KeyValue{Key: "b", Value: 2})
		require.Equal(t, false, found)
		require.Equal(t, int64(1), value)
	})

	t.Run("committing a messageid not present in cache", func(t *testing.T) {
		found, _ := d.Set(dedup.KeyValue{Key: "c", Value: 1})
		require.Equal(t, true, found)

		err := d.Commit([]string{"d"})
		require.NotNil(t, err)
	})
}

func Test_Dedup_Window(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Second))
	defer d.Close()

	found, _ := d.Set(dedup.KeyValue{Key: "to be deleted", Value: 1})
	require.Equal(t, true, found)

	err := d.Commit([]string{"to be deleted"})
	require.NoError(t, err)

	found, _ = d.Set(dedup.KeyValue{Key: "to be deleted", Value: 2})
	require.Equal(t, false, found)

	require.Eventually(t, func() bool {
		found, _ = d.Set(dedup.KeyValue{Key: "to be deleted", Value: 3})
		return found
	}, 2*time.Second, 100*time.Millisecond)
}

func Test_Dedup_ClearDB(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	t.Run("Setting a messageid with clear db and dedup window", func(t *testing.T) {
		d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
		found, _ := d.Set(dedup.KeyValue{Key: "a", Value: 1})
		require.Equal(t, true, found)
		err := d.Commit([]string{"a"})
		require.NoError(t, err)
		d.Close()
	})
	t.Run("Setting a messageid without cleardb should return false and previous value", func(t *testing.T) {
		dNew := dedup.New(dbPath)
		found, size := dNew.Set(dedup.KeyValue{Key: "a", Value: 2})
		require.Equal(t, false, found)
		require.Equal(t, int64(1), size)
		dNew.Close()
	})
	t.Run("Setting a messageid with cleardb should return true", func(t *testing.T) {
		dWithClear := dedup.New(dbPath, dedup.WithClearDB())
		found, _ := dWithClear.Set(dedup.KeyValue{Key: "a", Value: 1})
		require.Equal(t, true, found)
		dWithClear.Close()
	})
}

func Test_Dedup_ErrTxnTooBig(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test_errtxntoobig"
	defer os.RemoveAll(dbPath)
	os.RemoveAll(dbPath)
	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
	defer d.Close()

	size := 105_000
	messages := make([]string, size)
	for i := 0; i < size; i++ {
		messages[i] = uuid.New().String()
		d.Set(dedup.KeyValue{Key: messages[i], Value: int64(i + 1)})
	}
	err := d.Commit(messages)
	require.NoError(t, err)
}

func Benchmark_Dedup(b *testing.B) {
	config.Reset()
	logger.Reset()
	dbPath := path.Join("./testdata", "tmp", rand.String(10), "/DB_Benchmark_Dedup")
	b.Logf("using path %s, since tmpDir has issues in macOS\n", dbPath)
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.MkdirAll(dbPath, 0o750)
	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Minute))

	b.Run("no duplicates 1000 batch unique", func(b *testing.B) {
		batchSize := 1000

		msgIDs := make([]dedup.KeyValue, batchSize)
		keys := make([]string, 0)

		for i := 0; i < b.N; i++ {
			msgIDs[i%batchSize] = dedup.KeyValue{
				Key:   uuid.New().String(),
				Value: int64(i + 1),
			}

			if i%batchSize == batchSize-1 || i == b.N-1 {
				for _, msgID := range msgIDs[:i%batchSize] {
					d.Set(msgID)
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

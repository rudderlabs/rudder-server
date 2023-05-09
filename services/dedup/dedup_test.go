package dedup_test

import (
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
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

	t.Run("no duplicate if not marked as processed", func(t *testing.T) {
		messageIDs := []string{"a", "b", "c"}
		for _, messageID := range messageIDs {
			_, found := d.Get(messageID)
			require.Equal(t, false, found)
		}

		// Checking it again should still result as false
		for _, messageID := range messageIDs {
			_, found := d.Get(messageID)
			require.Equal(t, false, found)
		}
	})

	t.Run("duplicate after marked as processed", func(t *testing.T) {
		err := d.Set([]dedup.KeyValue{
			{Key: "a", Value: 1},
			{Key: "b", Value: 2},
			{Key: "c", Value: 3},
		})
		require.NoError(t, err)
		messageIDs := []string{"a", "b", "c"}
		for idx, messageID := range messageIDs {
			size, found := d.Get(messageID)
			require.Equal(t, int64(idx+1), size)
			require.Equal(t, true, found)
		}

		messageIDs = []string{"d", "e"}
		for _, messageID := range messageIDs {
			_, found := d.Get(messageID)
			require.Equal(t, false, found)
		}
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

	err := d.Set([]dedup.KeyValue{
		{Key: "to be deleted", Value: 1},
	})
	require.NoError(t, err)

	_, found := d.Get("to be deleted")
	require.Equal(t, true, found)

	require.Eventually(t, func() bool {
		_, found := d.Get("to be deleted")
		return !found
	}, 2*time.Second, 100*time.Millisecond)

	_, found = d.Get("to be deleted")
	require.Equal(t, false, found)
}

func Test_Dedup_ClearDB(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	{
		d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
		err := d.Set([]dedup.KeyValue{
			{Key: "a", Value: 1},
		})
		require.NoError(t, err)
		d.Close()
	}
	{
		dNew := dedup.New(dbPath)
		size, found := dNew.Get("a")
		require.Equal(t, true, found)
		require.Equal(t, int64(1), size)
		dNew.Close()
	}
	{
		dWithClear := dedup.New(dbPath, dedup.WithClearDB())
		_, found := dWithClear.Get("a")
		require.Equal(t, false, found)
		dWithClear.Close()
	}
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
	messages := make([]dedup.KeyValue, size)
	for i := 0; i < size; i++ {
		messages[i] = dedup.KeyValue{
			Key:   uuid.New().String(),
			Value: int64(i + 1),
		}
	}
	err := d.Set(messages)
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

		for i := 0; i < b.N; i++ {
			msgIDs[i%batchSize] = dedup.KeyValue{
				Key:   uuid.New().String(),
				Value: int64(i + 1),
			}

			if i%batchSize == batchSize-1 || i == b.N-1 {
				for _, msgID := range msgIDs[:i%batchSize] {
					d.Get(msgID.Key)
				}
				err := d.Set(msgIDs[:i%batchSize])
				require.NoError(b, err)
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

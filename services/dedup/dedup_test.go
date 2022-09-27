package dedup_test

import (
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/utils/logger"
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
		dups := d.FindDuplicates([]string{"a", "b", "c"}, nil)
		require.Equal(t, []int{}, dups)

		dupsAgain := d.FindDuplicates([]string{"a", "b", "c"}, nil)
		require.Equal(t, []int{}, dupsAgain)
	})

	t.Run("duplicate after marked as processed", func(t *testing.T) {
		err := d.MarkProcessed([]string{"a", "b", "c"})
		require.NoError(t, err)
		dups := d.FindDuplicates([]string{"a", "b", "c"}, nil)
		require.Equal(t, []int{0, 1, 2}, dups)

		dupsOther := d.FindDuplicates([]string{"d", "e"}, nil)
		require.Equal(t, []int{}, dupsOther)
	})

	t.Run("no duplicate if not marked as processed", func(t *testing.T) {
		dups := d.FindDuplicates([]string{"x", "y", "z"}, map[string]struct{}{"x": {}, "z": {}})
		require.Equal(t, []int{0, 2}, dups)

		dupsAgain := d.FindDuplicates([]string{"x", "y", "z"}, nil)
		require.Equal(t, []int{}, dupsAgain)
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

	err := d.MarkProcessed([]string{"to be deleted"})
	require.NoError(t, err)

	dups := d.FindDuplicates([]string{"to be deleted"}, nil)
	require.Equal(t, []int{0}, dups)

	require.Eventually(t, func() bool {
		return len(d.FindDuplicates([]string{"to be deleted"}, nil)) == 0
	}, 2*time.Second, 100*time.Millisecond)

	dupsAfter := d.FindDuplicates([]string{"to be deleted"}, nil)
	require.Equal(t, []int{}, dupsAfter)
}

func Test_Dedup_ClearDB(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	{
		d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
		err := d.MarkProcessed([]string{"a"})
		require.NoError(t, err)
		d.Close()
	}
	{
		dNew := dedup.New(dbPath)
		dupsAgain := dNew.FindDuplicates([]string{"a"}, nil)
		require.Equal(t, []int{0}, dupsAgain)
		dNew.Close()
	}
	{
		dWithClear := dedup.New(dbPath, dedup.WithClearDB())
		dupsAgain := dWithClear.FindDuplicates([]string{"a"}, nil)
		require.Equal(t, []int{}, dupsAgain)
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
	messageIDs := make([]string, size)
	for i := 0; i < size; i++ {
		messageIDs[i] = uuid.New().String()
	}
	err := d.MarkProcessed(messageIDs)
	require.NoError(t, err)
}

var duplicateIndexes []int

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

		msgIDs := make([]string, batchSize)

		for i := 0; i < b.N; i++ {
			msgIDs[i%batchSize] = uuid.New().String()

			if i%batchSize == batchSize-1 || i == b.N-1 {
				duplicateIndexes = d.FindDuplicates(msgIDs[:i%batchSize], nil)
				err := d.MarkProcessed(msgIDs[:i%batchSize])
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

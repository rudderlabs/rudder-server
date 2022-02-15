package dedup_test

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Test_Debup(t *testing.T) {
	config.Load()
	logger.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer os.RemoveAll(dbPath)
	os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
	defer d.Close()

	t.Run("no duplicate if not marked as processed", func(t *testing.T) {
		dups := d.FindDuplicates([]string{"a", "b", "c"}, nil)
		require.Equal(t, []int{}, dups)

		dupsAgain := d.FindDuplicates([]string{"a", "b", "c"}, nil)
		require.Equal(t, []int{}, dupsAgain)
	})

	t.Run("duplicate after marked as processed", func(t *testing.T) {
		d.MarkProcessed([]string{"a", "b", "c"})
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
func Test_Debup_Window(t *testing.T) {
	config.Load()
	logger.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer os.RemoveAll(dbPath)
	os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Second))
	defer d.Close()

	d.MarkProcessed([]string{"to be deleted"})

	dups := d.FindDuplicates([]string{"to be deleted"}, nil)
	require.Equal(t, []int{0}, dups)

	require.Eventually(t, func() bool {
		return len(d.FindDuplicates([]string{"to be deleted"}, nil)) == 0
	}, 2*time.Second, 100*time.Millisecond)

	dupsAfter := d.FindDuplicates([]string{"to be deleted"}, nil)
	require.Equal(t, []int{}, dupsAfter)
}

func Test_Debup_ClearDB(t *testing.T) {
	config.Load()
	logger.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer os.RemoveAll(dbPath)
	os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
	d.MarkProcessed([]string{"a"})
	d.Close()

	dNew := dedup.New(dbPath)
	dupsAgain := dNew.FindDuplicates([]string{"a"}, nil)
	require.Equal(t, []int{0}, dupsAgain)
	dNew.Close()

	// FIXME add test with clearDB
}

var duplicateIndexes []int

func Benchmark_Dedup(b *testing.B) {
	config.Load()
	logger.Init()
	rand.Seed(time.Now().UnixNano())
	dbPath := path.Join("./testdata", "tmp", randSeq(10), "/DB_Benchmark_Dedup")
	b.Logf("using path %s, since tmpDir has issues in macOS\n", dbPath)
	defer os.RemoveAll(dbPath)
	os.MkdirAll(dbPath, 0750)
	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Minute))

	b.Run("no duplicates 1000 batch unique", func(b *testing.B) {
		batchSize := 1000

		msgIDs := make([]string, batchSize)

		for i := 0; i < b.N; i++ {
			msgIDs[i%batchSize] = uuid.New().String()

			if i%batchSize == batchSize-1 || i == b.N-1 {
				duplicateIndexes = d.FindDuplicates(msgIDs[:i%batchSize], nil)
				d.MarkProcessed(msgIDs[:i%batchSize])
			}
		}
		b.ReportMetric(float64(b.N), "events")
		b.ReportMetric(float64(b.N*len(uuid.New().String())), "bytes")
	})
	d.Close()

	cmd := exec.Command("du", "-sh", dbPath)
	out, err := cmd.Output()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("db size:", string(out))
}

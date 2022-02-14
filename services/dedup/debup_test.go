package dedup_test

import (
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func Benchmark_Dedup(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		b.ReportAllocs()
		b.StartTimer()
	}
}

func Test_Debup(t *testing.T) {
	config.Load()
	logger.Init()

	dbPath := os.TempDir() + "/dedup_test"
	defer os.RemoveAll(dbPath)	
	os.Setenv("DEDUP_DEDUP_WINDOW", "1s")
	dedup.Init()
	d := dedup.New(dbPath, true)
	d.MarkProcessed([]string{"1", "2", "3"})

	dups := d.FindDuplicates([]string{"1", "2", "3"}, map[string]struct{}{"1": struct{}{}, "2": struct{}{}, "3": struct{}{}})

	require.Equal(t, []int{0, 1, 2}, dups)
}

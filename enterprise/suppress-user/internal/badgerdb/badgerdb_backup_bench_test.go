package badgerdb_test

import (
	"compress/gzip"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

// BenchmarkBackupRestore benchmarks the backup and restore time of the badger repository
// after seeding it with a very large number of suppressions.
func BenchmarkBackupRestore(b *testing.B) {
	b.StopTimer()
	totalSuppressions := 40_000_000
	batchSize := 5000
	backupFilename := path.Join(b.TempDir(), "backup.badger")

	repo1Path := path.Join(b.TempDir(), "repo-1")
	repo1, err := badgerdb.NewRepository(repo1Path, logger.NOP, stats.Default)
	require.NoError(b, err)

	for i := 0; i < totalSuppressions/batchSize; i++ {
		suppressions := generateSuppressions(i*batchSize, batchSize)
		token := []byte(fmt.Sprintf("token%d", i))
		require.NoError(b, repo1.Add(suppressions, token))
	}

	b.Run("backup", func(b *testing.B) {
		b.StopTimer()
		start := time.Now()
		f, err := os.Create(backupFilename)
		w, _ := gzip.NewWriterLevel(f, gzip.BestSpeed)
		defer func() { _ = f.Close() }()
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, repo1.Backup(w))
		w.Close()
		b.StopTimer()
		dur := time.Since(start)
		fileInfo, err := f.Stat()
		require.NoError(b, err)
		b.ReportMetric(float64(fileInfo.Size()/1024/1024), "filesize(MB)")
		b.ReportMetric(dur.Seconds(), "duration(sec)")
		require.NoError(b, repo1.Stop())
	})

	b.Run("restore", func(b *testing.B) {
		b.StopTimer()
		repo2Path := path.Join(b.TempDir(), "repo-2")
		repo2, err := badgerdb.NewRepository(repo2Path, logger.NOP, stats.Default)
		require.NoError(b, err)

		f, err := os.Open(backupFilename)
		require.NoError(b, err)
		defer func() { _ = f.Close() }()
		r, err := gzip.NewReader(f)
		require.NoError(b, err)
		fileInfo, err := f.Stat()
		require.NoError(b, err)
		b.ReportMetric(float64(fileInfo.Size()/1024/1024), "filesize(MB)")

		start := time.Now()
		b.StartTimer()
		require.NoError(b, repo2.Restore(r))
		b.StopTimer()
		r.Close()
		b.ReportMetric(time.Since(start).Seconds(), "duration(sec)")

		require.NoError(b, repo2.Stop())
	})
}

func generateSuppressions(startFrom, batchSize int) []model.Suppression {
	var res []model.Suppression

	for i := startFrom; i < startFrom+batchSize; i++ {
		res = append(res, model.Suppression{
			Canceled:    false,
			WorkspaceID: "1yaBlqltp5Y4V2NK8qePowlyaaaa",
			UserID:      fmt.Sprintf("client-%d", i),
		})
	}
	return res
}

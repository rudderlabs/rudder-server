package suppression_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

func BenchmarkAddAndSuppress(b *testing.B) {
	totalSuppressions := 20_000_000
	totalReads := 10_000
	batchSize := 5000

	runAddSuppressionsBenchmark := func(b *testing.B, repo suppression.Repository, batchSize, totalSuppressions int) {
		var totalTime time.Duration
		var totalAddSuppressions int
		for i := 0; i < totalSuppressions/batchSize; i++ {
			suppressions1 := generateSuppressions(i*batchSize/2, batchSize/2)
			suppressions2 := generateSuppressions(i*batchSize/2, batchSize/2)
			token := []byte(fmt.Sprintf("token%d", i))
			start := time.Now()
			require.NoError(b, repo.Add(suppressions1, token))
			require.NoError(b, repo.Add(suppressions2, token))
			totalTime += time.Since(start)
			totalAddSuppressions += batchSize
		}
		b.ReportMetric(float64(totalSuppressions)/totalTime.Seconds(), "suppressions/s(add)")
	}

	runSuppressBenchmark := func(b *testing.B, repo suppression.Repository, totalSuppressions, totalReads int) {
		var totalTime time.Duration
		for i := 0; i < totalReads; i++ {
			start := time.Now()
			idx := randomInt(totalSuppressions * 2) // multiply by 2 to include non-existing keys suppressions
			_, err := repo.Suppressed(fmt.Sprintf("workspace%d", idx), fmt.Sprintf("user%d", idx), fmt.Sprintf("source%d", idx))
			require.NoError(b, err)
			totalTime += time.Since(start)
		}
		b.ReportMetric(float64(totalSuppressions)/totalTime.Seconds(), "suppressions/s(read)")
	}

	runAddAndSuppressBenchmark := func(b *testing.B, repo suppression.Repository, totalSuppressions, batchSize, totalReads int) {
		runAddSuppressionsBenchmark(b, repo, batchSize, totalSuppressions)
		runSuppressBenchmark(b, repo, totalSuppressions, totalReads)
	}

	b.Run("badger", func(b *testing.B) {
		repo, err := suppression.NewBadgerRepository(b.TempDir(), logger.NOP)
		require.NoError(b, err)
		defer func() { _ = repo.Stop() }()
		runAddAndSuppressBenchmark(b, repo, totalSuppressions, batchSize, totalReads)
	})

	b.Run("memory", func(b *testing.B) {
		repo := suppression.NewMemoryRepository(logger.NOP)
		defer func() { _ = repo.Stop() }()
		runAddAndSuppressBenchmark(b, repo, totalSuppressions, batchSize, totalReads)
	})
}

func generateSuppressions(startFrom, batchSize int) []model.Suppression {
	var res []model.Suppression

	for i := startFrom; i < startFrom+batchSize; i++ {
		var sourceIDs []string
		wildcard := randomInt(2) == 0
		if wildcard {
			sourceIDs = []string{}
		} else {
			sourceIDs = []string{fmt.Sprintf("source%d", i), "otherSource", "anotherSource"}
		}
		res = append(res, model.Suppression{
			Canceled:    randomInt(2) == 0,
			WorkspaceID: fmt.Sprintf("workspace%d", i),
			UserID:      fmt.Sprintf("user%d", i),
			SourceIDs:   sourceIDs,
		})
	}
	return res
}

func randomInt(lt int) int {
	return rand.Int() % lt // skipcq: GSC-G404
}

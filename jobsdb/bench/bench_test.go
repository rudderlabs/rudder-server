package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	statsmetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/jobsdb/bench"
)

func TestBench(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t, postgres.WithOptions(
			"max_connections=200",
			"max_wal_size=2GB",
			"checkpoint_timeout=30",
			"shared_buffers=512MB",
			"work_mem=64MB",
			"hash_mem_multiplier=4",
			"maintenance_work_mem=200MB",
			"effective_cache_size=4GB",
			"wal_buffers=64MB",
			"random_page_cost=1.1",
			"autovacuum_vacuum_cost_delay=1",
			"autovacuum_naptime=20",
			"checkpoint_warning=0",
		),
			postgres.WithTag("17-alpine"),
			postgres.WithShmSize(256*bytesize.MB),
		)
		require.NoError(t, err)
		postgresContainer.DB.SetMaxOpenConns(60)
		postgresContainer.DB.SetMaxIdleConns(20)

		c := config.New()

		// JobsDB configuration
		c.Set("JobsDB.enableWriterQueue", true) // default: true
		c.Set("JobsDB.maxWriters", 4)           // default: 3
		c.Set("JobsDB.enableReaderQueue", true) //	default: true
		c.Set("JobsDB.maxReaders", 8)           // default: 6
		c.Set("JobsDB.enableToastOptimizations", true)
		c.Set("JobsDB.Compression.enabled", true)
		c.Set("JobsDB.Compression.algorithm", "lz4")
		c.Set("JobsDB.Compression.level", "level1")
		c.Set("JobsDB.indexOptimizations", true)
		c.Set("JobsDB.refreshDSListLoopSleepDuration", 1*time.Second)

		// Bench configuration
		c.Set("JobsDB.Bench.scenario", "simple")
		c.Set("JobsDB.Bench.payloadSize", 2*bytesize.KB) // default: 1KB
		c.Set("JobsDB.Bench.noOfSources", 15)            // default: 10
		c.Set("JobsDB.Bench.writerConcurrency", 4)
		c.Set("JobsDB.Bench.updateConcurrency", 1)
		c.Set("JobsDB.Bench.writerBatchSize", 10)
		c.Set("JobsDB.Bench.readerReadSize", 20000)
		c.Set("JobsDB.Bench.payloadLimit", 100*bytesize.MB)
		c.Set("JobsDB.Bench.insertRateLimit", 1000)

		l := logger.NewFactory(c)
		stat := stats.NewStats(c, l, statsmetric.Instance)
		b, err := bench.New(c, stat, l.NewLogger(), postgresContainer.DB)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled")
			case <-time.After(3 * time.Second):
				cancel()
				return nil
			}
		})
		g.Go(func() error {
			return b.Run(ctx)
		})
		require.NoError(t, g.Wait())
	})

	// TestBench/compaction seeds a read-write jobsdb with 20 datasets of 100K
	// jobs each (3KB payload) spread across 100 destinations, then drains it with
	// 100 consumer goroutines (one per destination). It repeats this for every
	// compaction flag combination and reports the time each takes to drain.
	//
	// This is a heavy benchmark (~1M * 2KB jobs, seeded 4 times). It is skipped
	// unless RUN_COMPACTION_BENCH is set.
	t.Run("compaction", func(t *testing.T) {
		if os.Getenv("RUN_COMPACTION_BENCH") == "" {
			t.Skip("set RUN_COMPACTION_BENCH=1 to run the compaction bench")
		}
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t, postgres.WithOptions(
			"max_connections=200",
			"max_wal_size=2GB",
			"checkpoint_timeout=30",
			"shared_buffers=512MB",
			"work_mem=64MB",
			"hash_mem_multiplier=4",
			"maintenance_work_mem=200MB",
			"effective_cache_size=4GB",
			"wal_buffers=64MB",
			"random_page_cost=1.1",
			"autovacuum_vacuum_cost_delay=1",
			"autovacuum_naptime=20",
			"checkpoint_warning=0",
		),
			postgres.WithTag("17-alpine"),
			postgres.WithShmSize(256*bytesize.MB),
		)
		require.NoError(t, err)
		postgresContainer.DB.SetMaxOpenConns(80)
		postgresContainer.DB.SetMaxIdleConns(10)

		c := config.New()

		// JobsDB configuration
		c.Set("JobsDB.enableWriterQueue", true)
		c.Set("JobsDB.maxWriters", 3)
		c.Set("JobsDB.enableReaderQueue", true)
		c.Set("JobsDB.maxReaders", 6)

		// Common jobsdb configuration under test
		c.Set("JobsDB.noResultsCacheStateOptimization", true)
		c.Set("JobsDB.logCacheBranchInvalidation", true)
		c.Set("JobsDB.warnOnStatusMissingPartitionID", true)
		c.Set("JobsDB.cacheExpiration", 2*time.Hour)
		c.Set("JobsDB.dsLimit", 4)
		c.Set("JobsDB.maxDSRetention", 20*time.Second)
		c.Set("JobsDB.migrateDSLoopSleepDuration", 10*time.Second)
		c.Set("JobsDB.jobMinRowsLeftMigrateThreshold", 0.7)
		c.Set("JobsDB.refreshDSListLoopSleepDuration", 2*time.Second)
		c.Set("JobsDB.addNewDSLoopSleepDuration", 2*time.Second)
		c.Set("JobsDB.maxMigrateDSProbe", 50)
		c.Set("JobsDB.maxTableSizeInMB", 900)
		c.Set("JobsDB.maxDSSize", 100000) // one dataset holds 100K jobs

		// Bench configuration
		c.Set("JobsDB.Bench.scenario", "compaction")
		c.Set("JobsDB.Bench.maintenanceDSN", postgresContainer.DBDsn) // dedicated maintenance pool
		c.Set("JobsDB.Bench.maintenancePoolConns", 10)
		c.Set("JobsDB.Bench.datasets", 20)
		c.Set("JobsDB.Bench.jobsPerDataset", 100000)
		c.Set("JobsDB.Bench.payloadSize", 3*bytesize.KB)
		c.Set("JobsDB.Bench.destinations", 100)
		c.Set("JobsDB.Bench.batchSize", 1000)
		c.Set("JobsDB.Bench.failProbability", 0.5)
		c.Set("JobsDB.Bench.maxFailures", 3)
		c.Set("JobsDB.Bench.seedConcurrency", 4)
		c.Set("JobsDB.Bench.seedBatchSize", 10000)

		l := logger.NewFactory(c)
		stat := stats.NewStats(c, l, statsmetric.Instance)
		b, err := bench.New(c, stat, l.NewLogger(), postgresContainer.DB)
		require.NoError(t, err)

		require.NoError(t, b.Run(context.Background()))
	})

	t.Run("two_stage", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t, postgres.WithOptions(
			"max_connections=200",
			"max_wal_size=2GB",
			"checkpoint_timeout=30",
			"shared_buffers=512MB",
			"work_mem=64MB",
			"hash_mem_multiplier=4",
			"maintenance_work_mem=200MB",
			"effective_cache_size=4GB",
			"wal_buffers=64MB",
			"random_page_cost=1.1",
			"autovacuum_vacuum_cost_delay=1",
			"autovacuum_naptime=20",
			"checkpoint_warning=0",
		),
			postgres.WithTag("17-alpine"),
			postgres.WithShmSize(256*bytesize.MB),
		)
		require.NoError(t, err)
		postgresContainer.DB.SetMaxOpenConns(60)
		postgresContainer.DB.SetMaxIdleConns(20)

		c := config.New()

		// JobsDB configuration
		c.Set("JobsDB.enableWriterQueue", true) // default: true
		c.Set("JobsDB.maxWriters", 4)           // default: 3
		c.Set("JobsDB.enableReaderQueue", true) //	default: true
		c.Set("JobsDB.maxReaders", 8)           // default: 6
		c.Set("JobsDB.enableToastOptimizations", false)
		c.Set("JobsDB.refreshDSListLoopSleepDuration", 1*time.Second)

		// Bench configuration
		c.Set("JobsDB.Bench.scenario", "two_stage")
		c.Set("JobsDB.Bench.payloadSize", 2*bytesize.KB) // default: 1KB
		c.Set("JobsDB.Bench.noOfSources", 15)            // default: 10
		c.Set("JobsDB.Bench.writerConcurrency", 4)
		c.Set("JobsDB.Bench.updateConcurrency", 1)
		c.Set("JobsDB.Bench.writerBatchSize", 10)
		c.Set("JobsDB.Bench.readerReadSize", 20000)
		c.Set("JobsDB.Bench.payloadLimit", 100*bytesize.MB)
		c.Set("JobsDB.Bench.insertRateLimit", 1000)

		l := logger.NewFactory(c)
		stat := stats.NewStats(c, l, statsmetric.Instance)
		b, err := bench.New(c, stat, l.NewLogger(), postgresContainer.DB)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled")
			case <-time.After(3 * time.Second):
				cancel()
				return nil
			}
		})
		g.Go(func() error {
			return b.Run(ctx)
		})
		require.NoError(t, g.Wait())
	})
}

package bench_test

import (
	"context"
	"fmt"
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
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/bench"
)

func TestBench(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t, postgres.WithOptions(
		"max_connections=200",
		"shared_buffers=800MB",
		"effective_cache_size=2GB",
		"work_mem=192MB",
		"wal_buffers=26MB",
		"effective_io_concurrency=100",
		"random_page_cost=1",
		"max_wal_size=30GB",
	),
		postgres.WithTag("17-alpine"),
		postgres.WithShmSize(256*bytesize.MB),
	)
	require.NoError(t, err)
	postgresContainer.DB.SetMaxOpenConns(60)
	postgresContainer.DB.SetMaxIdleConns(20)

	c := config.New()

	c.Set("JobsDB.enableWriterQueue", true) // default: true
	c.Set("JobsDB.maxWriters", 4)           // default: 3

	c.Set("JobsDB.enableReaderQueue", true) //	default: true
	c.Set("JobsDB.maxReaders", 8)           // default: 6

	c.Set("JobsDB.payloadColumnType", string(jobsdb.TEXT))
	jobsdb.GZIP = false
	jobsdb.LZ4 = false
	c.Set("JobsDB.enableToastOptimizations", false)

	c.Set("JobsDB.refreshDSListLoopSleepDuration", 1*time.Second)

	c.Set("JobsDB.Bench.payloadSize", 2*bytesize.KB) // default: 1KB
	c.Set("JobsDB.Bench.noOfSources", 15)            // default: 10
	c.Set("JobsDB.Bench.writerConcurrency", 4)
	c.Set("JobsDB.Bench.updateConcurrency", 1)
	c.Set("JobsDB.Bench.writerBatchSize", 5000)
	c.Set("JobsDB.Bench.readerReadSize", 20000)
	c.Set("JobsDB.Bench.payloadLimit", 100*bytesize.MB)

	if false {
		c.Set("JobsDB.payloadColumnType", string(jobsdb.TEXT))
		c.Set("JobsDB.enableToastOptimizations", true)
		c.Set("JobsDB.refreshDSListLoopSleepDuration", 1*time.Second)
	}

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
		case <-time.After(5 * time.Second):
			cancel()
			return nil
		}
	})
	g.Go(func() error {
		return b.Run(ctx)
	})
	require.NoError(t, g.Wait())
}

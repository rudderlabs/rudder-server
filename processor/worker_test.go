package processor

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kit_sync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	run := func(t *testing.T, pipelining, limitsReached bool) {
		wh := &mockWorkerHandle{
			pipelining: pipelining,
			log:        logger.NOP,
			loopEvents: 100,
			partitionStats: map[string]struct {
				queried     int
				marked      int
				processed   int
				transformed int
				stored      int
			}{},
			limitsReached: limitsReached,
		}

		ctx, cancel := context.WithCancel(context.Background())
		poolCtx, poolCancel := context.WithCancel(context.Background())

		if pipelining {
			var limiterWg sync.WaitGroup
			wh.limiters.query = kit_sync.NewLimiter(poolCtx, &limiterWg, "query", 2, stats.Default)
			wh.limiters.process = kit_sync.NewLimiter(poolCtx, &limiterWg, "process", 2, stats.Default)
			wh.limiters.store = kit_sync.NewLimiter(poolCtx, &limiterWg, "store", 2, stats.Default)
			wh.limiters.transform = kit_sync.NewLimiter(poolCtx, &limiterWg, "transform", 2, stats.Default)
			defer limiterWg.Wait()
		}

		defer poolCancel()

		// create a worker pool
		wp := workerpool.New(poolCtx, func(partition string) workerpool.Worker { return newProcessorWorker(partition, wh) }, logger.NOP)

		// start pinging for work for 100 partitions
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			partition := "p-" + strconv.Itoa(i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Millisecond):
						wp.PingWorker(partition)
					}
				}
			}()
		}
		// stop pinging after 5 seconds
		time.Sleep(5 * time.Second)
		cancel()
		wg.Wait()

		// wait for all workers to finish
		wp.Shutdown()

		// validate that all jobs were processed
		wh.validate(t)
	}

	t.Run("work without pipelining", func(t *testing.T) {
		t.Run("limits not reached", func(t *testing.T) {
			run(t, false, false)
		})
		t.Run("limits reached", func(t *testing.T) {
			run(t, false, true)
		})
	})

	t.Run("work with pipelining", func(t *testing.T) {
		t.Run("limits not reached", func(t *testing.T) {
			run(t, true, false)
		})
		t.Run("limits reached", func(t *testing.T) {
			run(t, true, true)
		})
	})
}

func TestWorkerPoolIdle(t *testing.T) {
	wh := &mockWorkerHandle{
		pipelining: true,
		log:        logger.NewLogger(),
		loopEvents: 0,
		partitionStats: map[string]struct {
			queried     int
			marked      int
			processed   int
			transformed int
			stored      int
		}{},
	}
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	// create a worker pool
	wp := workerpool.New(poolCtx,
		func(partition string) workerpool.Worker { return newProcessorWorker(partition, wh) },
		logger.NOP,
		workerpool.WithCleanupPeriod(200*time.Millisecond),
		workerpool.WithIdleTimeout(200*time.Millisecond))

	require.Equal(t, 0, wp.Size())

	// start pinging for work for 1 partition
	wp.PingWorker("p-1")

	require.Equal(t, 1, wp.Size())

	require.Eventually(t, func() bool {
		return wp.Size() == 0
	}, 2*time.Second, 10*time.Millisecond, "worker pool should be emptyied since worker will be idle (no jobs to process)")

	wp.Shutdown()
}

type mockWorkerHandle struct {
	pipelining     bool
	loopEvents     int
	statsMu        sync.RWMutex
	log            logger.Logger
	partitionStats map[string]struct {
		queried     int
		marked      int
		processed   int
		transformed int
		stored      int
	}

	limiters struct {
		query     kit_sync.Limiter
		process   kit_sync.Limiter
		transform kit_sync.Limiter
		store     kit_sync.Limiter
	}

	limitsReached bool
}

func (m *mockWorkerHandle) validate(t *testing.T) {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	for partition, s := range m.partitionStats {
		require.Equalf(t, s.queried, s.marked, "Partition %s: Queried %d, Marked %d", partition, s.queried, s.marked)
		require.Equalf(t, s.marked, s.processed, "Partition %s: Marked %d, Processed %d", partition, s.queried, s.marked)
		require.Equalf(t, s.processed, s.transformed, "Partition %s: Processed %d, Transformed %d", partition, s.queried, s.marked)
		require.Equalf(t, s.transformed, s.stored, "Partition %s: Transformed %d, Stored %d", partition, s.queried, s.marked)
	}
}

func (m *mockWorkerHandle) logger() logger.Logger {
	return m.log
}

func (m *mockWorkerHandle) config() workerHandleConfig {
	return workerHandleConfig{
		enablePipelining:      m.pipelining,
		maxEventsToProcess:    m.loopEvents,
		pipelineBufferedItems: 1,
		subJobSize:            10,
		readLoopSleep:         1 * time.Millisecond,
		maxLoopSleep:          100 * time.Millisecond,
	}
}

func (*mockWorkerHandle) rsourcesService() rsources.JobService {
	return nil
}

func (m *mockWorkerHandle) handlePendingGatewayJobs(key string) bool {
	jobs := m.getJobs(key)
	if len(jobs.Jobs) > 0 {
		_ = m.markExecuting(jobs.Jobs)
	}
	rsourcesStats := rsources.NewStatsCollector(m.rsourcesService())
	for _, subJob := range m.jobSplitter(jobs.Jobs, rsourcesStats) {
		m.Store(key,
			m.transformations(key,
				m.processJobsForDest(key, subJob),
			),
		)
	}
	return len(jobs.Jobs) > 0
}

func (*mockWorkerHandle) stats() *processorStats {
	return &processorStats{
		DBReadThroughput: stats.Default.NewStat("db_read_throughput", stats.CountType),
	}
}

func (m *mockWorkerHandle) getJobs(partition string) jobsdb.JobsResult {
	if m.limiters.query != nil {
		defer m.limiters.query.Begin(partition)()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.queried += m.loopEvents
	m.partitionStats[partition] = s

	m.log.Infof("getJobs partition: %s stats: %+v", partition, s)

	var jobs []*jobsdb.JobT
	for i := 0; i < m.loopEvents; i++ {
		jobs = append(jobs, &jobsdb.JobT{
			CustomVal: partition,
		})
	}
	return jobsdb.JobsResult{
		Jobs:          jobs,
		EventsCount:   m.loopEvents,
		LimitsReached: m.limitsReached,
	}
}

func (m *mockWorkerHandle) markExecuting(jobs []*jobsdb.JobT) error {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	partition := jobs[0].CustomVal
	s := m.partitionStats[partition]
	s.marked += len(jobs)
	m.partitionStats[partition] = s
	m.log.Infof("markExecuting partition: %s stats: %+v", partition, s)

	return nil
}

func (*mockWorkerHandle) jobSplitter(jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob {
	return []subJob{
		{
			subJobs:       jobs,
			hasMore:       false,
			rsourcesStats: rsourcesStats,
		},
	}
}

func (m *mockWorkerHandle) processJobsForDest(partition string, subJobs subJob) *transformationMessage {
	if m.limiters.process != nil {
		defer m.limiters.process.Begin(partition)()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.processed += len(subJobs.subJobs)
	m.partitionStats[partition] = s
	m.log.Infof("processJobsForDest partition: %s stats: %+v", partition, s)

	return &transformationMessage{
		totalEvents: len(subJobs.subJobs),
	}
}

func (m *mockWorkerHandle) transformations(partition string, in *transformationMessage) *storeMessage {
	if m.limiters.transform != nil {
		defer m.limiters.transform.Begin(partition)()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.transformed += in.totalEvents
	m.partitionStats[partition] = s
	m.log.Infof("transformations partition: %s stats: %+v", partition, s)

	return &storeMessage{
		totalEvents: in.totalEvents,
	}
}

func (m *mockWorkerHandle) Store(partition string, in *storeMessage) {
	if m.limiters.store != nil {
		defer m.limiters.store.Begin(partition)()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.stored += in.totalEvents
	m.partitionStats[partition] = s
	m.log.Infof("Store partition: %s stats: %+v", partition, s)
}

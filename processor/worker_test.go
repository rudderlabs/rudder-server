package processor

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

func TestWorkerPool(t *testing.T) {
	run := func(t *testing.T, pipelining, limitsReached, shouldProcessMultipleSubJobs bool) {
		wh := &mockWorkerHandle{
			pipelining: pipelining,
			log:        logger.NOP,
			loopEvents: 100,
			partitionStats: map[string]struct {
				queried      int
				marked       int
				processed    int
				transformed  int
				stored       int
				subBatches   int
				trackedUsers int
			}{},
			limitsReached:                limitsReached,
			shouldProcessMultipleSubJobs: shouldProcessMultipleSubJobs,
		}

		ctx, cancel := context.WithCancel(context.Background())
		poolCtx, poolCancel := context.WithCancel(context.Background())

		if pipelining {
			var limiterWg sync.WaitGroup
			wh.limiters.query = kitsync.NewLimiter(poolCtx, &limiterWg, "query", 2, stats.Default)
			wh.limiters.process = kitsync.NewLimiter(poolCtx, &limiterWg, "process", 2, stats.Default)
			wh.limiters.store = kitsync.NewLimiter(poolCtx, &limiterWg, "store", 2, stats.Default)
			wh.limiters.transform = kitsync.NewLimiter(poolCtx, &limiterWg, "transform", 2, stats.Default)
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
			run(t, false, false, false)
		})
		t.Run("limits reached", func(t *testing.T) {
			run(t, false, true, false)
		})
	})

	t.Run("work with pipelining", func(t *testing.T) {
		t.Run("limits not reached", func(t *testing.T) {
			run(t, true, false, false)
		})
		t.Run("limits reached", func(t *testing.T) {
			run(t, true, true, false)
		})
		t.Run("limits reached with multiple sub jobs", func(t *testing.T) {
			run(t, true, true, true)
		})
		t.Run("limits not reached with multiple sub jobs", func(t *testing.T) {
			run(t, true, false, true)
		})
	})
}

func TestWorkerPoolIdle(t *testing.T) {
	wh := &mockWorkerHandle{
		pipelining: true,
		log:        logger.NewLogger(),
		loopEvents: 0,
		partitionStats: map[string]struct {
			queried      int
			marked       int
			processed    int
			transformed  int
			stored       int
			subBatches   int
			trackedUsers int
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
		queried      int
		marked       int
		processed    int
		transformed  int
		stored       int
		subBatches   int
		trackedUsers int
	}

	limiters struct {
		query     kitsync.Limiter
		process   kitsync.Limiter
		transform kitsync.Limiter
		store     kitsync.Limiter
	}

	limitsReached                bool
	shouldProcessMultipleSubJobs bool
}

func (m *mockWorkerHandle) tracer() stats.Tracer {
	return stats.NOP.NewTracer("")
}

func (m *mockWorkerHandle) validate(t *testing.T) {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	for partition, s := range m.partitionStats {
		require.Equalf(t, s.queried, s.marked, "Partition %s: Queried %d, Marked %d", partition, s.queried, s.marked)
		require.Equalf(t, s.marked, s.processed, "Partition %s: Marked %d, Processed %d", partition, s.queried, s.marked)
		require.Equalf(t, s.processed, s.transformed, "Partition %s: Processed %d, Transformed %d", partition, s.queried, s.marked)
		require.Equalf(t, s.transformed, s.stored, "Partition %s: Transformed %d, Stored %d", partition, s.queried, s.marked)
		require.Equalf(t, s.subBatches, s.trackedUsers, "Partition %s: Tracked Users %d, Subjobs %d", partition, s.trackedUsers, s.subBatches)
	}
}

func (m *mockWorkerHandle) logger() logger.Logger {
	return m.log
}

func (m *mockWorkerHandle) config() workerHandleConfig {
	return workerHandleConfig{
		enablePipelining:      m.pipelining,
		maxEventsToProcess:    config.SingleValueLoader(m.loopEvents),
		pipelineBufferedItems: 1,
		subJobSize:            10,
		readLoopSleep:         config.SingleValueLoader(1 * time.Millisecond),
		maxLoopSleep:          config.SingleValueLoader(100 * time.Millisecond),
	}
}

func (*mockWorkerHandle) rsourcesService() rsources.JobService {
	return nil
}

func (m *mockWorkerHandle) handlePendingGatewayJobs(partition string) bool {
	jobs := m.getJobs(partition)
	if len(jobs.Jobs) > 0 {
		_ = m.markExecuting(partition, jobs.Jobs)
	}
	rsourcesStats := rsources.NewStatsCollector(m.rsourcesService(), rsources.IgnoreDestinationID())
	for _, subJob := range m.jobSplitter(jobs.Jobs, rsourcesStats) {
		var dest *transformationMessage
		var err error
		preTransMessage, err := m.processJobsForDest(partition, subJob)
		if err != nil {
			return false
		}
		dest, err = m.generateTransformationMessage(preTransMessage)
		if err != nil {
			return false
		}
		m.Store(partition, m.transformations(partition,
			dest,
		))
	}
	return len(jobs.Jobs) > 0
}

func (*mockWorkerHandle) stats() *processorStats {
	return &processorStats{
		DBReadThroughput: func(partition string) stats.Measurement {
			return stats.Default.NewStat("db_read_throughput", stats.CountType)
		},
		statDBReadOutOfOrder: func(partition string) stats.Measurement {
			return stats.NOP.NewStat("db_read_out_of_order", stats.CountType)
		},
		statDBReadOutOfSequence: func(partition string) stats.Measurement {
			return stats.NOP.NewStat("db_read_out_of_sequence", stats.CountType)
		},
	}
}

func (m *mockWorkerHandle) getJobs(partition string) jobsdb.JobsResult {
	if m.limiters.query != nil {
		defer m.limiters.query.Begin("")()
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

func (m *mockWorkerHandle) markExecuting(partition string, jobs []*jobsdb.JobT) error {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.marked += len(jobs)
	m.partitionStats[partition] = s
	m.log.Infof("markExecuting partition: %s stats: %+v", partition, s)

	return nil
}

func (m *mockWorkerHandle) jobSplitter(jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob {
	if !m.shouldProcessMultipleSubJobs {
		return []subJob{
			{
				subJobs:       jobs,
				hasMore:       false,
				rsourcesStats: rsourcesStats,
			},
		}
	}
	return []subJob{
		{
			subJobs:       jobs[0 : len(jobs)/3],
			hasMore:       true,
			rsourcesStats: rsourcesStats,
		},
		{
			subJobs:       jobs[len(jobs)/3 : 2*len(jobs)/2],
			hasMore:       true,
			rsourcesStats: rsourcesStats,
		},
		{
			subJobs:       jobs[2*len(jobs)/2:],
			hasMore:       false,
			rsourcesStats: rsourcesStats,
		},
	}
}

func (m *mockWorkerHandle) processJobsForDest(partition string, subJobs subJob) (*preTransformationMessage, error) {
	if m.limiters.process != nil {
		defer m.limiters.process.Begin("")()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.processed += len(subJobs.subJobs)
	s.subBatches += 1
	m.partitionStats[partition] = s
	m.log.Infof("processJobsForDest partition: %s stats: %+v", partition, s)

	return &preTransformationMessage{
		totalEvents: len(subJobs.subJobs),
		subJobs:     subJobs,
	}, nil
}

func (m *mockWorkerHandle) generateTransformationMessage(in *preTransformationMessage) (*transformationMessage, error) {
	return &transformationMessage{
		totalEvents: in.totalEvents,
		hasMore:     in.subJobs.hasMore,
		trackedUsersReports: []*trackedusers.UsersReport{
			{WorkspaceID: sampleWorkspaceID},
		},
	}, nil
}

func (m *mockWorkerHandle) transformations(partition string, in *transformationMessage) *storeMessage {
	if m.limiters.transform != nil {
		defer m.limiters.transform.Begin("")()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.transformed += in.totalEvents
	m.partitionStats[partition] = s
	m.log.Infof("transformations partition: %s stats: %+v", partition, s)

	return &storeMessage{
		totalEvents:         in.totalEvents,
		hasMore:             in.hasMore,
		trackedUsersReports: in.trackedUsersReports,
	}
}

func (m *mockWorkerHandle) Store(partition string, in *storeMessage) {
	if m.limiters.store != nil {
		defer m.limiters.store.Begin("")()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.stored += in.totalEvents
	s.trackedUsers += len(in.trackedUsersReports)
	m.partitionStats[partition] = s
	m.log.Infof("Store partition: %s stats: %+v", partition, s)
}

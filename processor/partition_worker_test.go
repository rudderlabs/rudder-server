package processor

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"
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
				queried              int
				marked               int
				processed            int
				userTransform        int
				destinationTransform int
				stored               int
				subBatches           int
				trackedUsers         int
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
			wh.limiters.usertransform = kitsync.NewLimiter(poolCtx, &limiterWg, "usertransform", 2, stats.Default)
			wh.limiters.destinationtransform = kitsync.NewLimiter(poolCtx, &limiterWg, "destinationtransform", 2, stats.Default)
			defer limiterWg.Wait()
		}

		defer poolCancel()

		// create a worker pool
		wp := workerpool.New(poolCtx, func(partition string) workerpool.Worker {
			return newPartitionWorker(partition, wh, stats.NOP.NewTracer(""))
		}, logger.NOP)

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
			queried              int
			marked               int
			processed            int
			userTransform        int
			destinationTransform int
			stored               int
			subBatches           int
			trackedUsers         int
		}{},
	}
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	// create a worker pool
	wp := workerpool.New(poolCtx,
		func(partition string) workerpool.Worker {
			return newPartitionWorker(partition, wh, stats.NOP.NewTracer(""))
		},
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
		queried              int
		marked               int
		processed            int
		userTransform        int
		destinationTransform int
		stored               int
		subBatches           int
		trackedUsers         int
	}

	limiters struct {
		query                kitsync.Limiter
		process              kitsync.Limiter
		store                kitsync.Limiter
		usertransform        kitsync.Limiter
		destinationtransform kitsync.Limiter
	}

	limitsReached                bool
	shouldProcessMultipleSubJobs bool
}

func (m *mockWorkerHandle) validate(t *testing.T) {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	for partition, s := range m.partitionStats {
		require.Equalf(t, s.queried, s.marked, "Partition %s: Queried %d, Marked %d", partition, s.queried, s.marked)
		require.Equalf(t, s.marked, s.processed, "Partition %s: Marked %d, Processed %d", partition, s.queried, s.marked)
		require.Equalf(t, s.processed, s.userTransform, "Partition %s: Processed %d, User Transform %d", partition, s.queried, s.marked)
		require.Equalf(t, s.userTransform, s.destinationTransform, "Partition %s: User Transform %d, Destination Transform %d", partition, s.queried, s.marked)
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
		pipelinesPerPartition: 3,
	}
}

func (*mockWorkerHandle) rsourcesService() rsources.JobService {
	return nil
}

func (m *mockWorkerHandle) handlePendingGatewayJobs(partition string) bool {
	ctx := context.Background()
	jobs := m.getJobsStage(ctx, partition)
	if len(jobs.Jobs) > 0 {
		_ = m.markExecuting(ctx, partition, jobs.Jobs)
	}
	rsourcesStats := rsources.NewStatsCollector(m.rsourcesService(), rsources.IgnoreDestinationID())
	for _, subJob := range m.jobSplitter(ctx, jobs.Jobs, rsourcesStats) {
		var dest *transformationMessage
		var err error
		preTransMessage, err := m.preprocessStage(partition, subJob)
		if err != nil {
			return false
		}
		dest, err = m.pretransformStage(partition, preTransMessage)
		if err != nil {
			return false
		}
		m.storeStage(partition, m.destinationTransformStage(partition,
			m.userTransformStage(partition, dest),
		))
	}
	return len(jobs.Jobs) > 0
}

func (*mockWorkerHandle) stats() *processorStats {
	return &processorStats{}
}

func (m *mockWorkerHandle) getJobsStage(_ context.Context, partition string) jobsdb.JobsResult {
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

func (m *mockWorkerHandle) markExecuting(_ context.Context, partition string, jobs []*jobsdb.JobT) error {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.marked += len(jobs)
	m.partitionStats[partition] = s
	m.log.Infof("markExecuting partition: %s stats: %+v", partition, s)

	return nil
}

func (m *mockWorkerHandle) jobSplitter(ctx context.Context, jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob {
	if !m.shouldProcessMultipleSubJobs {
		return []subJob{
			{
				ctx:           ctx,
				subJobs:       jobs,
				hasMore:       false,
				rsourcesStats: rsourcesStats,
			},
		}
	}
	return []subJob{
		{
			ctx:           ctx,
			subJobs:       jobs[0 : len(jobs)/3],
			hasMore:       true,
			rsourcesStats: rsourcesStats,
		},
		{
			ctx:           ctx,
			subJobs:       jobs[len(jobs)/3 : 2*len(jobs)/2],
			hasMore:       true,
			rsourcesStats: rsourcesStats,
		},
		{
			ctx:           ctx,
			subJobs:       jobs[2*len(jobs)/2:],
			hasMore:       false,
			rsourcesStats: rsourcesStats,
		},
	}
}

func (m *mockWorkerHandle) preprocessStage(partition string, subJobs subJob) (*preTransformationMessage, error) {
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

func (m *mockWorkerHandle) pretransformStage(_ string, in *preTransformationMessage) (*transformationMessage, error) {
	return &transformationMessage{
		ctx:         in.subJobs.ctx,
		totalEvents: in.totalEvents,
		hasMore:     in.subJobs.hasMore,
		trackedUsersReports: []*trackedusers.UsersReport{
			{WorkspaceID: sampleWorkspaceID},
		},
	}, nil
}

func (m *mockWorkerHandle) userTransformStage(partition string, in *transformationMessage) *userTransformData {
	if m.limiters.usertransform != nil {
		defer m.limiters.usertransform.Begin("")()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.userTransform += in.totalEvents
	m.partitionStats[partition] = s
	m.log.Infof("usertransformations partition: %s stats: %+v", partition, s)

	return &userTransformData{
		ctx:                 in.ctx,
		totalEvents:         in.totalEvents,
		hasMore:             in.hasMore,
		trackedUsersReports: in.trackedUsersReports,
	}
}

func (m *mockWorkerHandle) destinationTransformStage(partition string, in *userTransformData) *storeMessage {
	if m.limiters.destinationtransform != nil {
		defer m.limiters.destinationtransform.Begin("")()
	}
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	s.destinationTransform += in.totalEvents
	m.partitionStats[partition] = s
	m.log.Infof("destinationtransformations partition: %s stats: %+v", partition, s)

	return &storeMessage{
		ctx:                 context.Background(),
		totalEvents:         in.totalEvents,
		hasMore:             in.hasMore,
		trackedUsersReports: in.trackedUsersReports,
	}
}

func (m *mockWorkerHandle) storeStage(partition string, in *storeMessage) {
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

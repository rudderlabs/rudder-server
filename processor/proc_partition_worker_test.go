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

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// TestProcWorkerPool drives the proc pool (procPartitionWorker + procPipelineWorker)
// through the workerpool with a mock handle, asserting event-count conservation across
// the 4 proc stages (rebuild → userTransform → destinationTransform → store) for every
// partition (destination), under pipelining, limits-reached and multi-subjob merging.
func TestProcWorkerPool(t *testing.T) {
	run := func(t *testing.T, limitsReached, shouldProcessMultipleSubJobs bool) {
		wh := &mockProcWorkerHandle{
			log:                          logger.NOP,
			loopEvents:                   99, // divisible by 3 for the multi-subjob split
			partitionStats:               map[string]procPartitionStat{},
			limitsReached:                limitsReached,
			shouldProcessMultipleSubJobs: shouldProcessMultipleSubJobs,
		}

		ctx, cancel := context.WithCancel(context.Background())
		poolCtx, poolCancel := context.WithCancel(context.Background())

		var limiterWg sync.WaitGroup
		wh.limiters.pread = kitsync.NewLimiter(poolCtx, &limiterWg, "pread", 2, stats.Default)
		wh.limiters.rebuild = kitsync.NewLimiter(poolCtx, &limiterWg, "rebuild", 2, stats.Default)
		wh.limiters.usertransform = kitsync.NewLimiter(poolCtx, &limiterWg, "usertransform", 2, stats.Default)
		wh.limiters.destinationtransform = kitsync.NewLimiter(poolCtx, &limiterWg, "destinationtransform", 2, stats.Default)
		wh.limiters.store = kitsync.NewLimiter(poolCtx, &limiterWg, "store", 2, stats.Default)
		defer limiterWg.Wait()
		defer poolCancel()

		wp := workerpool.New(poolCtx, func(partition string) workerpool.Worker {
			return newProcPartitionWorker(partition, wh, stats.NOP.NewTracer(""), stats.NOP)
		}, logger.NOP)

		// ping for work for 20 destinations
		var wg sync.WaitGroup
		for i := range 20 {
			destinationID := "dest-" + strconv.Itoa(i)
			wg.Go(func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Millisecond):
						wp.PingWorker(destinationID)
					}
				}
			})
		}
		time.Sleep(2 * time.Second)
		cancel()
		wg.Wait()

		wp.Shutdown()

		wh.validate(t)
	}

	t.Run("limits not reached", func(t *testing.T) { run(t, false, false) })
	t.Run("limits reached", func(t *testing.T) { run(t, true, false) })
	t.Run("limits reached with multiple sub jobs", func(t *testing.T) { run(t, true, true) })
	t.Run("limits not reached with multiple sub jobs", func(t *testing.T) { run(t, false, true) })
}

// TestProcWorkerPoolIdle asserts that a proc worker for a drained destination (no jobs)
// is cleaned up by the pool.
func TestProcWorkerPoolIdle(t *testing.T) {
	wh := &mockProcWorkerHandle{
		log:            logger.NOP,
		loopEvents:     0,
		partitionStats: map[string]procPartitionStat{},
	}
	wp := workerpool.New(t.Context(),
		func(partition string) workerpool.Worker {
			return newProcPartitionWorker(partition, wh, stats.NOP.NewTracer(""), stats.NOP)
		},
		logger.NOP,
		workerpool.WithCleanupPeriod(200*time.Millisecond),
		workerpool.WithIdleTimeout(200*time.Millisecond))

	require.Equal(t, 0, wp.Size())
	wp.PingWorker("dest-1")
	require.Equal(t, 1, wp.Size())
	require.Eventually(t, func() bool {
		return wp.Size() == 0
	}, 2*time.Second, 10*time.Millisecond, "idle worker (no jobs) should be cleaned up")
	wp.Shutdown()
}

type procPartitionStat struct {
	queried              int
	marked               int
	rebuilt              int
	userTransform        int
	destinationTransform int
	stored               int
	subBatches           int
}

type mockProcWorkerHandle struct {
	loopEvents     int
	statsMu        sync.RWMutex
	log            logger.Logger
	partitionStats map[string]procPartitionStat

	limiters struct {
		pread                kitsync.Limiter
		rebuild              kitsync.Limiter
		usertransform        kitsync.Limiter
		destinationtransform kitsync.Limiter
		store                kitsync.Limiter
	}

	limitsReached                bool
	shouldProcessMultipleSubJobs bool
}

// validate asserts per-destination that no events were lost between any two stages.
func (m *mockProcWorkerHandle) validate(t *testing.T) {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()
	require.NotEmpty(t, m.partitionStats, "expected at least one destination to have been processed")
	for destinationID, s := range m.partitionStats {
		require.Positivef(t, s.queried, "dest %s: nothing queried", destinationID)
		require.Equalf(t, s.queried, s.marked, "dest %s: queried %d != marked %d", destinationID, s.queried, s.marked)
		require.Equalf(t, s.marked, s.rebuilt, "dest %s: marked %d != rebuilt %d", destinationID, s.marked, s.rebuilt)
		require.Equalf(t, s.rebuilt, s.userTransform, "dest %s: rebuilt %d != userTransform %d", destinationID, s.rebuilt, s.userTransform)
		require.Equalf(t, s.userTransform, s.destinationTransform, "dest %s: userTransform %d != destinationTransform %d", destinationID, s.userTransform, s.destinationTransform)
		require.Equalf(t, s.destinationTransform, s.stored, "dest %s: destinationTransform %d != stored %d", destinationID, s.destinationTransform, s.stored)
	}
}

func (m *mockProcWorkerHandle) update(partition string, f func(s *procPartitionStat)) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	s := m.partitionStats[partition]
	f(&s)
	m.partitionStats[partition] = s
}

func (m *mockProcWorkerHandle) logger() logger.Logger { return m.log }

func (m *mockProcWorkerHandle) config() workerHandleConfig {
	return workerHandleConfig{
		enablePipelining:      true,
		maxEventsToProcess:    config.SingleValueLoader(m.loopEvents),
		pipelineBufferedItems: 1,
		subJobSize:            10,
		readLoopSleep:         config.SingleValueLoader(1 * time.Millisecond),
		maxLoopSleep:          config.SingleValueLoader(100 * time.Millisecond),
		pipelinesPerPartition: 3,
		partitionProcessingDelay: func(string) config.ValueLoader[time.Duration] {
			return config.SingleValueLoader(0 * time.Millisecond)
		},
	}
}

func (*mockProcWorkerHandle) rsourcesService() rsources.JobService { return nil }
func (*mockProcWorkerHandle) stats() *processorStats               { return &processorStats{} }

func (m *mockProcWorkerHandle) getProcJobs(_ context.Context, destinationID string) jobsdb.JobsResult {
	if m.limiters.pread != nil {
		defer m.limiters.pread.Begin("")()
	}
	m.update(destinationID, func(s *procPartitionStat) { s.queried += m.loopEvents })

	jobs := make([]*jobsdb.JobT, 0, m.loopEvents)
	for i := 0; i < m.loopEvents; i++ {
		jobs = append(jobs, &jobsdb.JobT{
			UserID:    strconv.Itoa(i), // spread across pipelines
			CustomVal: destinationID,
		})
	}
	return jobsdb.JobsResult{Jobs: jobs, EventsCount: m.loopEvents, LimitsReached: m.limitsReached}
}

func (m *mockProcWorkerHandle) procMarkExecuting(_ context.Context, destinationID string, jobs []*jobsdb.JobT) error {
	m.update(destinationID, func(s *procPartitionStat) { s.marked += len(jobs) })
	return nil
}

func (m *mockProcWorkerHandle) jobSplitter(ctx context.Context, jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob {
	if !m.shouldProcessMultipleSubJobs || len(jobs) < 3 {
		return []subJob{{ctx: ctx, subJobs: jobs, hasMore: false, rsourcesStats: rsourcesStats}}
	}
	third := len(jobs) / 3
	return []subJob{
		{ctx: ctx, subJobs: jobs[:third], hasMore: true, rsourcesStats: rsourcesStats},
		{ctx: ctx, subJobs: jobs[third : 2*third], hasMore: true, rsourcesStats: rsourcesStats},
		{ctx: ctx, subJobs: jobs[2*third:], hasMore: false, rsourcesStats: rsourcesStats},
	}
}

func (m *mockProcWorkerHandle) procRebuildStage(destinationID string, in subJob) (*transformationMessage, error) {
	if m.limiters.rebuild != nil {
		defer m.limiters.rebuild.Begin("")()
	}
	m.update(destinationID, func(s *procPartitionStat) {
		s.rebuilt += len(in.subJobs)
		s.subBatches++
	})
	return &transformationMessage{ctx: in.ctx, totalEvents: len(in.subJobs), hasMore: in.hasMore}, nil
}

func (m *mockProcWorkerHandle) userTransformStage(partition string, in *transformationMessage) *userTransformData {
	if m.limiters.usertransform != nil {
		defer m.limiters.usertransform.Begin("")()
	}
	m.update(partition, func(s *procPartitionStat) { s.userTransform += in.totalEvents })
	return &userTransformData{ctx: in.ctx, totalEvents: in.totalEvents, hasMore: in.hasMore}
}

func (m *mockProcWorkerHandle) destinationTransformStage(partition string, in *userTransformData) *storeMessage {
	if m.limiters.destinationtransform != nil {
		defer m.limiters.destinationtransform.Begin("")()
	}
	m.update(partition, func(s *procPartitionStat) { s.destinationTransform += in.totalEvents })
	return &storeMessage{ctx: in.ctx, totalEvents: in.totalEvents, hasMore: in.hasMore}
}

func (m *mockProcWorkerHandle) procStoreStage(partition string, _ int, in *storeMessage) {
	if m.limiters.store != nil {
		defer m.limiters.store.Begin("")()
	}
	m.update(partition, func(s *procPartitionStat) { s.stored += in.totalEvents })
}

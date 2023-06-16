package workerpool

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestWorkerPool(t *testing.T) {
	var workersMu sync.Mutex
	var workers []*mockWorker

	ctx, cancel := context.WithCancel(context.Background())
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	// create a worker pool
	wp := New(poolCtx,
		func(partition string) Worker {
			workersMu.Lock()
			defer workersMu.Unlock()
			w := &mockWorker{
				partition: partition,
				idleAfter: 100 * time.Millisecond,
			}
			workers = append(workers, w)
			return w
		},
		logger.NOP)

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
	require.Len(t, workers, 100)
	for _, w := range workers {
		require.True(t, w.stopped)
	}
}

func TestWorkerPoolIdle(t *testing.T) {
	poolCtx, poolCancel := context.WithCancel(context.Background())
	defer poolCancel()

	// create a worker pool
	wp := New(poolCtx,
		func(partition string) Worker {
			return &mockWorker{
				partition: partition,
				idleAfter: 100 * time.Millisecond,
			}
		},
		logger.NOP,
		WithCleanupPeriod(100*time.Millisecond),
		WithIdleTimeout(100*time.Millisecond))

	require.Equal(t, 0, wp.Size())

	// start pinging for work for 1 partition
	wp.PingWorker("p-1")

	require.Equal(t, 1, wp.Size())

	require.Eventually(t, func() bool {
		return wp.Size() == 0
	}, 10*time.Second, 10*time.Millisecond, "worker pool should be emptyied since worker will be idle (no jobs to process)")

	wp.Shutdown()
}

type mockWorker struct {
	partition string
	idleAfter time.Duration
	firstPing time.Time
	lastPing  time.Time
	pings     int
	stopped   bool
}

// Ping triggers the worker to pick more jobs
func (mw *mockWorker) Work() bool {
	if mw.firstPing.IsZero() {
		mw.firstPing = time.Now()
	}
	mw.lastPing = time.Now()
	mw.pings++
	return mw.firstPing.Add(mw.idleAfter).Before(time.Now())
}

func (mw *mockWorker) SleepDurations() (min, max time.Duration) {
	return mw.idleAfter, mw.idleAfter
}

func (mw *mockWorker) Stop() {
	mw.stopped = true
}

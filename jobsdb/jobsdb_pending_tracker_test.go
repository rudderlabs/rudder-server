package jobsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestPendingTrackerPrune(t *testing.T) {
	snap := func(xmin, xmax uint64) pgSnapshot { return pgSnapshot{xmin: xmin, xmax: xmax} }
	tr := &pendingEventsTracker{inflight: map[uint64]int{}}
	tr.transitions = []transitionRange{
		{lo: 0, hi: 10, snap: snap(100, 110)},
		{lo: 10, hi: 20, snap: snap(200, 210)},
		{lo: 20, hi: 30, snap: snap(300, 300)},
	}
	tr.inflight[150] = 1 // a status transaction that the second snapshot may have seen is still in flight
	tr.pruneLocked()
	require.Len(t, tr.transitions, 2, "only the first range has no in-flight transaction below its xmax")
	require.EqualValues(t, 10, tr.transitions[0].lo)

	delete(tr.inflight, 150)
	tr.pruneLocked()
	require.Len(t, tr.transitions, 1, "the newest range is always kept")
	require.EqualValues(t, 20, tr.transitions[0].lo)

	require.Nil(t, tr.findTransitionLocked(5), "a pruned range is not found")
	require.NotNil(t, tr.findTransitionLocked(25))
	require.Nil(t, tr.findTransitionLocked(30))
}

// pendingTrackerEnv is a gw jobsdb with a separate writer and a tracked reader, like a gateway and a processor.
type pendingTrackerEnv struct {
	t       *testing.T
	writer  *Handle
	reader  *Handle
	tracker *pendingEventsTracker
	stats   *memstats.Store
	addDS   chan time.Time
	compact chan time.Time // drives the reader's compaction loop
}

func newPendingTrackerEnv(t *testing.T, multiConsumer bool, configure ...func(c *config.Config)) *pendingTrackerEnv {
	t.Helper()
	_ = startPostgres(t)
	c := config.New()
	c.Set("JobsDB.maxDSSize", 10)
	for _, f := range configure {
		f(c)
	}
	st, err := memstats.New()
	require.NoError(t, err)
	env := &pendingTrackerEnv{t: t, stats: st, addDS: make(chan time.Time), compact: make(chan time.Time)}

	opts := []OptsFunc{WithConfig(c), WithStats(st)}
	if multiConsumer {
		opts = append(opts, WithMultiConsumer())
	}
	env.writer = NewForWrite("gw", append(opts, WithTriggerAddNewDS(func() <-chan time.Time { return env.addDS }))...)
	require.NoError(t, env.writer.Start())
	t.Cleanup(env.writer.TearDown)

	never := make(chan time.Time)
	env.reader = NewForRead("gw", opts...)
	env.reader.TriggerRefreshDS = func() <-chan time.Time { return never }
	env.reader.TriggerCompaction = func() <-chan time.Time { return env.compact }
	require.NoError(t, env.reader.Start())
	t.Cleanup(env.reader.TearDown)

	tracker, err := NewPendingEventsTracker(env.reader)
	require.NoError(t, err)
	env.tracker = tracker.(*pendingEventsTracker)
	return env
}

func (e *pendingTrackerEnv) store(source string, n int, consumers ...string) {
	e.t.Helper()
	require.NoError(e.t, e.storeErr(source, n, consumers...))
}

func (e *pendingTrackerEnv) storeErr(source string, n int, consumers ...string) error {
	jobs := make([]*JobT, n)
	for i := range jobs {
		jobs[i] = &JobT{
			UUID:         uuid.New(),
			UserID:       "user",
			WorkspaceId:  "ws",
			CustomVal:    "GW",
			Parameters:   fmt.Appendf(nil, `{"source_id":%q}`, source),
			EventPayload: []byte(`{}`),
			EventCount:   1,
			Consumers:    consumers,
		}
	}
	return e.writer.Store(context.Background(), jobs)
}

// rotate seals the last dataset and makes the reader see the new one.
func (e *pendingTrackerEnv) rotate() {
	e.t.Helper()
	before, _ := e.reader.dsList.snapshot()
	e.addDS <- time.Now()
	e.addDS <- time.Now() // the loop picks the second send only after it handled the first
	require.NoError(e.t, e.reader.RefreshDSList(context.Background()))
	after, _ := e.reader.dsList.snapshot()
	require.Greater(e.t, len(after), len(before), "a new dataset should exist")
}

// unprocessed returns the unprocessed jobs of a source, through the tracker like the processor does.
func (e *pendingTrackerEnv) unprocessed(source string) []*JobT {
	e.t.Helper()
	jobs, err := e.unprocessedErr(source, "")
	require.NoError(e.t, err)
	return jobs
}

// unprocessedFor returns the jobs of a source that are unprocessed for a consumer of a multi-consumer jobsdb.
func (e *pendingTrackerEnv) unprocessedFor(source, consumer string) []*JobT {
	e.t.Helper()
	jobs, err := e.unprocessedErr(source, consumer)
	require.NoError(e.t, err)
	return jobs
}

func (e *pendingTrackerEnv) unprocessedErr(source, consumer string) ([]*JobT, error) {
	res, err := e.tracker.GetUnprocessed(context.Background(), GetQueryParams{
		JobsLimit:        1000,
		Consumer:         consumer,
		ParameterFilters: []ParameterFilterT{{Name: "source_id", Value: source}},
	})
	return res.Jobs, err
}

func succeededStatuses(jobs []*JobT, consumer string) []*JobStatusT {
	statuses := make([]*JobStatusT, len(jobs))
	for i, job := range jobs {
		statuses[i] = &JobStatusT{
			JobID:         job.JobID,
			JobState:      Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			CustomVal:     job.CustomVal,
			Consumer:      consumer,
		}
	}
	return statuses
}

func (e *pendingTrackerEnv) succeed(jobs []*JobT, consumer string) {
	e.t.Helper()
	require.NoError(e.t, e.tracker.UpdateJobStatus(context.Background(), succeededStatuses(jobs, consumer)))
}

func (e *pendingTrackerEnv) iterate() {
	e.t.Helper()
	require.NoError(e.t, e.tracker.iterate(context.Background()))
}

// gauge returns the last exported value of a source's gauge, or -1 if it was never exported.
func (e *pendingTrackerEnv) gauge(source, consumer string) float64 {
	return lastValue(e.stats.Get("jobsdb_gw_pending_events_count", stats.Tags{"workspaceId": "ws", "sourceId": source, "consumer": consumer}))
}

// gaugeAll returns the last exported value of a consumer's _all gauge, or -1 if it was never exported.
func (e *pendingTrackerEnv) gaugeAll(consumer string) float64 {
	return lastValue(e.stats.Get("jobsdb_gw_pending_events_count_all", stats.Tags{"consumer": consumer}))
}

func lastValue(m *memstats.Measurement) float64 {
	if m == nil {
		return -1
	}
	return m.LastValue()
}

// polls returns how many poll counts ran so far.
func (e *pendingTrackerEnv) polls() int {
	m := e.stats.Get("pending_tracker_query", stats.Tags{"customVal": "gw", "op": "poll"})
	if m == nil {
		return 0
	}
	return len(m.Durations())
}

// settle iterates until no poll count runs for a while, i.e. until the pg_stat counters of earlier writes are flushed.
func (e *pendingTrackerEnv) settle() {
	e.t.Helper()
	quietSince, last := time.Now(), e.polls()
	require.Eventually(e.t, func() bool {
		e.iterate()
		if n := e.polls(); n != last {
			quietSince, last = time.Now(), n
		}
		return time.Since(quietSince) > 2*time.Second
	}, 30*time.Second, 100*time.Millisecond)
}

// eventually iterates until the exported gauges of the default consumer match, to absorb the pg_stat flush delay of the poll.
func (e *pendingTrackerEnv) eventually(expected map[string]float64) {
	e.t.Helper()
	var total float64
	for _, v := range expected {
		total += v
	}
	require.Eventually(e.t, func() bool {
		if err := e.tracker.iterate(context.Background()); err != nil {
			return false
		}
		for source, v := range expected {
			if e.gauge(source, "") != v {
				return false
			}
		}
		return e.gaugeAll("") == total
	}, 30*time.Second, 100*time.Millisecond, "expected %v", expected)
}

func TestPendingTracker(t *testing.T) {
	t.Run("rejects a handle that is not read-only", func(t *testing.T) {
		for _, ownerType := range []OwnerType{Write, ReadWrite} {
			_, err := NewPendingEventsTracker(&Handle{ownerType: ownerType, tablePrefix: "gw"})
			require.Error(t, err, ownerType)
		}
	})

	t.Run("live, frozen and rotation", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		env.store("A", 6)
		env.store("B", 4)
		require.EqualValues(t, -1, env.gaugeAll(""), "nothing is exported before the first transition")

		env.iterate() // first transition, single dataset: everything is live
		require.EqualValues(t, 6, env.gauge("A", ""))
		require.EqualValues(t, 4, env.gauge("B", ""))
		require.EqualValues(t, 10, env.gaugeAll(""))

		env.succeed(env.unprocessed("A")[:2], "") // live jobs: the listener does nothing, the poll counts them
		env.eventually(map[string]float64{"A": 4, "B": 4})

		env.store("A", 5) // ds_1 is now over maxDSSize
		env.rotate()
		env.store("B", 3) // into ds_2
		env.iterate()     // transition: ds_1 is frozen and counted once, ds_2 is live
		require.EqualValues(t, 9, env.gauge("A", ""))
		require.EqualValues(t, 7, env.gauge("B", ""))

		env.succeed(env.unprocessed("A")[:3], "") // frozen jobs: the listener subtracts at once
		env.iterate()
		require.EqualValues(t, 6, env.gauge("A", ""))

		bJobs := env.unprocessed("B")
		env.succeed(bJobs[len(bJobs)-1:], "") // the newest B job is live, in ds_2
		env.eventually(map[string]float64{"A": 6, "B": 6})

		env.succeed(env.unprocessed("A"), "")
		env.succeed(env.unprocessed("B"), "")
		env.eventually(map[string]float64{"A": 0, "B": 0})
	})

	t.Run("statuses that race with the transition", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		env.store("A", 12)
		env.iterate()
		env.rotate()
		jobs := env.unprocessed("A") // all 12 are in ds_1, which the next transition freezes

		env.succeed(jobs[0:2], "") // before the transition: the jobs are still live, the count excludes them
		env.tracker.afterSnapshot = func() {
			env.succeed(jobs[2:5], "") // after the snapshot: the count includes them, the listener subtracts
		}
		env.tracker.afterCount = func() {
			env.succeed(jobs[5:6], "") // after the count, before the gauge write: the listener subtracts
		}
		env.iterate()
		env.tracker.afterSnapshot, env.tracker.afterCount = nil, nil
		require.EqualValues(t, 6, env.gauge("A", ""))
		require.EqualValues(t, 6, env.gaugeAll(""))
	})

	t.Run("a late listener for a status the snapshot saw", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		env.store("A", 12)
		env.iterate()
		env.rotate()
		jobs := env.unprocessed("A") // all in ds_1, which the next transition freezes

		committed, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() { // commits before the snapshot, but its listeners run only after the boundary moved
			done <- env.tracker.WithUpdateSafeTx(context.Background(), func(tx UpdateSafeTx) error {
				tx.Tx().AddSuccessListener(func() { close(committed); <-release }) // runs before the tracker's listener
				return env.tracker.UpdateJobStatusInTx(context.Background(), tx, succeededStatuses(jobs[:4], ""))
			})
		}()
		<-committed
		env.tracker.afterSnapshot = func() {
			close(release)
			require.NoError(t, <-done) // the tracker's listener ran with the new boundary and the snapshot
		}
		env.iterate()
		env.tracker.afterSnapshot = nil
		require.EqualValues(t, 8, env.gauge("A", ""), "the snapshot saw the commit, so the count excluded the jobs and the listener did nothing")
	})

	t.Run("poll skips an unchanged dataset", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		env.store("A", 3)
		env.iterate()
		env.settle()
		before := env.polls()
		for range 5 {
			env.iterate()
		}
		require.Equal(t, before, env.polls(), "no writes: no poll count")

		env.store("A", 1)
		env.eventually(map[string]float64{"A": 4})
		require.Greater(t, env.polls(), before, "a store changes the fingerprint: a poll count runs")
		env.settle()
		before = env.polls()
		for range 5 {
			env.iterate()
		}
		require.Equal(t, before, env.polls(), "no writes since the last poll: no poll count")
	})

	t.Run("compaction keeps the gauge", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		env.store("A", 12)
		env.rotate()
		env.store("A", 12)
		env.rotate()
		env.store("A", 2)
		env.iterate()
		require.EqualValues(t, 26, env.gauge("A", ""))
		jobs := env.unprocessed("A")
		env.succeed(jobs[:20], "") // ds_1 completes, ds_2 almost completes
		require.NoError(t, env.reader.doCompaction(context.Background()))
		require.NoError(t, env.reader.RefreshDSList(context.Background()))
		env.eventually(map[string]float64{"A": 6})
	})

	t.Run("compaction cannot commit between pinning the datasets and reading the snapshot", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false, func(c *config.Config) {
			c.Set("JobsDB.maxDSRetention", "1ms") // a sealed dataset with a terminal status is eligible for compaction at once
		})
		env.store("A", 12)
		env.iterate() // single dataset: all 12 are live
		jobs := env.unprocessed("A")
		env.succeed(jobs[:2], "") // ds_1 gets terminal statuses, so it becomes eligible once sealed
		env.rotate()
		env.store("A", 1) // ds_2

		env.tracker.afterPin = func() {
			// A compaction committing here would move ds_1's pending jobs to a new dataset that the pinned
			// list does not hold, and the statuses below would land there, out of the count's sight.
			env.compact <- time.Now()
			env.compact <- time.Now() // the loop takes the second only after it handled the first
			env.succeed(jobs[2:5], "")
		}
		env.iterate() // transition: freezes ds_1
		env.tracker.afterPin = nil
		require.EqualValues(t, 8, env.gauge("A", ""), "12 + 1 stored, 5 completed")

		env.compact <- time.Now() // without the transition holding it off, compaction runs and the gauge stays correct
		env.compact <- time.Now()
		require.NoError(t, env.reader.RefreshDSList(context.Background()))
		env.eventually(map[string]float64{"A": 8})
	})

	t.Run("a compaction before the lock never moves the boundary backwards", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false, func(c *config.Config) {
			c.Set("JobsDB.maxDSRetention", "1ms") // a sealed dataset with a terminal status is eligible for compaction at once
		})
		env.store("A", 12) // ds_1: job ids 1-12
		env.rotate()
		env.store("A", 2) // ds_2: job ids 13-14
		env.iterate()     // boundary 13: ds_1 frozen, ds_2 live
		require.EqualValues(t, 13, env.tracker.currentBoundary())

		jobs := env.unprocessed("A")
		env.succeed(jobs[10:14], "") // ds_1's two highest jobs, and both ds_2 jobs
		env.rotate()                 // ds_2 is sealed: a transition to boundary 15 is due

		env.tracker.beforeCompactionLock = func() {
			// compaction moves ds_1's pending jobs 1-10 to a new dataset and then drops the completed ds_2,
			// so the highest sealed job id falls to 10, below the current boundary. It takes two rounds:
			// the first one stops after ds_1, because it already holds maxDSSize pending jobs
			for range 3 { // the loop takes each send only after it handled the previous one
				env.compact <- time.Now()
			}
			_, ranges := env.reader.dsList.snapshot()
			require.EqualValues(t, 11, sealedBoundary(ranges))
		}
		env.iterate()
		env.tracker.beforeCompactionLock = nil
		require.EqualValues(t, 13, env.tracker.currentBoundary(), "the boundary never moves backwards")
		env.eventually(map[string]float64{"A": 10})
	})

	t.Run("a failed transition resets, and the next one recovers", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		env.store("A", 12)
		env.iterate()
		env.rotate()
		env.store("A", 2)

		ctx, cancel := context.WithCancel(context.Background())
		env.tracker.afterSnapshot = cancel // the boundary has moved, then the count fails
		require.Error(t, env.tracker.iterate(ctx))
		env.tracker.afterSnapshot = nil
		require.EqualValues(t, 0, env.tracker.currentBoundary())
		require.Nil(t, env.tracker.liveFingerprint)
		require.EqualValues(t, 1, env.stats.Get("jobsdb_pending_tracker_reset", stats.Tags{"customVal": "gw"}).LastValue())

		env.iterate() // a transition from zero
		require.EqualValues(t, 14, env.gauge("A", ""))
		require.EqualValues(t, 13, env.tracker.currentBoundary())
	})

	t.Run("multi-consumer", func(t *testing.T) {
		env := newPendingTrackerEnv(t, true)
		env.store("A", 12, "proc", "arc")
		env.rotate()
		env.store("A", 2, "proc", "arc")
		env.iterate()
		require.EqualValues(t, 14, env.gauge("A", "proc"))
		require.EqualValues(t, 14, env.gauge("A", "arc"))

		jobs := env.unprocessedFor("A", "proc")
		env.succeed(jobs[:5], "proc") // frozen jobs, one consumer only
		env.iterate()
		require.EqualValues(t, 9, env.gauge("A", "proc"))
		require.EqualValues(t, 14, env.gauge("A", "arc"))
		require.EqualValues(t, 9, env.gaugeAll("proc"))
		require.EqualValues(t, 14, env.gaugeAll("arc"))
	})

	t.Run("stop and start keep the state", func(t *testing.T) {
		env := newPendingTrackerEnv(t, false)
		trigger := make(chan time.Time)
		env.tracker.trigger = func() <-chan time.Time { return trigger }
		env.store("A", 3)
		require.NoError(t, env.tracker.Start())
		require.Eventually(t, func() bool { return env.gauge("A", "") == 3 }, 10*time.Second, 50*time.Millisecond)
		env.tracker.Stop()
		require.NoError(t, env.tracker.Start())
		trigger <- time.Now()
		trigger <- time.Now()
		require.EqualValues(t, 3, env.gauge("A", ""))
		env.tracker.Stop()
	})
}

// TestPendingTrackerConcurrency stores, completes and rotates concurrently with the tracker, then
// compares the gauge with the number of jobs that were stored and not completed.
func TestPendingTrackerConcurrency(t *testing.T) {
	env := newPendingTrackerEnv(t, false)
	sources := []string{"A", "B", "C"}
	var stored, completed sync.Map // source -> *atomic.Int64
	counter := func(m *sync.Map, source string) *atomic.Int64 {
		v, _ := m.LoadOrStore(source, &atomic.Int64{})
		return v.(*atomic.Int64)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var writers, others errgroup.Group
	for i := range 3 {
		writers.Go(func() error {
			deadline := time.Now().Add(3 * time.Second)
			for n := 0; ctx.Err() == nil && time.Now().Before(deadline); n++ {
				time.Sleep(10 * time.Millisecond)
				source := sources[(i+n)%len(sources)]
				if err := env.storeErr(source, 3); err != nil {
					return err
				}
				counter(&stored, source).Add(3)
			}
			return nil
		})
	}
	stop := make(chan struct{})
	others.Go(func() error { // the processor: one completer, so a job never gets two terminal statuses
		for {
			select {
			case <-stop:
				return nil
			default:
			}
			for _, source := range sources {
				jobs, err := env.unprocessedErr(source, "")
				if err != nil {
					return err
				}
				if len(jobs) > 5 {
					jobs = jobs[:5]
				}
				if len(jobs) == 0 {
					continue
				}
				if err := env.tracker.UpdateJobStatus(context.Background(), succeededStatuses(jobs, "")); err != nil {
					return err
				}
				counter(&completed, source).Add(int64(len(jobs)))
			}
		}
	})
	others.Go(func() error { // dataset rotation
		for {
			select {
			case <-stop:
				return nil
			case <-time.After(100 * time.Millisecond):
			}
			env.addDS <- time.Now()
			if err := env.reader.RefreshDSList(context.Background()); err != nil {
				return err
			}
		}
	})
	others.Go(func() error { // the tracker loop
		for {
			select {
			case <-stop:
				return nil
			case <-time.After(20 * time.Millisecond):
			}
			if err := env.tracker.iterate(context.Background()); err != nil {
				return err
			}
		}
	})
	require.NoError(t, writers.Wait())
	close(stop) // stop completing while jobs are still pending, so the final gauges are not trivially zero
	require.NoError(t, others.Wait())

	expected := make(map[string]float64)
	for _, source := range sources {
		expected[source] = float64(counter(&stored, source).Load() - counter(&completed, source).Load())
	}
	require.NoError(t, env.reader.RefreshDSList(context.Background()))
	env.eventually(expected)

	// the run must have frozen jobs while they were being completed, or it proves nothing
	transitions := env.stats.Get("pending_tracker_query", stats.Tags{"customVal": "gw", "op": "transition"})
	require.NotNil(t, transitions)
	require.Greater(t, len(transitions.Durations()), 3, "datasets should rotate several times during the run")
	var total float64
	for _, source := range sources {
		total += float64(counter(&completed, source).Load())
	}
	require.Greater(t, total, 100.0, "jobs should be completed during the run")
	var pending float64
	for _, v := range expected {
		pending += v
	}
	require.Greater(t, pending, 0.0, "jobs should still be pending at the end")
	t.Logf("transitions: %d, completed: %.0f, expected pending: %v", len(transitions.Durations()), total, expected)
}

package jobsdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

// readCursorEnv is an rt jobsdb read through a read cursor, like a router reading its jobs.
type readCursorEnv struct {
	t     *testing.T
	conf  *config.Config
	stats *memstats.Store
	addDS chan time.Time
	h     *Handle
	inner *hookedJobsDB // the JobsDB under the read cursor
	db    *readCursor
}

// hookedJobsDB calls afterRead, if set, once a read to process returns and before the read cursor sees its result.
type hookedJobsDB struct {
	JobsDB
	afterRead func()
}

func (db *hookedJobsDB) GetToProcess(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) {
	res, err := db.JobsDB.GetToProcess(ctx, params, more)
	if db.afterRead != nil {
		db.afterRead()
	}
	return res, err
}

func newReadCursorEnv(t *testing.T) *readCursorEnv {
	t.Helper()
	_ = startPostgres(t)
	c := config.New()
	c.Set("JobsDB.maxDSSize", 10)
	c.Set("JobsDB.readCursor.persistMinSkippedDatasets", 1)
	e := &readCursorEnv{t: t, conf: c}
	e.open()
	t.Cleanup(e.close)
	return e
}

// open starts the handle and its read cursor, and recovers the executing jobs like a router does on start.
func (e *readCursorEnv) open() {
	e.t.Helper()
	st, err := memstats.New()
	require.NoError(e.t, err)
	e.stats, e.addDS = st, make(chan time.Time)
	e.h = NewForReadWrite("rt", WithConfig(e.conf), WithStats(st), WithTriggerAddNewDS(func() <-chan time.Time { return e.addDS }),
		WithDSLimit(e.conf.GetReloadableIntVar(0, 1, "JobsDB.dsLimit")))
	never := make(chan time.Time)
	e.h.TriggerCompaction = func() <-chan time.Time { return never } // compact runs it synchronously instead
	e.inner = &hookedJobsDB{JobsDB: e.h}
	e.db = NewReadCursor(e.inner, e.h).(*readCursor)
	require.NoError(e.t, e.db.Start())
	e.db.FailExecuting()
}

func (e *readCursorEnv) close() {
	if e.h == nil {
		return
	}
	e.db.Stop()
	e.h.Close()
	e.h = nil
}

func (e *readCursorEnv) restart() {
	e.t.Helper()
	e.close()
	e.open()
}

func (e *readCursorEnv) store(dest string, n int) {
	e.t.Helper()
	require.NoError(e.t, e.storeErr(dest, n))
}

func (e *readCursorEnv) storeErr(dest string, n int) error {
	return e.h.Store(context.Background(), readCursorJobs(dest, n))
}

func readCursorJobs(dest string, n int) []*JobT {
	jobs := make([]*JobT, n)
	for i := range jobs {
		jobs[i] = &JobT{
			UUID:         uuid.New(),
			UserID:       "user",
			WorkspaceId:  "ws",
			CustomVal:    "WEBHOOK",
			Parameters:   fmt.Appendf(nil, `{"source_id":"src","destination_id":%q}`, dest),
			EventPayload: []byte(`{}`),
			EventCount:   1,
		}
	}
	return jobs
}

// rotate seals the last dataset.
func (e *readCursorEnv) rotate() {
	e.t.Helper()
	before, _ := e.h.dsList.snapshot()
	e.addDS <- time.Now()
	e.addDS <- time.Now() // the loop takes the second only after it handled the first
	after, _ := e.h.dsList.snapshot()
	require.Greater(e.t, len(after), len(before), "a new dataset should exist")
}

// compact runs a compaction round, so that its datasets don't change under lowestPending.
func (e *readCursorEnv) compact() {
	e.t.Helper()
	require.NoError(e.t, e.h.doCompaction(context.Background()))
}

func ids(jobs []*JobT) []int64 {
	return lo.Map(jobs, func(j *JobT, _ int) int64 { return j.JobID })
}

func destParams(dest string, limit int) GetQueryParams {
	return GetQueryParams{
		CustomValFilters: []string{"WEBHOOK"},
		ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: dest}},
		JobsLimit:        limit,
	}
}

// pickup reads the jobs of a destination to process, like the router does.
func (e *readCursorEnv) pickup(dest string, limit int) []*JobT {
	e.t.Helper()
	res, err := e.db.GetToProcess(context.Background(), destParams(dest, limit), nil)
	require.NoError(e.t, err)
	return res.Jobs
}

func (e *readCursorEnv) mark(jobs []*JobT, state string) {
	e.t.Helper()
	require.NoError(e.t, e.markErr(jobs, state))
}

func (e *readCursorEnv) markErr(jobs []*JobT, state string) error {
	return e.db.UpdateJobStatus(context.Background(), readCursorStatuses(jobs, state))
}

func readCursorStatuses(jobs []*JobT, state string) []*JobStatusT {
	return lo.Map(jobs, func(job *JobT, _ int) *JobStatusT {
		return &JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			CustomVal:     job.CustomVal,
			PartitionID:   job.PartitionID,
		}
	})
}

func (e *readCursorEnv) pipeline(dest string) *pipeline {
	e.db.pipelinesMu.RLock()
	defer e.db.pipelinesMu.RUnlock()
	return e.db.pipelines[pipelineKey{CustomVal: "WEBHOOK", FilterType: destinationFilterType, FilterValue: dest}]
}

func (e *readCursorEnv) watermark(dest string) int64 {
	if p := e.pipeline(dest); p != nil {
		return p.getWatermark()
	}
	return 0
}

func (e *readCursorEnv) applied(dest string) int64 {
	if p := e.pipeline(dest); p != nil {
		return p.applied.Load()
	}
	return 0
}

func (e *readCursorEnv) persist() {
	e.t.Helper()
	require.NoError(e.t, e.db.persist(context.Background()))
}

func (e *readCursorEnv) skips() int {
	return int(lo.SumBy(e.stats.GetByName("jobsdb_cursor_skips"), func(m memstats.Metric) float64 { return m.Value }))
}

// lowestPending returns the lowest pending job of a destination, reading every dataset directly, or -1 if it has none.
func (e *readCursorEnv) lowestPending(dest string) (int64, error) {
	dsList, err := getDSList(e.h, e.h.dbHandle, e.h.tablePrefix)
	if err != nil {
		return 0, err
	}
	lowest := int64(-1)
	for _, ds := range dsList {
		var id sql.NullInt64
		if err := e.h.dbHandle.QueryRow(fmt.Sprintf(`SELECT MIN(j.job_id) FROM %q j
			LEFT JOIN LATERAL (SELECT job_state FROM %q s WHERE s.job_id = j.job_id ORDER BY s.id DESC LIMIT 1) ls ON true
			WHERE j.parameters->>'destination_id' = $1 AND (ls.job_state IS NULL OR ls.job_state <> ALL($2))`, ds.JobTable, ds.JobStatusTable),
			dest, pq.Array(validTerminalStates)).Scan(&id); err != nil {
			return 0, err
		}
		if id.Valid && (lowest == -1 || id.Int64 < lowest) {
			lowest = id.Int64
		}
	}
	return lowest, nil
}

// requireInvariant checks that no pending job of a destination is below its watermark. The watermark is read
// first: it only moves forward and a job below it never becomes pending again, so a pending job found later
// was pending when the watermark was read.
func (e *readCursorEnv) requireInvariant(dest string, msgAndArgs ...any) {
	e.t.Helper()
	watermark := e.watermark(dest)
	lowest, err := e.lowestPending(dest)
	require.NoError(e.t, err)
	if lowest != -1 {
		require.LessOrEqualf(e.t, watermark, lowest, "a pending job of %q is below its watermark %v", dest, msgAndArgs)
	}
}

func TestReadCursor(t *testing.T) {
	t.Run("a pipeline with jobs only in the last dataset skips the sealed ones once its watermark is persisted, also after a restart", func(t *testing.T) {
		env := newReadCursorEnv(t)
		for range 3 {
			env.store("a", 10)
			env.rotate()
		}
		env.store("b", 2)

		jobs := env.pickup("b", 100)
		require.Len(t, jobs, 2)
		require.Equal(t, jobs[0].JobID, env.watermark("b"), "b has nothing pending below its first job")
		require.Len(t, env.pickup("b", 100), 2)
		require.Zero(t, env.skips(), "the watermark is not persisted yet")

		env.persist()
		require.Len(t, env.pickup("b", 100), 2)
		require.Equal(t, 3, env.skips(), "the read after the persist skips the 3 sealed datasets")

		env.restart()
		require.Equal(t, jobs[0].JobID, env.watermark("b"), "the watermark is loaded on start")
		require.Len(t, env.pickup("b", 100), 2)
		require.Equal(t, 3, env.skips(), "the first read after the restart skips the sealed datasets")
	})

	t.Run("a watermark is persisted and applied while it skips at least the first persistMinSkippedDatasets sealed datasets", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		env.store("a", 5)
		env.store("b", 5)
		env.rotate()
		env.store("b", 10)
		env.rotate()

		env.mark(env.pickup("a", 100)[:10], Succeeded.State)
		require.Len(t, env.pickup("a", 100), 5)
		require.EqualValues(t, 11, env.watermark("a"), "a skips the first sealed dataset")
		env.mark(env.pickup("b", 100)[:5], Succeeded.State)
		require.Len(t, env.pickup("b", 100), 10)
		require.EqualValues(t, 21, env.watermark("b"), "b skips the first two")
		require.Zero(t, env.applied("a"))
		require.Zero(t, env.applied("b"))

		env.persist()
		require.EqualValues(t, 11, env.applied("a"))
		require.EqualValues(t, 21, env.applied("b"))

		env.conf.Set("JobsDB.readCursor.persistMinSkippedDatasets", 2)
		env.persist()
		require.Zero(t, env.applied("a"), "a skips too few datasets to stay persisted")
		require.EqualValues(t, 21, env.applied("b"))

		env.restart()
		require.Zero(t, env.watermark("a"), "a skipped too few datasets to be persisted")
		require.EqualValues(t, 21, env.watermark("b"))
	})

	t.Run("a pending job at the end of a sealed dataset is read once its watermark is persisted", func(t *testing.T) {
		env := newReadCursorEnv(t)
		for range 2 {
			env.store("a", 10)
			env.rotate()
		}
		env.store("a", 1)
		jobs := env.pickup("a", 100)
		env.mark(jobs[:19], Succeeded.State)
		env.mark(jobs[19:20], Failed.State)
		env.pickup("a", 100)
		require.Equal(t, jobs[19].JobID, env.watermark("a"))

		env.persist()
		require.Equal(t, ids(jobs[19:]), ids(env.pickup("a", 100)))
		require.Equal(t, 1, env.skips(), "the read skips only the first sealed dataset")
	})

	t.Run("a read without a workspace, source or destination filter is of the pipeline of all jobs", func(t *testing.T) {
		env := newReadCursorEnv(t)
		readAll := func() []*JobT {
			t.Helper()
			res, err := env.db.GetToProcess(context.Background(), GetQueryParams{CustomValFilters: []string{"WEBHOOK"}, JobsLimit: 100}, nil)
			require.NoError(t, err)
			return res.Jobs
		}
		env.store("a", 10)
		env.rotate()
		env.store("b", 10)
		env.rotate()
		env.store("b", 1)
		jobs := readAll()
		require.Len(t, jobs, 21)

		env.mark(jobs[:10], Succeeded.State)
		env.mark(jobs[10:20], Executing.State)
		require.Equal(t, ids(jobs[20:]), ids(readAll()))
		env.persist()
		readAll()
		require.Equal(t, 1, env.skips(), "the executing jobs of b hold the watermark back")

		env.mark(jobs[10:20], Succeeded.State)
		readAll()
		env.persist()
		readAll()
		require.Equal(t, 4, env.skips(), "the read before the persist skips the first sealed dataset, the read after it both")
	})

	t.Run("a job that commits after a later one holds the watermark back, also if its dataset is sealed during the read", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		env.mark(env.pickup("a", 100), Succeeded.State)

		stored, commit := make(chan struct{}), make(chan struct{})
		g := &errgroup.Group{}
		g.Go(func() error { // its job id comes first, its commit last
			return env.h.WithStoreSafeTx(context.Background(), func(tx StoreSafeTx) error {
				if err := env.h.StoreInTx(context.Background(), tx, readCursorJobs("a", 1)); err != nil {
					return err
				}
				close(stored)
				<-commit
				return nil
			})
		})
		<-stored
		env.store("a", 9)              // fills the dataset, so that it can be sealed
		env.inner.afterRead = func() { // the job commits and its dataset is sealed before the read cursor sees the result
			env.inner.afterRead = nil
			close(commit)
			require.NoError(t, g.Wait())
			env.rotate()
		}
		require.Len(t, env.pickup("a", 100), 9, "the read sees only the later jobs")
		env.requireInvariant("a")
		require.Len(t, env.pickup("a", 100), 10)
	})

	t.Run("a read that finds no jobs moves the watermark up to the live dataset", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 1)
		env.mark(env.pickup("a", 100), Succeeded.State)
		env.store("b", 9)
		env.rotate()
		for range 2 {
			env.store("b", 10)
			env.rotate()
		}
		require.Empty(t, env.pickup("a", 100))
		require.EqualValues(t, 31, env.watermark("a"), "a has nothing pending in the sealed datasets")
		env.requireInvariant("a")

		env.persist()
		require.Empty(t, env.pickup("a", 100))
		require.Equal(t, 3, env.skips())
	})

	t.Run("a read that queries nothing doesn't move the watermark", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		noPayload := destParams("a", 100)
		noPayload.PayloadSizeLimit = -1
		for _, params := range []GetQueryParams{destParams("a", 0), noPayload} {
			res, err := env.db.GetToProcess(context.Background(), params, nil)
			require.NoError(t, err)
			require.Empty(t, res.Jobs)
		}
		require.Zero(t, env.watermark("a"))
	})

	t.Run("a read that stops at the datasets limit without finding jobs doesn't move the watermark", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("b", 10)
		env.rotate()
		env.store("a", 1)
		env.store("b", 9)
		env.rotate()
		env.conf.Set("JobsDB.dsLimit", 1)
		require.Empty(t, env.pickup("a", 100), "the read stops after the first dataset")
		require.Zero(t, env.watermark("a"))
		env.requireInvariant("a")

		env.conf.Set("JobsDB.dsLimit", 0)
		require.Len(t, env.pickup("a", 100), 1)
	})

	t.Run("a read of a later page doesn't move the watermark", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		env.store("a", 1)
		first, err := env.db.GetToProcess(context.Background(), destParams("a", 2), nil)
		require.NoError(t, err)
		second, err := env.db.GetToProcess(context.Background(), destParams("a", 2), first.More)
		require.NoError(t, err)
		require.Len(t, second.Jobs, 2)
		require.Equal(t, first.Jobs[0].JobID, env.watermark("a"), "the jobs of the first page may not be picked up")
		env.requireInvariant("a")
	})

	t.Run("a terminal status that rolls back leaves its job pending", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		env.store("a", 1)
		jobs := env.pickup("a", 100)
		env.mark(jobs[:1], Executing.State)
		ctx := context.Background()
		rollback := errors.New("rollback")
		require.ErrorIs(t, env.db.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
			if err := env.db.UpdateJobStatusInTx(ctx, tx, readCursorStatuses(jobs[:1], Succeeded.State)); err != nil {
				return err
			}
			return rollback
		}), rollback)
		require.Equal(t, ids(jobs[1:]), ids(env.pickup("a", 100)))
		require.Equal(t, jobs[0].JobID, env.watermark("a"), "the executing job holds the watermark back")
		env.requireInvariant("a")
	})

	t.Run("a read that ends after the read cursor got disabled doesn't move the watermark", func(t *testing.T) {
		rc := &readCursor{h: &Handle{}, pipelines: map[pipelineKey]*pipeline{}}
		p := rc.getPipeline(pipelineKey{CustomVal: "WEBHOOK", FilterType: destinationFilterType, FilterValue: "d1"})
		rc.disabled.Store(true)
		rc.endRead(p, 100, JobsResult{Jobs: []*JobT{{JobID: 10}}})
		require.Zero(t, p.getWatermark())
	})

	t.Run("a job holds the watermark back from before its first pending status commits until it is terminal", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		env.store("a", 1)

		jobs := env.pickup("a", 100)
		require.Len(t, jobs, 11)
		sealed, live := jobs[:10], jobs[10:]
		ctx := context.Background()
		require.NoError(t, env.db.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
			tx.Tx().AddSuccessListener(func() { // a read as soon as the statuses commit
				require.Equal(t, ids(live), ids(env.pickup("a", 100)), "the executing jobs are invisible to the read")
			})
			return env.db.UpdateJobStatusInTx(ctx, tx, readCursorStatuses(sealed, Executing.State))
		}))
		require.Equal(t, sealed[0].JobID, env.watermark("a"), "but they hold the watermark back")

		env.mark(sealed[:1], Failed.State)
		env.mark(sealed[1:], Succeeded.State)
		require.Equal(t, []int64{sealed[0].JobID, live[0].JobID}, ids(env.pickup("a", 100)))
		require.Equal(t, sealed[0].JobID, env.watermark("a"), "the failed job is still pending")

		env.mark(sealed[:1], Executing.State)
		env.mark(sealed[:1], Succeeded.State)
		require.Equal(t, ids(live), ids(env.pickup("a", 100)))
		require.Equal(t, live[0].JobID, env.watermark("a"), "nothing of a is pending in the sealed dataset any more")
		env.requireInvariant("a")
	})

	t.Run("a status that lacks an attribute of its job disables the read cursor", func(t *testing.T) {
		env := newReadCursorEnv(t)
		disabledGauge := func() float64 {
			return env.stats.Get("jobsdb_read_cursor_disabled", stats.Tags{"tablePrefix": "rt"}).LastValue()
		}
		persisted := func() (n int) {
			require.NoError(t, env.h.dbHandle.QueryRow(`SELECT COUNT(*) FROM `+env.h.readCursorsTable()).Scan(&n))
			return n
		}
		for range 2 {
			env.store("a", 10)
			env.rotate()
		}
		env.store("a", 1)
		jobs := env.pickup("a", 100)
		env.mark(jobs[:10], Succeeded.State)
		env.pickup("a", 100)
		env.persist()
		require.Equal(t, 1, persisted())
		require.Equal(t, jobs[10].JobID, env.applied("a"))
		require.Zero(t, disabledGauge())

		require.NoError(t, env.db.UpdateJobStatus(context.Background(), lo.Map(jobs[10:20], func(job *JobT, _ int) *JobStatusT {
			return &JobStatusT{
				JobID: job.JobID, JobState: Executing.State, AttemptNum: 1, ExecTime: time.Now(), RetryTime: time.Now(),
				ErrorResponse: []byte(`{}`), Parameters: []byte(`{}`), WorkspaceId: job.WorkspaceId, CustomVal: job.CustomVal,
			}
		})))
		require.True(t, env.db.disabled.Load())
		require.EqualValues(t, 1, disabledGauge())
		require.Equal(t, ids(jobs[20:]), ids(env.pickup("a", 100)), "the executing jobs are invisible")
		env.requireInvariant("a")

		env.persist()
		require.Zero(t, persisted(), "a disabled read cursor persists no watermarks")
		require.Zero(t, env.applied("a"))
	})

	t.Run("reads without unprocessed don't move the watermark", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.store("a", 10)
		env.rotate()
		env.store("a", 5)
		jobs := env.pickup("a", 100)
		env.mark(jobs[10:], Executing.State)
		env.mark(jobs[10:], Importing.State) // the jobs of the sealed dataset stay unprocessed

		res, err := env.db.GetImporting(context.Background(), destParams("a", 100), nil)
		require.NoError(t, err)
		require.Equal(t, ids(jobs[10:]), ids(res.Jobs))
		require.Equal(t, jobs[0].JobID, env.watermark("a"), "the importing read did not see the unprocessed jobs below")

		require.Len(t, env.pickup("a", 100), 10)
		env.requireInvariant("a")
	})

	t.Run("random operations never leave a pending job below a watermark", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.conf.Set("JobsDB.maxDSRetention", "1ms") // a sealed dataset with a terminal status is compacted, its pending jobs copied
		rnd := rand.New(rand.NewPCG(1, 2))
		dests := []string{"a", "b", "c"}
		executing := map[string][]*JobT{}
		lastDSJobs := 0
		for step := range 400 {
			dest := dests[rnd.IntN(len(dests))]
			switch op := rnd.IntN(100); {
			case op < 25: // jobs arrive, mostly for a
				if rnd.IntN(3) > 0 {
					dest = "a"
				}
				n := 1 + rnd.IntN(4)
				env.store(dest, n)
				lastDSJobs += n
			case op < 30:
				if lastDSJobs >= 10 {
					env.rotate()
					lastDSJobs = 0
				}
			case op < 60: // pickup, then mark what was picked executing
				jobs := env.pickup(dest, 1+rnd.IntN(5))
				env.mark(jobs, Executing.State)
				executing[dest] = append(executing[dest], jobs...)
			case op < 92: // complete some executing jobs
				jobs := executing[dest]
				if len(jobs) == 0 {
					continue
				}
				n := 1 + rnd.IntN(len(jobs))
				done := jobs[:n]
				executing[dest] = jobs[n:]
				env.mark(done, []string{Succeeded.State, Succeeded.State, Aborted.State, Failed.State, Waiting.State}[rnd.IntN(5)])
			case op < 95:
				env.compact()
			case op < 98:
				env.persist()
			default: // the executing jobs are failed by the recovery on start
				env.restart()
				clear(executing)
			}
			for _, d := range dests {
				env.requireInvariant(d, "step", step)
			}
		}
		t.Logf("watermarks: a=%d b=%d c=%d, skips: %d", env.watermark("a"), env.watermark("b"), env.watermark("c"), env.skips())
	})

	t.Run("concurrent pickups, workers and stores never leave a pending job below a watermark", func(t *testing.T) {
		env := newReadCursorEnv(t)
		env.conf.Set("JobsDB.readCursor.persistInterval", "10ms")
		env.restart()
		dests := []string{"a", "b", "c", "d"}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		g, ctx := errgroup.WithContext(ctx)
		type claims struct { // the jobs handed to the workers, so that two pickups don't hand out the same job
			sync.Mutex
			claimed map[int64]struct{}
		}
		claim := func(c *claims, jobs []*JobT) []*JobT {
			c.Lock()
			defer c.Unlock()
			return lo.Filter(jobs, func(j *JobT, _ int) bool {
				if _, ok := c.claimed[j.JobID]; ok {
					return false
				}
				c.claimed[j.JobID] = struct{}{}
				return true
			})
		}
		pickup := func(ctx context.Context, dest string, limit int, c *claims, work chan<- []*JobT) error {
			for ctx.Err() == nil {
				res, err := env.db.GetToProcess(ctx, destParams(dest, limit), nil)
				if err != nil {
					if ctx.Err() != nil {
						return nil //nolint:nilerr // the test is over
					}
					return err
				}
				jobs := claim(c, res.Jobs)
				if len(jobs) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				if err := env.markErr(jobs, Executing.State); err != nil {
					return err
				}
				select {
				case work <- jobs:
				case <-ctx.Done():
				}
			}
			return nil
		}

		g.Go(func() error { // the processor storing jobs, sealing a dataset now and then
			rnd := rand.New(rand.NewPCG(3, 4))
			for i := 0; ctx.Err() == nil; i++ {
				if err := env.storeErr(dests[rnd.IntN(len(dests))], 1+rnd.IntN(5)); err != nil {
					return err
				}
				if i%5 == 4 {
					select {
					case env.addDS <- time.Now():
					case <-ctx.Done():
					}
				}
				time.Sleep(time.Millisecond)
			}
			return nil
		})
		for i, dest := range dests { // a router partition per destination: pickup, mark executing, hand to the workers
			work := make(chan []*JobT, 10)
			c := &claims{claimed: map[int64]struct{}{}}
			var pickups sync.WaitGroup
			pickers := 1
			if dest == "d" { // two concurrent pickups, so that reads of a pipeline overlap with its executing writes
				pickers = 2
			}
			for range pickers {
				pickups.Add(1)
				g.Go(func() error {
					defer pickups.Done()
					return pickup(ctx, dest, 1+i, c, work)
				})
			}
			g.Go(func() error { pickups.Wait(); close(work); return nil })
			var workers sync.WaitGroup
			for w := range 3 {
				workers.Go(func() {
					rnd := rand.New(rand.NewPCG(uint64(i), uint64(w)))
					for jobs := range work {
						time.Sleep(time.Duration(rnd.IntN(3)) * time.Millisecond)
						for _, job := range jobs {
							state := []string{Succeeded.State, Succeeded.State, Aborted.State, Failed.State, Waiting.State}[rnd.IntN(5)]
							if env.markErr([]*JobT{job}, state) == nil && (state == Failed.State || state == Waiting.State) {
								c.Lock()
								delete(c.claimed, job.JobID) // to be picked up again
								c.Unlock()
							}
						}
					}
				})
			}
			g.Go(func() error { workers.Wait(); return nil })
		}
		g.Go(func() error { // the invariant, checked while everything runs
			for ctx.Err() == nil {
				for _, dest := range dests {
					watermark := env.watermark(dest)
					lowest, err := env.lowestPending(dest)
					if err != nil {
						if ctx.Err() != nil {
							return nil //nolint:nilerr // the test is over
						}
						return err
					}
					if lowest != -1 && watermark > lowest {
						return fmt.Errorf("a pending job %d of %q is below its watermark %d", lowest, dest, watermark)
					}
				}
			}
			return nil
		})
		require.NoError(t, g.Wait())
		for _, dest := range dests {
			env.requireInvariant(dest)
		}
		t.Logf("watermarks: %v, skips: %d", lo.Map(dests, func(d string, _ int) int64 { return env.watermark(d) }), env.skips())
	})
}

func TestPipelineKey(t *testing.T) {
	key := pipelineKey{CustomVal: "WEBHOOK", FilterType: destinationFilterType, FilterValue: "d1"}

	t.Run("it is persisted as its fields joined by the separator", func(t *testing.T) {
		require.Equal(t, "WEBHOOK|destination|d1|", key.String(), "custom value, filter type, filter value and consumer")
		parsed, ok := parsePipelineKey(key.String())
		require.True(t, ok)
		require.Equal(t, key, parsed)
		_, ok = parsePipelineKey("WEB|HOOK|destination|d1|")
		require.False(t, ok, "a key whose filters contain the separator does not parse")
	})

	t.Run("it is the filters of a read", func(t *testing.T) {
		rc := &readCursor{h: &Handle{}}
		read, err := rc.pipelineKeyOf(&GetQueryParams{CustomValFilters: []string{"WEBHOOK"}, ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: "d1"}}})
		require.NoError(t, err)
		require.Equal(t, key, read)
		read, err = rc.pipelineKeyOf(&GetQueryParams{WorkspaceID: "ws"})
		require.NoError(t, err)
		require.Equal(t, pipelineKey{FilterType: workspaceFilterType, FilterValue: "ws"}, read, "the custom value is optional")
		read, err = rc.pipelineKeyOf(&GetQueryParams{CustomValFilters: []string{"WEBHOOK"}})
		require.NoError(t, err)
		require.Equal(t, pipelineKey{CustomVal: "WEBHOOK", FilterType: noneFilterType}, read, "a read without a workspace, source or destination filter")
		rc.h.conf.multiConsumer = true
		read, err = rc.pipelineKeyOf(&GetQueryParams{Consumer: "d1"})
		require.NoError(t, err)
		require.Equal(t, pipelineKey{FilterType: noneFilterType, Consumer: "d1"}, read, "and the consumer, on a multi-consumer handle")
		rc.h.conf.multiConsumer = false
		for name, params := range map[string]GetQueryParams{
			"two custom values":                  {CustomValFilters: []string{"WEBHOOK", "S3"}, WorkspaceID: "ws"},
			"partition filters":                  {WorkspaceID: "ws", PartitionFilters: []string{"p1"}},
			"a workspace and a parameter filter": {WorkspaceID: "ws", ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: "d1"}}},
			"two parameter filters":              {ParameterFilters: []ParameterFilterT{{Name: "source_id", Value: "s1"}, {Name: "destination_id", Value: "d1"}}},
			"another parameter filter":           {ParameterFilters: []ParameterFilterT{{Name: "source_category", Value: "webhook"}}},
		} {
			_, err = rc.pipelineKeyOf(&params)
			require.Error(t, err, "no pipeline key holds a read with %s", name)
		}
	})

	t.Run("a read that filters on more than a pipeline key holds disables the read cursor", func(t *testing.T) {
		rc := &readCursor{h: &Handle{}, log: logger.NOP, disabledStat: stats.NOP.NewStat("", stats.GaugeType), pipelines: map[pipelineKey]*pipeline{}}
		params := GetQueryParams{WorkspaceID: "ws", PartitionFilters: []string{"p1"}}
		p, _ := rc.startRead([]string{Unprocessed.State}, &params, true)
		require.Nil(t, p)
		require.True(t, rc.disabled.Load())
	})

	t.Run("a status has the key of its job for each key shape", func(t *testing.T) {
		rc := &readCursor{h: &Handle{}, pipelines: map[pipelineKey]*pipeline{}}
		rc.getPipeline(key)
		rc.getPipeline(pipelineKey{FilterType: sourceFilterType, FilterValue: "s1"})
		rc.getPipeline(pipelineKey{CustomVal: "WEBHOOK", FilterType: destinationFilterType, FilterValue: "d2"}) // the shape of key
		rc.getPipeline(pipelineKey{FilterType: noneFilterType})

		status := &JobStatusT{CustomVal: "WEBHOOK", WorkspaceId: "ws", JobParameters: []byte(`{"source_id":"s1","destination_id":"d1"}`)}
		keys, err := rc.appendStatusKeys(nil, rc.shapes, status)
		require.NoError(t, err)
		require.Equal(t, []pipelineKey{key, {FilterType: sourceFilterType, FilterValue: "s1"}, {FilterType: noneFilterType}}, keys, "no workspace shape exists")

		_, err = rc.appendStatusKeys(nil, rc.shapes, &JobStatusT{CustomVal: "WEBHOOK", WorkspaceId: "ws", JobParameters: []byte(`{"source_id":"s1"}`)})
		require.ErrorContains(t, err, `job 0, state "", custom value "WEBHOOK", workspace "ws", source "s1", destination "", consumer ""`, "the status lacks the destination of its job")
	})

	t.Run("on a multi-consumer handle a status has the keys of its consumer", func(t *testing.T) {
		rc := &readCursor{h: &Handle{}, pipelines: map[pipelineKey]*pipeline{}}
		rc.h.conf.multiConsumer = true
		rc.getPipeline(pipelineKey{FilterType: noneFilterType, Consumer: "d1"})
		status := &JobStatusT{CustomVal: "PROC", WorkspaceId: "ws", JobParameters: []byte(`{"source_id":"s1"}`), Consumer: "d2"}
		keys, err := rc.appendStatusKeys(nil, rc.shapes, status)
		require.NoError(t, err)
		require.Equal(t, []pipelineKey{{FilterType: noneFilterType, Consumer: "d2"}}, keys)
		status.Consumer = ""
		keys, err = rc.appendStatusKeys(nil, rc.shapes, status)
		require.NoError(t, err)
		require.Equal(t, []pipelineKey{{FilterType: noneFilterType}}, keys, "the empty consumer is the default one")
	})

	t.Run("a status needs the custom value, workspace and parameters of its job, whatever the key shapes", func(t *testing.T) {
		rc := &readCursor{h: &Handle{}, pipelines: map[pipelineKey]*pipeline{}}
		complete := JobStatusT{CustomVal: "WEBHOOK", WorkspaceId: "ws", JobParameters: []byte(`{"source_id":"s1"}`)}
		_, err := rc.appendStatusKeys(nil, rc.shapes, &complete)
		require.NoError(t, err)
		for name, lack := range map[string]func(*JobStatusT){
			"custom value": func(s *JobStatusT) { s.CustomVal = "" },
			"workspace":    func(s *JobStatusT) { s.WorkspaceId = "" },
			"parameters":   func(s *JobStatusT) { s.JobParameters = nil },
		} {
			status := complete
			lack(&status)
			_, err := rc.appendStatusKeys(nil, rc.shapes, &status)
			require.Error(t, err, "the status lacks the %s of its job", name)
		}
	})
}

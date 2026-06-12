package jobsdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

// TestMultiConsumerCompaction is the end-to-end test for multi-consumer compaction.
// It runs the same scenario through all three execution paths and verifies
// the three MC invariants in each:
//
//  1. Only jobs pending for at least one consumer are copied.
//  2. All consumer statuses (terminal included) for moved jobs are carried over.
//  3. The destination registry is seeded with exactly the consumers of moved jobs.
func TestMultiConsumerCompaction(t *testing.T) {
	_ = startPostgres(t)

	newMCJobDB := func(t *testing.T) (*Handle, *config.Config) {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 100000)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
		}
		jd.conf.multiConsumer = true
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd, c
	}

	createDS := func(t *testing.T, jd *Handle, idx string) {
		t.Helper()
		require.NoError(t, jd.dsListLock.WithLockInCtx(context.Background(), func(l lock.LockToken) error {
			dsList, _ := jd.dsList.snapshot()
			if err := jd.WithTx(context.Background(), func(txn *tx.Tx) error {
				return jd.addNewDSInTx(context.Background(), txn, l, dsList, newDataSet(jd.tablePrefix, idx))
			}); err != nil {
				return err
			}
			return jd.doRefreshDSRangeList(l)
		}))
	}

	mcStatus := func(job *JobT, consumer, state string) *JobStatusT {
		return &JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   job.WorkspaceId,
			CustomVal:     job.CustomVal,
			Consumer:      consumer,
		}
	}

	for _, tc := range []struct {
		name            string
		nonBlocking     bool
		deferStatusLock bool
	}{
		{"blocking", false, false},
		{"non-blocking", true, false},
		{"non-blocking-deferred", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			jd, c := newMCJobDB(t)
			c.Set("JobsDB."+jd.tablePrefix+".nonBlockingCompaction", tc.nonBlocking)
			c.Set("JobsDB."+jd.tablePrefix+".compactionDeferStatusLock", tc.deferStatusLock)
			c.Set("JobsDB."+jd.tablePrefix+".maxDSRetention", "1ms")
			c.Set("JobsDB."+jd.tablePrefix+".compactionMinDSAge", "0s")
			// Disable status cleanup so it doesn't interfere with the compaction assertions.
			c.Set("JobsDB."+jd.tablePrefix+".skipMultiConsumerStatusCompaction", true)

			// DS1: 4 jobs with mixed terminal/non-terminal consumer states.
			//   job0: [A,B] A=succeeded B=succeeded → all terminal → NOT copied
			//   job1: [A,B] A=succeeded B=failed    → A done, B pending → COPY
			//   job2: [A,B] A=failed    B=failed    → both pending → COPY
			//   job3: [A]   no status               → unprocessed → COPY
			jobs := genJobs(defaultWorkspaceID, "mc", 4, 1)
			jobs[0].Consumers = []string{"A", "B"}
			jobs[1].Consumers = []string{"A", "B"}
			jobs[2].Consumers = []string{"A", "B"}
			jobs[3].Consumers = []string{"A"}
			require.NoError(t, jd.Store(ctx, jobs))

			require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
				mcStatus(jobs[0], "A", Succeeded.State),
				mcStatus(jobs[0], "B", Succeeded.State),
				mcStatus(jobs[1], "A", Succeeded.State),
				mcStatus(jobs[1], "B", Failed.State),
				mcStatus(jobs[2], "A", Failed.State),
				mcStatus(jobs[2], "B", Failed.State),
			}))

			// Acquire a reader before compaction so async drops are blocked long enough
			// for the assertions below to inspect the destination tables.
			_, _, release, err := jd.acquireDSListForRead(ctx)
			require.NoError(t, err)
			defer release()

			createDS(t, jd, "2") // empty last DS — makes DS1 non-last and eligible

			// Let the statuses age past maxDSRetention=1ms before running compaction.
			time.Sleep(5 * time.Millisecond)

			require.NoError(t, jd.doCompaction(ctx))

			destDS := newDataSet(jd.tablePrefix, "1_1")

			// Invariant 1: only the 3 non-fully-terminal jobs are copied.
			var jobCount int64
			require.NoError(t, jd.dbHandle.QueryRow(
				fmt.Sprintf(`SELECT count(*) FROM %q`, destDS.JobTable),
			).Scan(&jobCount))
			require.EqualValues(t, 3, jobCount)

			var job0Count int64
			require.NoError(t, jd.dbHandle.QueryRow(
				fmt.Sprintf(`SELECT count(*) FROM %q WHERE job_id = $1`, destDS.JobTable),
				jobs[0].JobID,
			).Scan(&job0Count))
			require.Zero(t, job0Count, "fully-terminal job must not be copied")

			// Invariant 2: only statuses for pending consumers are carried over.
			// job1/A (succeeded) is trimmed from consumers, so its status is excluded.
			// Remaining: job1/B=failed, job2/A=failed, job2/B=failed = 3 rows.
			// job3 had no status, so it contributes 0.
			var statusCount int64
			require.NoError(t, jd.dbHandle.QueryRow(
				fmt.Sprintf(`SELECT count(*) FROM %q`, destDS.JobStatusTable),
			).Scan(&statusCount))
			require.EqualValues(t, 3, statusCount, "only pending consumers' statuses must be carried over")

			// Invariant 3: destination registry = exact consumers of moved jobs = {A, B}.
			statusRows, err := jd.dbHandle.QueryContext(ctx,
				fmt.Sprintf(`SELECT consumer FROM %q ORDER BY consumer`, destDS.consumersRegistryTable()))
			require.NoError(t, err)
			var consumers []string
			for statusRows.Next() {
				var consumer string
				require.NoError(t, statusRows.Scan(&consumer))
				consumers = append(consumers, consumer)
			}
			require.NoError(t, statusRows.Err())
			_ = statusRows.Close()
			require.Equal(t, []string{"A", "B"}, consumers)
		})
	}
}

// TestMCCleanStatusTable verifies that cleanStatusTable uses DISTINCT ON (job_id, consumer)
// for MC handles, keeping exactly one status row per (job, consumer) pair rather than one per job.
func TestMCCleanStatusTable(t *testing.T) {
	_ = startPostgres(t)
	ctx := context.Background()

	c := config.New()
	jd := &Handle{
		TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
		TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
		config:            c,
	}
	jd.conf.multiConsumer = true
	require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
	defer jd.TearDown()

	// 2 jobs, each assigned to consumers A and B.
	jobs := genJobs(defaultWorkspaceID, "mc", 2, 1)
	jobs[0].Consumers = []string{"A", "B"}
	jobs[1].Consumers = []string{"A", "B"}
	require.NoError(t, jd.Store(ctx, jobs))

	mcStatus := func(job *JobT, consumer, state string) *JobStatusT {
		return &JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   job.WorkspaceId,
			CustomVal:     job.CustomVal,
			Consumer:      consumer,
		}
	}

	// Insert multiple rounds of statuses per (job, consumer).
	// Total inserted: 4 + 3 + 1 = 8 rows. After dedup: 4 (one per pair).
	//
	// Insertion order (affects which row is latest per pair):
	//   Round 1 (ids 1-4): job0/A=executing, job0/B=executing, job1/A=executing, job1/B=failed
	//   Round 2 (ids 5-7): job0/A=failed,    job0/B=failed,    job1/A=succeeded
	//   Round 3 (id 8):    job0/A=executing  ← latest for job0/A overrides round-2 entry
	require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
		mcStatus(jobs[0], "A", "executing"),
		mcStatus(jobs[0], "B", "executing"),
		mcStatus(jobs[1], "A", "executing"),
		mcStatus(jobs[1], "B", "failed"),
	}))
	require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
		mcStatus(jobs[0], "A", "failed"),
		mcStatus(jobs[0], "B", "failed"),
		mcStatus(jobs[1], "A", "succeeded"),
	}))
	require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
		mcStatus(jobs[0], "A", "executing"),
	}))

	ds := jd.getDSListSnapshot()[0]

	var beforeCount int64
	require.NoError(t, jd.dbHandle.QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %q`, ds.JobStatusTable),
	).Scan(&beforeCount))
	require.EqualValues(t, 8, beforeCount)

	require.NoError(t, jd.withMaintenanceTx(ctx, func(txn *tx.Tx) error {
		_, err := jd.cleanStatusTable(ctx, txn, ds.JobStatusTable, false)
		return err
	}))

	// After cleanup: one row per (job, consumer) pair = 4 rows.
	type statusRow struct {
		jobID    int64
		consumer string
		state    string
	}
	dbRows, err := jd.dbHandle.QueryContext(ctx,
		fmt.Sprintf(`SELECT job_id, consumer, job_state FROM %q ORDER BY job_id, consumer`, ds.JobStatusTable))
	require.NoError(t, err)
	var got []statusRow
	for dbRows.Next() {
		var r statusRow
		require.NoError(t, dbRows.Scan(&r.jobID, &r.consumer, &r.state))
		got = append(got, r)
	}
	require.NoError(t, dbRows.Err())
	_ = dbRows.Close()
	require.Equal(t, []statusRow{
		{jobs[0].JobID, "A", "executing"}, // latest: round-3 executing
		{jobs[0].JobID, "B", "failed"},    // latest: round-2 failed
		{jobs[1].JobID, "A", "succeeded"}, // latest: round-2 succeeded
		{jobs[1].JobID, "B", "failed"},    // only round-1 entry
	}, got, "one row per (job, consumer) pair must survive, with the latest state")
}

// TestMCSkipStatusCompaction verifies that:
//   - The MC default (skipMultiConsumerStatusCompaction=true) makes cleanupStatusTables a no-op.
//   - When forced to false, the MC-correct DISTINCT ON (job_id, consumer) deduplication runs.
func TestMCSkipStatusCompaction(t *testing.T) {
	_ = startPostgres(t)
	ctx := context.Background()

	newMCJobDB := func(t *testing.T, skip bool) *Handle {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 100000)
		c.Set("JobsDB.jobStatusMigrateThreshold", 0.1) // low threshold so any excess triggers cleanup
		c.Set("JobsDB.skipMultiConsumerStatusCompaction", skip)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
		}
		jd.conf.multiConsumer = true
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd
	}

	// populate creates n jobs (consumers [A,B]) and inserts 5 failed statuses per (job, consumer).
	// That is 10n total status rows, giving a ratio of 5.0 against the 0.1 threshold.
	populate := func(t *testing.T, jd *Handle, n int) (ds dataSetT, rowsBefore int64) {
		t.Helper()
		jobs := genJobs(defaultWorkspaceID, "mc", n, 1)
		for i := range jobs {
			jobs[i].Consumers = []string{"A", "B"}
		}
		require.NoError(t, jd.Store(ctx, jobs))

		for range 5 {
			statuses := make([]*JobStatusT, 0, 2*n)
			for _, job := range jobs {
				for _, consumer := range []string{"A", "B"} {
					statuses = append(statuses, &JobStatusT{
						JobID:         job.JobID,
						JobState:      Failed.State,
						AttemptNum:    1,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "999",
						ErrorResponse: []byte(`{}`),
						Parameters:    []byte(`{}`),
						WorkspaceId:   job.WorkspaceId,
						CustomVal:     job.CustomVal,
						Consumer:      consumer,
					})
				}
			}
			require.NoError(t, jd.UpdateJobStatus(ctx, statuses))
		}

		ds = jd.getDSListSnapshot()[0]
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT count(*) FROM %q`, ds.JobStatusTable),
		).Scan(&rowsBefore))
		// ANALYZE so pg_class.reltuples reflects the actual row counts for the cleanup check.
		_, err := jd.dbHandle.Exec(fmt.Sprintf(`ANALYZE %q, %q`, ds.JobTable, ds.JobStatusTable))
		require.NoError(t, err)
		return ds, rowsBefore
	}

	t.Run("skip=true (MC default) — cleanupStatusTables is a no-op", func(t *testing.T) {
		jd := newMCJobDB(t, true)
		ds, rowsBefore := populate(t, jd, 10)

		require.NoError(t, jd.cleanupStatusTables(ctx, jd.getDSListSnapshot()))

		var rowsAfter int64
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT count(*) FROM %q`, ds.JobStatusTable),
		).Scan(&rowsAfter))
		require.Equal(t, rowsBefore, rowsAfter, "skip=true must not touch the status table")
	})

	t.Run("skip=false — deduplicates by (job_id, consumer)", func(t *testing.T) {
		jd := newMCJobDB(t, false)
		ds, rowsBefore := populate(t, jd, 10)
		require.EqualValues(t, 100, rowsBefore) // 10 jobs × 2 consumers × 5 rounds

		require.NoError(t, jd.cleanupStatusTables(ctx, jd.getDSListSnapshot()))

		var rowsAfter int64
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT count(*) FROM %q`, ds.JobStatusTable),
		).Scan(&rowsAfter))
		// One row per (job, consumer) pair: 10 × 2 = 20.
		require.EqualValues(t, 20, rowsAfter, "one row per (job, consumer) pair must survive")
	})
}

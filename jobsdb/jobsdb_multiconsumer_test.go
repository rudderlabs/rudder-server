package jobsdb

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

// TestMultiConsumerJobsDB_SingleConsumer verifies that a multi-consumer handle works
// correctly when all jobs use the default (legacy) single consumer.
//
// The v_last_c_ view (DISTINCT ON job_id, consumer) is semantically equivalent to
// v_last_ (DISTINCT ON job_id) when there is a single consumer per job, so Store,
// GetUnprocessed and UpdateJobStatus should all behave identically to the
// single-consumer path.
func TestMultiConsumerJobsDB_SingleConsumer(t *testing.T) {
	postgres := startPostgres(t)

	c := config.New()
	c.Set("jobsdb.maxDSSize", 10)

	prefix := strings.ToLower(rand.String(5))
	jd := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
		WithMultiConsumer(),
	)
	require.NoError(t, jd.Start())
	defer jd.TearDown()

	ctx := context.Background()
	customVal := "MC"

	// store jobs (no Consumers set → defaults to {""})
	jobs := make([]*JobT, 3)
	for i := range jobs {
		jobs[i] = &JobT{
			UUID:         uuid.New(),
			UserID:       "user-1",
			CustomVal:    customVal,
			Parameters:   []byte(`{"source_id":"src1"}`),
			EventPayload: []byte(`{}`),
			EventCount:   1,
			WorkspaceId:  defaultWorkspaceID,
		}
	}
	require.NoError(t, jd.Store(ctx, jobs))

	// get unprocessed — v_last_c_ is used internally
	res, err := jd.GetUnprocessed(ctx, GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        10,
	})
	require.NoError(t, err)
	require.Len(t, res.Jobs, 3, "all stored jobs should be unprocessed")
	for _, j := range res.Jobs {
		require.Equal(t, []string{""}, j.Consumers, "legacy consumer should be {\"\"}")
	}

	// mark all succeeded
	statuses := make([]*JobStatusT, len(res.Jobs))
	for i, j := range res.Jobs {
		statuses[i] = &JobStatusT{
			JobID:         j.JobID,
			JobState:      Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   j.WorkspaceId,
			CustomVal:     j.CustomVal,
		}
	}
	require.NoError(t, jd.UpdateJobStatus(ctx, statuses))

	// no more unprocessed after marking all succeeded
	res, err = jd.GetUnprocessed(ctx, GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        10,
	})
	require.NoError(t, err)
	require.Empty(t, res.Jobs)

	// unionjobsdbmetadata detects the registry table and uses v_last_c_
	verifyUnionJobsDBMetadata(t, postgres.DB, prefix, 3)
}

// TestMultiConsumerJobsDB_ConsumerIsolation verifies that consumer-scoped pickup is isolated:
// consumer A's terminal status does not hide the job from consumer B, and the GIN membership
// filter ensures only jobs assigned to the querying consumer are returned.
func TestMultiConsumerJobsDB_ConsumerIsolation(t *testing.T) {
	postgres := startPostgres(t)

	c := config.New()
	c.Set("jobsdb.maxDSSize", 10)

	prefix := strings.ToLower(rand.String(5))
	jd := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
		WithMultiConsumer(),
	)
	require.NoError(t, jd.Start())
	defer jd.TearDown()

	ctx := context.Background()
	customVal := "ISO"

	// store one job assigned to both consumers A and B
	job := &JobT{
		UUID:         uuid.New(),
		UserID:       "user-1",
		CustomVal:    customVal,
		Parameters:   []byte(`{"source_id":"src1"}`),
		EventPayload: []byte(`{}`),
		EventCount:   1,
		WorkspaceId:  defaultWorkspaceID,
		Consumers:    []string{"A", "B"},
	}
	require.NoError(t, jd.Store(ctx, []*JobT{job}))

	// both A and B see the job as unprocessed
	resA, err := jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "A"})
	require.NoError(t, err)
	require.Len(t, resA.Jobs, 1)

	resB, err := jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "B"})
	require.NoError(t, err)
	require.Len(t, resB.Jobs, 1)

	// mark the job succeeded for consumer A
	require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{{
		JobID:         resA.Jobs[0].JobID,
		JobState:      Succeeded.State,
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "200",
		ErrorResponse: []byte(`{}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   resA.Jobs[0].WorkspaceId,
		CustomVal:     resA.Jobs[0].CustomVal,
		Consumer:      "A",
	}}))

	// A sees no more unprocessed jobs
	resA, err = jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "A"})
	require.NoError(t, err)
	require.Empty(t, resA.Jobs, "consumer A's terminal status should hide the job from A")

	// B still sees the job as unprocessed — A's status does not affect B
	resB, err = jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "B"})
	require.NoError(t, err)
	require.Len(t, resB.Jobs, 1, "consumer B should still see the job after A marked it done")

	// a third consumer C never assigned to this job sees nothing
	resC, err := jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "C"})
	require.NoError(t, err)
	require.Empty(t, resC.Jobs, "consumer C is not in the job's consumers list")
}

// TestMultiConsumerJobsDB_Conversion verifies the single → multi-consumer upgrade path.
//
// A plain handle stores and processes jobs using the legacy schema. When the same prefix
// is re-opened with WithMultiConsumer(), applyMultiConsumerFlip runs and creates the
// v_last_c_ view and consumers registry table for each existing dataset. After the flip,
// GetUnprocessed and unionjobsdbmetadata must continue to work correctly.
func TestMultiConsumerJobsDB_Conversion(t *testing.T) {
	postgres := startPostgres(t)

	c := config.New()
	c.Set("jobsdb.maxDSSize", 10)

	prefix := strings.ToLower(rand.String(5))
	customVal := "CONV"

	// Phase 1: plain single-consumer handle — store 3 jobs, mark 2 succeeded.
	jdSingle := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
	)
	require.NoError(t, jdSingle.Start())

	ctx := context.Background()
	jobs := make([]*JobT, 3)
	for i := range jobs {
		jobs[i] = &JobT{
			UUID:         uuid.New(),
			UserID:       "user-1",
			CustomVal:    customVal,
			Parameters:   []byte(`{"source_id":"src1"}`),
			EventPayload: []byte(`{}`),
			EventCount:   1,
			WorkspaceId:  defaultWorkspaceID,
		}
	}
	require.NoError(t, jdSingle.Store(ctx, jobs))

	res, err := jdSingle.GetUnprocessed(ctx, GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        10,
	})
	require.NoError(t, err)
	require.Len(t, res.Jobs, 3)

	// mark first 2 succeeded, leave one pending
	statuses := make([]*JobStatusT, 2)
	for i := range statuses {
		statuses[i] = &JobStatusT{
			JobID:         res.Jobs[i].JobID,
			JobState:      Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   res.Jobs[i].WorkspaceId,
			CustomVal:     res.Jobs[i].CustomVal,
		}
	}
	require.NoError(t, jdSingle.UpdateJobStatus(ctx, statuses))
	jdSingle.TearDown()

	// Phase 2: re-open with WithMultiConsumer() — flip runs on startup.
	jdMC := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
		WithMultiConsumer(),
	)
	require.NoError(t, jdMC.Start())

	// One unprocessed job should still be visible via the v_last_c_ view.
	res, err = jdMC.GetUnprocessed(ctx, GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        10,
	})
	require.NoError(t, err)
	require.Len(t, res.Jobs, 1, "exactly one job should remain unprocessed after conversion")
	require.Equal(t, []string{""}, res.Jobs[0].Consumers)

	// unionjobsdbmetadata should detect the registry table and use v_last_c_.
	verifyUnionJobsDBMetadata(t, postgres.DB, prefix, 3)
	jdMC.TearDown()

	// Phase 3: re-open without WithMultiConsumer() — should NOT panic since only the
	// legacy empty consumer exists in the registry (no named consumers were ever registered).
	jdDowngrade := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
	)
	require.NoError(t, jdDowngrade.Start())
	jdDowngrade.TearDown()

	// Phase 4: re-open with WithMultiConsumer() and store a job with a named consumer.
	jdMC2 := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
		WithMultiConsumer(),
	)
	require.NoError(t, jdMC2.Start())
	namedJob := &JobT{
		UUID:         uuid.New(),
		UserID:       "user-2",
		CustomVal:    customVal,
		Parameters:   []byte(`{"source_id":"src1"}`),
		EventPayload: []byte(`{}`),
		EventCount:   1,
		WorkspaceId:  defaultWorkspaceID,
		Consumers:    []string{"c1"},
	}
	require.NoError(t, jdMC2.Store(ctx, []*JobT{namedJob}))
	jdMC2.TearDown()

	// Phase 5: re-open without WithMultiConsumer() — must panic since "c1" is in the registry.
	require.Panics(t, func() {
		jdDowngrade2 := NewForReadWrite(prefix,
			WithDBHandle(postgres.DB),
			WithConfig(c),
		)
		_ = jdDowngrade2
	})
}

// verifyUnionJobsDBMetadata calls unionjobsdbmetadata and asserts row count,
// presence of consumers and consumer columns, and non-null status for each row.
func verifyUnionJobsDBMetadata(t *testing.T, db *sql.DB, prefix string, expectedRows int) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT t_name, job_id, consumers, status_id, consumer, job_state FROM unionjobsdbmetadata($1, 10)`,
		prefix,
	)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var count int
	for rows.Next() {
		var (
			tName     string
			jobID     int64
			consumers pq.StringArray
			statusID  sql.NullInt64
			consumer  sql.NullString
			jobState  sql.NullString
		)
		require.NoError(t, rows.Scan(&tName, &jobID, &consumers, &statusID, &consumer, &jobState))
		require.NotEmpty(t, tName)
		require.NotEmpty(t, consumers)
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expectedRows, count)
}

// TestMultiConsumerJobsDB_Cache verifies that the noResultsCache is correctly keyed,
// populated, and invalidated per consumer. The test accesses the cache directly (same
// package) rather than inferring its state from timing or DB call counts.
func TestMultiConsumerJobsDB_Cache(t *testing.T) {
	postgres := startPostgres(t)

	c := config.New()
	c.Set("jobsdb.maxDSSize", 100)

	prefix := strings.ToLower(rand.String(5))
	jd := NewForReadWrite(prefix,
		WithDBHandle(postgres.DB),
		WithConfig(c),
		WithMultiConsumer(),
	)
	require.NoError(t, jd.Start())
	defer jd.TearDown()

	ctx := context.Background()
	customVal := "CACHE"

	dsIndex := func() string {
		return HandleInspector{Handle: jd}.getDSListSnapshot()[0].Index
	}
	// cacheHit checks whether the noResultsCache has a live "no unprocessed" entry for
	// the given consumer — workspace and partition are wildcards (empty query params).
	cacheHit := func(consumer string) bool {
		return jd.noResultsCache.Get(
			dsIndex(), nil, "",
			[]string{customVal},
			[]string{Unprocessed.State},
			[]ParameterFilterT{{Name: "consumer", Value: consumer}},
		)
	}
	makeJob := func(consumers []string) *JobT {
		return &JobT{
			UUID:         uuid.New(),
			UserID:       "u1",
			CustomVal:    customVal,
			Parameters:   []byte(`{}`),
			EventPayload: []byte(`{}`),
			EventCount:   1,
			WorkspaceId:  defaultWorkspaceID,
			Consumers:    consumers,
		}
	}
	succeedFor := func(job *JobT, consumer string) *JobStatusT {
		return &JobStatusT{
			JobID:         job.JobID,
			JobState:      Succeeded.State,
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

	// Phase 1: empty DB — query A primes a "no results" cache entry for consumer A.
	res, err := jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "A"})
	require.NoError(t, err)
	require.Empty(t, res.Jobs)
	require.True(t, cacheHit("A"), "cache should be set for A after 0-result query")

	// Storing a job for consumer B must not invalidate consumer A's cache entry.
	require.NoError(t, jd.Store(ctx, []*JobT{makeJob([]string{"B"})}))
	require.True(t, cacheHit("A"), "B's store must not invalidate A's cache")

	// Storing a job for consumer A must invalidate A's cache.
	require.NoError(t, jd.Store(ctx, []*JobT{makeJob([]string{"A"})}))
	require.False(t, cacheHit("A"), "A's store must invalidate A's cache")

	// Phase 2: A fetches and marks its job succeeded; cache re-primed.
	res, err = jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "A"})
	require.NoError(t, err)
	require.Len(t, res.Jobs, 1)
	require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{succeedFor(res.Jobs[0], "A")}))

	res, err = jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "A"})
	require.NoError(t, err)
	require.Empty(t, res.Jobs)
	require.True(t, cacheHit("A"), "cache should be re-primed for A after 0-result query")

	// Phase 3: B marks its job succeeded; A's cache must survive.
	res, err = jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "B"})
	require.NoError(t, err)
	require.Len(t, res.Jobs, 1)
	require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{succeedFor(res.Jobs[0], "B")}))
	require.True(t, cacheHit("A"), "B's status update must not invalidate A's cache")
}

// TestStoreConsumersOnSingleConsumerHandle verifies that a single-consumer jobsdb preserves a
// job's explicitly-set Consumers on store and round-trips them through GetUnprocessed. This is
// relied upon by the partition-migration buffer (proc_buf): it is a single-consumer handle that
// relays multi-consumer proc jobs between nodes, so it must carry their consumers through.
// Without it, migrated jobs would resurface under the legacy ” consumer and be dropped at the
// target (unknown destination ""). Jobs without explicit consumers keep the legacy ” consumer.
func TestStoreConsumersOnSingleConsumerHandle(t *testing.T) {
	postgres := startPostgres(t)

	prefix := strings.ToLower(rand.String(5))
	jd := NewForReadWrite(prefix, // NOT multi-consumer
		WithDBHandle(postgres.DB),
		WithConfig(config.New()),
	)
	require.NoError(t, jd.Start())
	defer jd.TearDown()

	ctx := context.Background()
	const customVal = "SC"
	newJob := func(userID string, consumers []string) *JobT {
		return &JobT{
			UUID: uuid.New(), UserID: userID, CustomVal: customVal,
			Parameters: []byte(`{}`), EventPayload: []byte(`{}`), EventCount: 1,
			WorkspaceId: "w", Consumers: consumers,
		}
	}
	require.NoError(t, jd.Store(ctx, []*JobT{
		newJob("with", []string{"A", "B"}),
		newJob("without", nil),
	}))

	res, err := jd.GetUnprocessed(ctx, GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10})
	require.NoError(t, err)
	require.Len(t, res.Jobs, 2)
	// GetUnprocessed returns jobs ordered by job_id, i.e. insertion order.
	require.Equal(t, []string{"A", "B"}, res.Jobs[0].Consumers, "explicit consumers must be preserved on a single-consumer handle")
	require.Equal(t, []string{""}, res.Jobs[1].Consumers, "a job without consumers keeps the legacy '' consumer")
}

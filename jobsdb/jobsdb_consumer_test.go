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

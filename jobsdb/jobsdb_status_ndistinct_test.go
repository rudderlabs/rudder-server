package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

func TestDefaultStatusJobIDNDistinct(t *testing.T) {
	for prefix, expected := range map[string]float64{
		"gw":       -0.5,
		"gw_buf":   -0.5,
		"proc":     -0.1,
		"proc_buf": -0.5,
		"rt":       -0.5,
		"batch_rt": -0.5,
		"arc":      -0.5,
	} {
		require.Equal(t, expected, defaultStatusJobIDNDistinct(prefix), prefix)
	}
}

func TestStatusJobIDNDistinct(t *testing.T) {
	postgres := startPostgres(t)

	for _, tc := range []struct {
		name     string
		value    any
		expected sql.NullString
	}{
		{name: "default", expected: sql.NullString{String: "-0.5", Valid: true}},
		{name: "configured", value: -0.25, expected: sql.NullString{String: "-0.25", Valid: true}},
		{name: "zero disables it", value: 0.0},
		{name: "values out of range are skipped", value: 5.0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := strings.ToLower(rand.String(5))
			c := config.New()
			if tc.value != nil {
				c.Set(fmt.Sprintf("JobsDB.%s.statusJobIDNDistinct", prefix), tc.value)
			}
			jd := NewForReadWrite(prefix, WithDBHandle(postgres.DB), WithConfig(c))
			require.NoError(t, jd.Start())
			defer jd.TearDown()

			var options sql.NullString
			require.NoError(t, postgres.DB.QueryRow(
				`SELECT (SELECT option_value FROM pg_options_to_table(attoptions) WHERE option_name = 'n_distinct') FROM pg_attribute WHERE attrelid = $1::regclass AND attname = 'job_id'`,
				prefix+"_job_status_1",
			).Scan(&options))
			require.Equal(t, tc.expected, options)
		})
	}

	t.Run("ANALYZE stores the pinned value", func(t *testing.T) {
		prefix := strings.ToLower(rand.String(5))
		jd := NewForReadWrite(prefix, WithDBHandle(postgres.DB), WithConfig(config.New()))
		require.NoError(t, jd.Start())
		defer jd.TearDown()

		table := prefix + "_job_status_1"
		_, err := postgres.DB.Exec(fmt.Sprintf(
			`INSERT INTO %[1]q (job_id, job_state, attempt) SELECT 1 + g %% 4, 'failed', g FROM generate_series(1, 400) g;
			ANALYZE %[1]q`, table))
		require.NoError(t, err)

		var nDistinct float64
		require.NoError(t, postgres.DB.QueryRow(
			`SELECT n_distinct FROM pg_stats WHERE tablename = $1 AND attname = 'job_id'`, table,
		).Scan(&nDistinct))
		require.Equal(t, -0.5, nDistinct)
	})
}

// TestStatusJobIDNDistinctPickup loads a status table through the size range where stale statistics make the
// planner walk the id index backwards, and picks up jobs after every load. Autovacuum is off, so the statistics
// stay those of the few retried jobs that the table held when it was analyzed.
func TestStatusJobIDNDistinctPickup(t *testing.T) {
	const (
		jobs            = 120_000
		retriedJobs     = 4
		retriedStatuses = 432
		jobsPerLoad     = 2_500
		maxStatuses     = 70_000
		pickupTimeout   = 5 * time.Second
	)
	postgres := startPostgres(t)
	_, err := postgres.DB.Exec(`ALTER SYSTEM SET random_page_cost = 1.1`)
	require.NoError(t, err)
	_, err = postgres.DB.Exec(`SELECT pg_reload_conf()`)
	require.NoError(t, err)

	// pickupsUntilStall loads the status table and returns true as soon as a pickup hits the timeout.
	pickupsUntilStall := func(t *testing.T, c *config.Config) bool {
		t.Helper()
		prefix := strings.ToLower(rand.String(5))
		c.Set("JobsDB.maxDSSize", 2*jobs)
		jd := NewForReadWrite(prefix, WithDBHandle(postgres.DB), WithConfig(c))
		require.NoError(t, jd.Start())
		defer jd.TearDown()

		jobsTable, statusTable := prefix+"_jobs_1", prefix+"_job_status_1"
		_, err := postgres.DB.Exec(fmt.Sprintf(
			`ALTER TABLE %[2]q SET (autovacuum_enabled = false);
			INSERT INTO %[1]q (uuid, user_id, custom_val, parameters, event_payload)
				SELECT gen_random_uuid(), 'user', 'BQ', '{"destination_id": "dest"}', '{"key": "value"}' FROM generate_series(1, %[3]d);
			INSERT INTO %[2]q (job_id, job_state, attempt, error_code)
				SELECT 1 + g %% %[4]d, 'failed', g, '500' FROM generate_series(1, %[5]d) g;
			ANALYZE %[1]q;
			ANALYZE %[2]q`,
			jobsTable, statusTable, jobs, retriedJobs, retriedStatuses,
		))
		require.NoError(t, err)

		pickup := func() (stalled bool) {
			ctx, cancel := context.WithTimeout(context.Background(), pickupTimeout)
			defer cancel()
			res, err := jd.GetJobs(ctx, []string{Failed.State, Unprocessed.State}, GetQueryParams{
				CustomValFilters: []string{"BQ"},
				ParameterFilters: []ParameterFilterT{{Name: "destination_id", Value: "dest"}},
				JobsLimit:        20_000,
				PayloadSizeLimit: 1 << 30,
			})
			if ctx.Err() != nil {
				return true
			}
			require.NoError(t, err)
			require.NotEmpty(t, res.Jobs)
			return false
		}

		firstJob := retriedJobs + 1
		for statuses := retriedStatuses; statuses < maxStatuses; firstJob += jobsPerLoad {
			for _, state := range []string{Executing.State, Succeeded.State} {
				_, err := postgres.DB.Exec(fmt.Sprintf(
					`INSERT INTO %q (job_id, job_state, attempt) SELECT g, $1, 1 FROM generate_series($2::bigint, $3::bigint) g`, statusTable,
				), state, firstJob, firstJob+jobsPerLoad-1)
				require.NoError(t, err)
				statuses += jobsPerLoad
				if pickup() {
					return true
				}
			}
		}
		return false
	}

	t.Run("stalls without a pinned n_distinct", func(t *testing.T) {
		c := config.New()
		c.Set("JobsDB.statusJobIDNDistinct", 0.0)
		require.True(t, pickupsUntilStall(t, c))
	})

	t.Run("stays fast with the default", func(t *testing.T) {
		require.False(t, pickupsUntilStall(t, config.New()))
	})
}

package rsources

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)

func TestFailedRecordsPerformanceTest(t *testing.T) {
	var noPaginationDur time.Duration
	var paginationDur time.Duration

	total := 100_000
	pageSize := 10_000
	t.Run("100k records no pagination", func(t *testing.T) {
		noPaginationDur = RunFailedRecordsPerformanceTest(t, total, 0)
		t.Logf("100k records no pagination took %s", noPaginationDur)
	})
	t.Run("100k records with 10k pagination", func(t *testing.T) {
		paginationDur = RunFailedRecordsPerformanceTest(t, total, pageSize)
		t.Logf("100k records with 10k pagination took %s", paginationDur)
	})

	t.Run("pagination vs no pagination comparison", func(t *testing.T) {
		pages := total / pageSize
		if noPaginationDur > 0 && int(paginationDur/noPaginationDur) > pages {
			t.Errorf("Pagination time (%s) is more than %dx slower than no pagination time (%s)", paginationDur, pages, noPaginationDur)
		}
	})
}

func TestFailedRecords(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	sts, err := memstats.New()
	require.NoError(t, err, "should create stats")

	service, err := NewJobService(JobServiceConfig{
		LocalHostname:       postgresContainer.Host,
		MaxPoolSize:         1,
		LocalConn:           postgresContainer.DBDsn,
		Log:                 logger.NOP,
		ShouldSetupSharedDB: true,
	}, sts)
	require.NoError(t, err)
	// Create 2 different job run ids with 10 records each
	_, err = postgresContainer.DB.Exec(`INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id) SELECT id::text, id::text, '1', '1', '1' FROM generate_series(1, 2) as id`)
	require.NoError(t, err)
	_, err = postgresContainer.DB.Exec(`INSERT INTO rsources_failed_keys_v2_records (id, record_id) SELECT k.id, to_json('"'||s.id||'"') from rsources_failed_keys_v2 k CROSS JOIN (SELECT id::text FROM generate_series(1, 10) as id) s`)
	require.NoError(t, err)

	t.Run("no result", func(t *testing.T) {
		r, err := service.GetFailedRecords(context.Background(), "3", JobFilter{}, PagingInfo{})
		require.NoError(t, err)
		require.Len(t, r.Tasks, 0)
	})
	t.Run("invalid pagination token", func(t *testing.T) {
		_, err := service.GetFailedRecords(context.Background(), "1", JobFilter{}, PagingInfo{Size: 1, NextPageToken: "invalid"})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidPaginationToken)
	})
}

func BenchmarkFailedRecordsPerformanceTest(b *testing.B) {
	b.Run("10Mil records no pagination", func(b *testing.B) {
		d := RunFailedRecordsPerformanceTest(b, 10_000_000, 0)
		b.ReportMetric(float64(d.Milliseconds()), "duration_millis")
	})
	b.Run("10Mil records with 100k pagination", func(b *testing.B) {
		d := RunFailedRecordsPerformanceTest(b, 10_000_000, 100_000)
		b.ReportMetric(float64(d.Milliseconds()), "duration_millis")
	})
}

func RunFailedRecordsPerformanceTest(t testing.TB, recordCount, pageSize int) time.Duration {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	sts, err := memstats.New()
	require.NoError(t, err, "should create stats")

	service, err := NewJobService(JobServiceConfig{
		LocalHostname:       postgresContainer.Host,
		MaxPoolSize:         1,
		LocalConn:           postgresContainer.DBDsn,
		Log:                 logger.NOP,
		ShouldSetupSharedDB: true,
	}, sts)
	require.NoError(t, err)

	// seed the database with records

	// Create 20k different job run ids with 10 records each
	_, err = postgresContainer.DB.Exec(`INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id) SELECT id::text, id::text, '1', '1', '1' FROM generate_series(1, 20000) as id`)
	require.NoError(t, err)
	_, err = postgresContainer.DB.Exec(`INSERT INTO rsources_failed_keys_v2_records (id, record_id) SELECT k.id, to_json('"'||s.id||'"') from rsources_failed_keys_v2 k CROSS JOIN (SELECT id::text FROM generate_series(1, 10) as id) s`)
	require.NoError(t, err)
	// job run id 1 should have [recordCount] records
	_, err = postgresContainer.DB.Exec(fmt.Sprintf(`INSERT INTO rsources_failed_keys_v2_records (id, record_id) SELECT k.id, to_json('"'||s.id||'"') from rsources_failed_keys_v2 k CROSS JOIN (SELECT id::text FROM generate_series(11, %d) as id) s WHERE k.job_run_id = '1'`, recordCount))
	require.NoError(t, err)

	start := time.Now()
	paging := PagingInfo{
		Size: pageSize,
	}
	var total int
	for total < recordCount {
		r, err := service.GetFailedRecords(context.Background(), "1", JobFilter{}, paging)
		require.NoError(t, err)
		require.Len(t, r.Tasks, 1)
		require.Len(t, r.Tasks[0].Sources, 1)
		require.Len(t, r.Tasks[0].Sources[0].Destinations, 1)
		total += len(r.Tasks[0].Sources[0].Destinations[0].Records)
	}
	return time.Since(start)
}

package rsources

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)

// TestFailedRecordsErrorResponse drives the durable half of the wire contract against
// a real postgres: the additive error_response column, the batched insert that feeds
// it and the v2 read path that serves it (test plan A4.1, A4.2).
func TestFailedRecordsErrorResponse(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	sts, err := memstats.New()
	require.NoError(t, err)

	service, err := NewJobService(context.Background(), JobServiceConfig{
		LocalHostname: postgresContainer.Host,
		MaxPoolSize:   1,
		LocalConn:     postgresContainer.DBDsn,
		Log:           logger.NOP,
		// a tiny batch size so the multi-batch insert path is exercised
		FailedRecordsInsertBatchSize: config.SingleValueLoader(2),
	}, sts)
	require.NoError(t, err)

	key := JobTargetKey{TaskRunID: "task-1", SourceID: "src-1", DestinationID: "dst-1"}

	addRecords := func(t *testing.T, jobRunID string, records []FailedRecord) {
		t.Helper()
		tx, err := postgresContainer.DB.Begin()
		require.NoError(t, err)
		require.NoError(t, service.AddFailedRecords(context.Background(), tx, jobRunID, key, records))
		require.NoError(t, tx.Commit())
	}

	t.Run("the column is additive: text, not null, defaulting to empty", func(t *testing.T) {
		var dataType, isNullable string
		var columnDefault sql.NullString
		require.NoError(t, postgresContainer.DB.QueryRow(`
			SELECT data_type, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_name = 'rsources_failed_keys_v2_records' AND column_name = 'error_response'`,
		).Scan(&dataType, &isNullable, &columnDefault))

		require.Equal(t, "text", dataType, "the cap is enforced in code, so the column must not be a varchar(n)")
		require.Equal(t, "NO", isNullable)
		require.True(t, columnDefault.Valid)
		require.Contains(t, columnDefault.String, "''")
	})

	t.Run("A4.1 the message round-trips to the v2 leaf alongside record and code", func(t *testing.T) {
		jobRunID := "run-roundtrip"
		addRecords(t, jobRunID, []FailedRecord{
			{Record: []byte(`"rec-1"`), Code: 422, ErrorResponse: `{"message":"invalid email"}`},
			{Record: []byte(`"rec-2"`), Code: 410, ErrorResponse: "source_not_found"},
			{Record: []byte(`"rec-3"`), Code: 500}, // captured with no message
		})

		got, err := service.GetFailedRecords(context.Background(), jobRunID, JobFilter{}, PagingInfo{})
		require.NoError(t, err)

		records := got.Tasks[0].Sources[0].Destinations[0].Records
		require.Equal(t, []FailedRecord{
			{Record: []byte(`"rec-1"`), Code: 422, ErrorResponse: `{"message":"invalid email"}`},
			{Record: []byte(`"rec-2"`), Code: 410, ErrorResponse: "source_not_found"},
			{Record: []byte(`"rec-3"`), Code: 500, ErrorResponse: ""},
		}, records)
	})

	t.Run("messages survive the record-id dedup inside a batch", func(t *testing.T) {
		jobRunID := "run-dedup"
		addRecords(t, jobRunID, []FailedRecord{
			{Record: []byte(`"dup"`), Code: 422, ErrorResponse: "first wins"},
			{Record: []byte(`"dup"`), Code: 500, ErrorResponse: "second is dropped"},
			{Record: []byte(`"other"`), Code: 422, ErrorResponse: "other message"},
		})

		got, err := service.GetFailedRecords(context.Background(), jobRunID, JobFilter{}, PagingInfo{})
		require.NoError(t, err)
		require.Equal(t, []FailedRecord{
			{Record: []byte(`"dup"`), Code: 422, ErrorResponse: "first wins"},
			{Record: []byte(`"other"`), Code: 422, ErrorResponse: "other message"},
		}, got.Tasks[0].Sources[0].Destinations[0].Records)
	})

	t.Run("A4.2 the message is present on every page and the cursor is unchanged", func(t *testing.T) {
		jobRunID := "run-paged"
		const total = 7
		records := make([]FailedRecord, 0, total)
		for i := range total {
			records = append(records, FailedRecord{
				Record:        []byte(`"rec-` + strconv.Itoa(i) + `"`),
				Code:          422,
				ErrorResponse: "message " + strconv.Itoa(i),
			})
		}
		addRecords(t, jobRunID, records)

		paging := PagingInfo{Size: 2}
		var seen int
		var pages int
		for {
			got, err := service.GetFailedRecords(context.Background(), jobRunID, JobFilter{}, paging)
			require.NoError(t, err)
			page := got.Tasks[0].Sources[0].Destinations[0].Records
			if len(page) == 0 {
				break
			}
			pages++
			for _, r := range page {
				require.NotEmpty(t, r.ErrorResponse, "record %s lost its message on page %d", r.Record, pages)
				require.Equal(t,
					"message "+strings.TrimSuffix(strings.TrimPrefix(string(r.Record), `"rec-`), `"`),
					r.ErrorResponse)
				seen++
			}
			if got.Paging == nil {
				break
			}
			paging = *got.Paging
		}
		require.Equal(t, total, seen)
		require.Greater(t, pages, 1, "the test must actually paginate")
	})

	t.Run("a message at the cap lands verbatim", func(t *testing.T) {
		jobRunID := "run-capped"
		capped := strings.Repeat("x", defaultMaxErrorLength)
		addRecords(t, jobRunID, []FailedRecord{{Record: []byte(`"rec-big"`), Code: 422, ErrorResponse: capped}})

		got, err := service.GetFailedRecords(context.Background(), jobRunID, JobFilter{}, PagingInfo{})
		require.NoError(t, err)
		require.Equal(t, capped, got.Tasks[0].Sources[0].Destinations[0].Records[0].ErrorResponse)
	})

	t.Run("sql metacharacters land verbatim through the bind parameters", func(t *testing.T) {
		jobRunID := "run-injection"
		nasty := `'; DROP TABLE rsources_failed_keys_v2_records; -- \`
		addRecords(t, jobRunID, []FailedRecord{{Record: []byte(`"rec-nasty"`), Code: 422, ErrorResponse: nasty}})

		got, err := service.GetFailedRecords(context.Background(), jobRunID, JobFilter{}, PagingInfo{})
		require.NoError(t, err)
		require.Equal(t, nasty, got.Tasks[0].Sources[0].Destinations[0].Records[0].ErrorResponse)

		var count int
		require.NoError(t, postgresContainer.DB.QueryRow(
			`SELECT count(*) FROM rsources_failed_keys_v2_records`).Scan(&count))
		require.Positive(t, count, "the table must still exist and hold rows")
	})

	t.Run("rows written before the migration read back as an empty message", func(t *testing.T) {
		jobRunID := "run-legacy"
		_, err := postgresContainer.DB.Exec(
			`INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id)
			 VALUES ('legacy-id', $1, 'task-1', 'src-1', 'dst-1')`, jobRunID)
		require.NoError(t, err)
		// No error_response supplied: exactly what a pre-migration writer produces.
		_, err = postgresContainer.DB.Exec(
			`INSERT INTO rsources_failed_keys_v2_records (id, record_id, code) VALUES ('legacy-id', '"rec-old"', 422)`)
		require.NoError(t, err)

		got, err := service.GetFailedRecords(context.Background(), jobRunID, JobFilter{}, PagingInfo{})
		require.NoError(t, err)
		require.Equal(t, []FailedRecord{{Record: []byte(`"rec-old"`), Code: 422, ErrorResponse: ""}},
			got.Tasks[0].Sources[0].Destinations[0].Records)
	})
}

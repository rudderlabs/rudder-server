package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	rsRand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/utils/tx"
)

// TestUnionJobsDBFunctions verifies that the unionjobsdb and unionjobsdbmetadata SQL functions
// include the first num datasets in dataset index order, with a compaction dataset (e.g. 3_1)
// right after the dataset it follows, and all datasets when num is NULL.
func TestUnionJobsDBFunctions(t *testing.T) {
	postgres := startPostgres(t)
	ctx := context.Background()

	statStore, err := memstats.New()
	require.NoError(t, err)
	jd := &Handle{
		TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
		TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
		stats:             statStore,
	}
	prefix := strings.ToLower(rsRand.String(5))
	require.NoError(t, jd.Setup(ReadWrite, false, prefix))
	t.Cleanup(jd.TearDown)

	// dataset 1 is created by Setup
	indices := []string{"1", "2", "3", "3_1", "4", "5", "6", "7", "8", "9", "10", "11", "12"}
	require.NoError(t, jd.WithTx(ctx, func(txn *tx.Tx) error {
		for _, idx := range indices[1:] {
			if err := jd.createDSInTx(ctx, txn, newDataSet(prefix, idx)); err != nil {
				return err
			}
		}
		return nil
	}))
	jobTables := lo.Map(indices, func(idx string, _ int) string { return newDataSet(prefix, idx).JobTable })
	for _, jobTable := range jobTables {
		_, err := postgres.DB.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %q (uuid, user_id, parameters, custom_val, event_payload) VALUES ($1, 'user', '{}', 'MOCKDS', '{}')`, jobTable),
			uuid.New(),
		)
		require.NoError(t, err)
	}

	unionTables := func(t *testing.T, fn string, num any) []string {
		t.Helper()
		rows, err := postgres.DB.QueryContext(ctx, fmt.Sprintf(`SELECT t_name FROM %s($1, $2)`, fn), prefix, num)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		var tables []string
		for rows.Next() {
			var table string
			require.NoError(t, rows.Scan(&table))
			tables = append(tables, table)
		}
		require.NoError(t, rows.Err())
		return tables
	}

	for _, fn := range []string{"unionjobsdb", "unionjobsdbmetadata"} {
		t.Run(fn, func(t *testing.T) {
			t.Run("limit", func(t *testing.T) {
				require.ElementsMatch(t, jobTables[:4], unionTables(t, fn, 4))
			})
			t.Run("null limit", func(t *testing.T) {
				require.ElementsMatch(t, jobTables, unionTables(t, fn, sql.NullInt64{}))
			})
		})
	}
}

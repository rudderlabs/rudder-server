package warehouse

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

// RetrieveRecordsFromWarehouse retrieves records from the warehouse based on the given query.
// It returns a slice of slices, where each inner slice represents a record's values.
func RetrieveRecordsFromWarehouse(
	t testing.TB,
	db *sql.DB,
	query string,
) [][]string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	_ = rows.Err()

	columns, err := rows.Columns()
	require.NoError(t, err)

	var records [][]string
	for rows.Next() {
		resultSet := make([]any, len(columns))
		resultSetPtrs := make([]any, len(columns))
		for i := 0; i < len(columns); i++ {
			resultSetPtrs[i] = &resultSet[i]
		}

		err = rows.Scan(resultSetPtrs...)
		require.NoError(t, err)

		records = append(records, lo.Map(resultSet, func(item any, index int) string {
			switch item := item.(type) {
			case time.Time:
				return item.Format(time.RFC3339)
			case string:
				if t, err := time.Parse(time.RFC3339Nano, item); err == nil {
					return t.Format(time.RFC3339)
				}
				return item
			default:
				return cast.ToString(item)
			}
		}))
	}
	return records
}

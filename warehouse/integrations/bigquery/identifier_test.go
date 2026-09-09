package bigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, "`column\\`name`", quoteIdentifier("column`name"))
	require.Equal(t, "`project-id.data\\`set.table$20240101`", quoteTablePath("project-id", "data`set", "table$20240101"))
	require.Equal(t, "`row_id`, `column\\`name`, `table_name`", quoteColumnList("row_id, column`name, table_name"))
}

func TestDeduplicationQueryQuotesIdentifiers(t *testing.T) {
	bq := &BigQuery{projectID: "project-id", namespace: "data`set", conf: config.New()}
	query, err := bq.deduplicationQuery("table`x", model.TableSchema{
		"id":        "string",
		"loaded_at": "datetime",
	})
	require.NoError(t, err)
	require.Contains(t, query, "FROM `project-id.data\\`set.table\\`x`")
	require.Contains(t, query, "PARTITION BY `id` ORDER BY `loaded_at` DESC")
	require.NotContains(t, query, "data`set.table`x")

	partitionQuery, err := bq.deduplicationQuery("partitioned", model.TableSchema{"id": "string"})
	require.NoError(t, err)
	require.True(t, strings.Contains(partitionQuery, "_PARTITIONTIME BETWEEN"), partitionQuery)
}

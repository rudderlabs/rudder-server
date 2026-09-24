package snowflake

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// TestColumnsWithDataTypesQuotesIdentifiers ensures a malicious column name
// cannot break out of the double-quoted identifier in the generated CREATE
// TABLE column fragment. The double quote must be doubled.
func TestColumnsWithDataTypesQuotesIdentifiers(t *testing.T) {
	columnName := `x" NUMBER); DROP TABLE users; --`

	fragment := columnsWithDataTypes(model.TableSchema{
		columnName: model.StringDataType,
	}, map[string]string{model.StringDataType: "varchar"})

	require.Contains(t, fragment, `"x"" NUMBER); DROP TABLE users; --"`)
	require.NotContains(t, fragment, `x" NUMBER); DROP`)
}

// TestDeleteByStatementUsesProviderCase pins the column case in the source job
// cleanup statement. Snowflake stores an unquoted identifier upper cased, so
// quoting the lower case spelling the code starts from would produce an
// "invalid identifier" error against a real warehouse. This has regressed once,
// and the live tests that would catch it only run with credentials.
func TestDeleteByStatementUsesProviderCase(t *testing.T) {
	statement := deleteByStatement("my_namespace", "tracks")

	require.Contains(t, statement, `DELETE FROM "my_namespace"."tracks"`)
	require.Contains(t, statement, `"CONTEXT_SOURCES_JOB_RUN_ID" <> ?`)
	require.Contains(t, statement, `"CONTEXT_SOURCES_TASK_RUN_ID" <> ?`)
	require.Contains(t, statement, `"CONTEXT_SOURCE_ID" = ?`)
	require.Contains(t, statement, `"RECEIVED_AT" < ?`)
	// The lower case spelling must not reach the statement quoted.
	require.NotContains(t, statement, `"context_sources_job_run_id"`)
	require.NotContains(t, statement, `"received_at"`)
}

// TestMergeWindowJoinClauseUsesProviderCase does the same for the merge window
// column, which comes from configuration and may be given in any case.
func TestMergeWindowJoinClauseUsesProviderCase(t *testing.T) {
	require.Equal(t,
		` AND original."RECEIVED_AT" >= DATEADD(hour, -720, CURRENT_TIMESTAMP())`,
		mergeWindowJoinClause("received_at", 30*24*time.Hour),
	)
	// An identifier carrying the delimiter still cannot break out.
	require.Equal(t,
		` AND original."EVIL"" OR 1=1 --" >= DATEADD(hour, -1, CURRENT_TIMESTAMP())`,
		mergeWindowJoinClause(`evil" OR 1=1 --`, time.Hour),
	)
}

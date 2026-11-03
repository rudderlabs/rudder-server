package deltalake

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// TestColumnsWithDataTypesQuotesBacktickIdentifiers ensures a malicious column
// name cannot break out of the backtick-delimited identifier in the generated
// CREATE TABLE column fragment. The backtick must be doubled.
func TestColumnsWithDataTypesQuotesBacktickIdentifiers(t *testing.T) {
	columnName := "x` STRING); DROP TABLE users; --"

	fragment := columnsWithDataTypes(model.TableSchema{
		columnName: model.StringDataType,
	})

	require.Contains(t, fragment, "`x`` STRING); DROP TABLE users; --`")
	require.NotContains(t, fragment, "x` STRING); DROP")

	// The generated event_date column quotes the column it casts from.
	withReceivedAt := columnsWithDataTypes(model.TableSchema{"received_at": "datetime"})
	require.Contains(t, withReceivedAt, "`event_date` DATE GENERATED ALWAYS AS ( CAST(`received_at` AS DATE) )")
}

// TestTableLocationQuery pins the LOCATION clause. The path itself must keep the
// format it had before the identifiers were quoted, and a namespace or table name
// carrying a quote or a trailing backslash has to be escaped the Spark way, with a
// backslash, so it cannot terminate the string literal.
func TestTableLocationQuery(t *testing.T) {
	locationQuery := func(t *testing.T, namespace, externalLocation string, enabled bool) string {
		t.Helper()

		d := New(config.New(), logger.NOP, stats.NOP)
		d.Namespace = namespace
		d.Warehouse = model.Warehouse{
			Destination: backendconfig.DestinationT{
				Config: map[string]any{
					"enableExternalLocation": enabled,
					"externalLocation":       externalLocation,
				},
			},
		}
		return d.tableLocationQuery("table")
	}

	t.Run("disabled", func(t *testing.T) {
		require.Empty(t, locationQuery(t, "namespace", "s3://bucket/prefix", false))
	})
	t.Run("no external location", func(t *testing.T) {
		require.Empty(t, locationQuery(t, "namespace", "", true))
	})
	t.Run("path is unchanged", func(t *testing.T) {
		require.Equal(t, "LOCATION 's3://bucket/prefix/namespace/table'", locationQuery(t, "namespace", "s3://bucket/prefix", true))
	})
	t.Run("quote is escaped", func(t *testing.T) {
		// A bare quote would close the literal and leave the rest of the path as SQL.
		require.Equal(t, `LOCATION 's3://bucket/evil\'; DROP TABLE users; --/table'`, locationQuery(t, `evil'; DROP TABLE users; --`, "s3://bucket", true))
	})
	t.Run("trailing backslash is escaped", func(t *testing.T) {
		// Unescaped, the backslash would escape the closing quote.
		require.Equal(t, `LOCATION 's3://bucket/evil\\\\/table'`, locationQuery(t, `evil\\`, "s3://bucket", true))
	})
}

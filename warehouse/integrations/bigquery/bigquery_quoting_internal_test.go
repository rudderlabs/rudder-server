package bigquery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// BigQuery's DDL (CreateTable/AddColumns/DropTable/CreateSchema) goes through the
// typed BigQuery Go client API, where table and column names are passed as struct
// fields rather than interpolated into SQL - so those paths cannot be used for SQL
// injection, and BigQuery additionally rejects backticks/illegal characters in
// identifiers at the API level. The remaining injection surface is the raw-SQL
// paths that interpolate a potentially attacker-influenced identifier via backtick
// quoting - most notably the deduplication view built by deduplicationQuery.
//
// This test proves a malicious table name cannot break out of the backtick-
// delimited identifier: the backtick (and backslash) must be backslash-escaped, so
// the identifier cannot be terminated early to inject trailing SQL.
func TestDeduplicationQueryQuotesBacktickIdentifiers(t *testing.T) {
	bq := &BigQuery{}
	bq.conf = config.New()
	bq.warehouse = model.Warehouse{
		Destination: backendconfig.DestinationT{Config: map[string]any{}},
	}
	bq.projectID = "test_project"
	bq.namespace = "test_namespace"

	maliciousTable := "evil_table`); DROP TABLE victim_secrets; --"

	query, err := bq.deduplicationQuery(maliciousTable, model.TableSchema{"id": "string"})
	require.NoError(t, err)

	// BigQuery escapes the backtick delimiter (and backslash) with a preceding
	// backslash, not by doubling. The table name must appear backtick-quoted with
	// the embedded backtick backslash-escaped.
	require.Contains(t, query, warehouseutils.BigQueryQuoteTablePath(bq.projectID, bq.namespace, maliciousTable))
	require.Contains(t, query, "`test_project.test_namespace.evil_table\\`); DROP TABLE victim_secrets; --`")

	// The raw, unescaped breakout must never appear in the generated SQL.
	require.NotContains(t, query, "`evil_table`); DROP")
}

// TestQualifiedTableNamesQuoteBacktickIdentifiers guards the helper that BigQuery
// uses to build every table path in its raw-SQL paths (deduplication view, users
// merge, delete by). BigQuery uses backslash escaping, so a backtick in the name
// becomes \` rather than being doubled.
func TestQualifiedTableNamesQuoteBacktickIdentifiers(t *testing.T) {
	tableName := "evil_table`); DROP TABLE victim_secrets; --"

	qualifiedName := warehouseutils.BigQueryQuoteTablePath("project", "namespace", tableName)

	require.Equal(t, "`project.namespace.evil_table\\`); DROP TABLE victim_secrets; --`", qualifiedName)
	require.NotContains(t, qualifiedName, "evil_table`); DROP")
}

func TestUsersMergeQueryQuotesIdentifiers(t *testing.T) {
	bq := &BigQuery{namespace: "data`set"}
	query := bq.usersMergeQuery(model.TableSchema{
		"id":                     "string",
		"email":                  "string",
		"evil`) AS x FROM y; --": "string",
		`trailing\`:              "string",
	}, "SELECT * FROM dedup", "rudder_staging_users")

	require.Equal(t, `SELECT DISTINCT * FROM (
			SELECT `+"`id`"+`, FIRST_VALUE(`+"`email`"+` IGNORE NULLS) OVER (PARTITION BY `+"`id`"+` ORDER BY `+"`received_at`"+` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `+"`email`"+`,FIRST_VALUE(`+"`evil\\`) AS x FROM y; --`"+` IGNORE NULLS) OVER (PARTITION BY `+"`id`"+` ORDER BY `+"`received_at`"+` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `+"`evil\\`) AS x FROM y; --`"+`,FIRST_VALUE(`+"`trailing\\\\`"+` IGNORE NULLS) OVER (PARTITION BY `+"`id`"+` ORDER BY `+"`received_at`"+` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `+"`trailing\\\\`"+` FROM (
				(
					SELECT `+"`id`"+`, `+"`email`,`evil\\`) AS x FROM y; --`,`trailing\\\\`"+` FROM (SELECT * FROM dedup) WHERE (
						`+"`id`"+` in (SELECT `+"`id`"+` FROM `+"`data\\`set.rudder_staging_users`"+`)
					)
				) UNION ALL (
					SELECT `+"`id`"+`, `+"`email`,`evil\\`) AS x FROM y; --`,`trailing\\\\`"+` FROM `+"`data\\`set.rudder_staging_users`"+`
				)
			)
		)`, query)
}

// TestDeduplicationQueryPartitionAndOrder covers the two branches of the view that
// are not exercised by the malicious-name test: the composite partition key, whose
// parts have to be quoted one by one rather than as a single identifier, and the
// loaded_at ORDER BY. The default partition filter is the _PARTITIONTIME pseudo
// column, which must stay unquoted.
func TestDeduplicationQueryPartitionAndOrder(t *testing.T) {
	bq := &BigQuery{}
	bq.conf = config.New()
	bq.warehouse = model.Warehouse{
		Destination: backendconfig.DestinationT{Config: map[string]any{}},
	}
	bq.projectID = "test_project"
	bq.namespace = "test_namespace"

	t.Run("composite partition key and loaded_at order", func(t *testing.T) {
		query, err := bq.deduplicationQuery(warehouseutils.DiscardsTable, model.TableSchema{
			"row_id":      "string",
			"column_name": "string",
			"table_name":  "string",
			"loaded_at":   "datetime",
		})
		require.NoError(t, err)

		require.Contains(t, query, "PARTITION BY `row_id`, `column_name`, `table_name`")
		require.Contains(t, query, "ORDER BY `loaded_at` DESC")
		// The pseudo column is not an identifier and must not be quoted.
		require.Contains(t, query, "_PARTITIONTIME BETWEEN")
		require.NotContains(t, query, "`_PARTITIONTIME`")
	})

	t.Run("single partition key without loaded_at", func(t *testing.T) {
		query, err := bq.deduplicationQuery(warehouseutils.UsersTable, model.TableSchema{"id": "string"})
		require.NoError(t, err)

		require.Contains(t, query, "PARTITION BY `id`")
		require.NotContains(t, query, "ORDER BY")
	})
}

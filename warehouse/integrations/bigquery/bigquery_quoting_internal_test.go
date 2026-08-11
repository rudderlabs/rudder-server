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
	require.Contains(t, query,
		warehouseutils.BigQueryBacktickQuoteQualifiedIdentifier(bq.projectID, bq.namespace, maliciousTable))
	require.Contains(t, query, "`evil_table\\`); DROP TABLE victim_secrets; --`")

	// The raw, unescaped breakout must never appear in the generated SQL.
	require.NotContains(t, query, "`evil_table`); DROP")
}

// TestQualifiedTableNamesQuoteBacktickIdentifiers guards the helper that BigQuery
// uses to build every qualified identifier in its raw-SQL paths (deduplication
// view, users merge, INFORMATION_SCHEMA lookups). BigQuery uses backslash
// escaping, so a backtick in the name becomes \` rather than being doubled.
func TestQualifiedTableNamesQuoteBacktickIdentifiers(t *testing.T) {
	tableName := "evil_table`); DROP TABLE victim_secrets; --"

	qualifiedName := warehouseutils.BigQueryBacktickQuoteQualifiedIdentifier("project", "namespace", tableName)

	require.Equal(t, "`project`.`namespace`.`evil_table\\`); DROP TABLE victim_secrets; --`", qualifiedName)
	require.NotContains(t, qualifiedName, "evil_table`); DROP")
}

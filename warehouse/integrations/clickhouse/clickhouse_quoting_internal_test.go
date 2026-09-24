package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TestColumnsWithDataTypesNeutralizesSQLInjection proves the reason for the identifier
// quoting on ClickHouse: a column name crafted to break out of the double quoted
// identifier in the generated CREATE TABLE column list must be neutralized. ClickHouse
// applies string literal escape rules inside quoted identifiers, so the delimiter and any
// backslash are escaped with a backslash rather than doubled. The backslash case guards
// against a doubling escaper, where a trailing backslash would escape the closing quote
// and let the rest of the column list run on.
func TestColumnsWithDataTypesNeutralizesSQLInjection(t *testing.T) {
	ch := &Clickhouse{}

	payloads := map[string]string{
		"drop_table":         `x" String);drop table rudder_secrets;--`,
		"backslash_breakout": `x\" String);drop table rudder_secrets;--`,
	}

	for name, columnName := range payloads {
		t.Run(name, func(t *testing.T) {
			fragment := ch.ColumnsWithDataTypes(warehouseutils.DiscardsTable, model.TableSchema{
				columnName: model.StringDataType,
			}, nil)

			require.Contains(t, fragment, warehouseutils.ClickHouseQuoteIdentifier(columnName))
			require.NotContains(t, fragment, columnName)
		})
	}
}

// TestS3CopyStatementQuotesIdentifiersAndLiterals covers the copy path: the column list is
// quoted, and the load folder, credentials and the structure argument are escaped as string
// literals so a quote in any of them cannot terminate the literal early.
func TestS3CopyStatementQuotesIdentifiersAndLiterals(t *testing.T) {
	args := s3TableFunctionArgs(
		`s3://bucket/it's/*.csv.gz`,
		"access-key",
		`secret'key`,
		"",
		`"evil\"col" String`,
	)
	statement := copySQLStatement(
		`ns"x`,
		`table"y`,
		warehouseutils.JoinQuotedIdentifiers([]string{`evil"col`, "id"}, warehouseutils.ClickHouseQuoteIdentifier, ","),
		args,
		nil,
	)

	require.Contains(t, statement, `INSERT INTO "ns\"x"."table\"y"`)
	require.Contains(t, statement, `"evil\"col","id"`)
	// The literal escaper doubles the quote, which ClickHouse accepts alongside \'.
	require.Contains(t, statement, `'s3://bucket/it''s/*.csv.gz'`)
	require.Contains(t, statement, `'secret''key'`)
	require.NotContains(t, statement, `'s3://bucket/it's/`)
	// The structure argument is escaped twice: the column name is quoted as an
	// identifier, then that whole fragment is escaped as a string literal, so the
	// backslash the identifier escaper added is itself doubled. ClickHouse unwraps
	// both layers back to the column named evil"col.
	require.Contains(t, statement, `'"evil\\"col" String'`)
}

// TestColumnsWithDataTypesAssertsExpectedText pins the generated fragment rather than
// comparing it against the helper it was built with, so a change to the escaping rule
// has to be stated here instead of tracking the production code silently.
func TestColumnsWithDataTypesAssertsExpectedText(t *testing.T) {
	ch := &Clickhouse{}

	fragment := ch.ColumnsWithDataTypes(warehouseutils.DiscardsTable, model.TableSchema{
		`x" String);drop table rudder_secrets;--`: model.StringDataType,
	}, nil)

	require.Contains(t, fragment, `"x\" String);drop table rudder_secrets;--"`)
}

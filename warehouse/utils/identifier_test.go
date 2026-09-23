package warehouseutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// sink contains every delimiter and escape character the warehouse dialects care
// about: a " b ` c ] d ' e \ f
const sink = "a\"b`c]d'e\\f"

func TestIdentifierQuoting(t *testing.T) {
	t.Run("double quote", func(t *testing.T) {
		require.Equal(t, `"x""; DROP TABLE users; --"`, DoubleQuoteIdentifier(`x"; DROP TABLE users; --`))
		// The backslash is an ordinary character inside a double-quoted identifier.
		require.Equal(t, `"evil\"`, DoubleQuoteIdentifier(`evil\`))
		require.Equal(t, "\"a\"\"b`c]d'e\\f\"", DoubleQuoteIdentifier(sink))
	})
	t.Run("bracket", func(t *testing.T) {
		require.Equal(t, `[x]]; DROP TABLE users; --]`, BracketQuoteIdentifier(`x]; DROP TABLE users; --`))
		require.Equal(t, "[a\"b`c]]d'e\\f]", BracketQuoteIdentifier(sink))
	})
	t.Run("backtick", func(t *testing.T) {
		require.Equal(t, "`x``; DROP TABLE users; --`", BacktickQuoteIdentifier("x`; DROP TABLE users; --"))
		require.Equal(t, "`a\"b``c]d'e\\f`", BacktickQuoteIdentifier(sink))
	})
	t.Run("clickhouse", func(t *testing.T) {
		// ClickHouse applies string literal escape rules inside quoted identifiers, so the
		// delimiter and the backslash are backslash escaped rather than doubled.
		require.Equal(t, `"x\"; DROP TABLE users; --"`, ClickHouseQuoteIdentifier(`x"; DROP TABLE users; --`))
		require.Equal(t, `"evil\\"`, ClickHouseQuoteIdentifier(`evil\`))
		require.Equal(t, "\"a\\\"b`c]d'e\\\\f\"", ClickHouseQuoteIdentifier(sink))
	})
	t.Run("bigquery backtick", func(t *testing.T) {
		require.Equal(t, "`x\\`; DROP TABLE users; --`", BigQueryQuoteIdentifier("x`; DROP TABLE users; --"))
		// The backslash must be escaped, otherwise a name ending in \ escapes the closing backtick.
		require.Equal(t, "`evil\\\\`", BigQueryQuoteIdentifier(`evil\`))
		require.Equal(t, "`a\"b\\`c]d'e\\\\f`", BigQueryQuoteIdentifier(sink))
	})
}

func TestQualifiedIdentifierQuoting(t *testing.T) {
	require.Equal(t, `"schema""x"."table""y"`, QuoteQualifiedIdentifier(DoubleQuoteIdentifier, `schema"x`, `table"y`))
	require.Equal(t, `[schema]]x].[table]]y]`, QuoteQualifiedIdentifier(BracketQuoteIdentifier, `schema]x`, `table]y`))
	require.Equal(t, "`schema``x`.`table``y`", QuoteQualifiedIdentifier(BacktickQuoteIdentifier, "schema`x", "table`y"))
	require.Equal(t, "`project.data\\`set.table$20240101`", BigQueryQuoteTablePath("project", "data`set", "table$20240101"))
}

func TestDialectQualifiedAndJoinHelpers(t *testing.T) {
	require.Equal(t, `"schema""x"."table""y"`, DoubleQuoteQualifiedIdentifier(`schema"x`, `table"y`))
	require.Equal(t, `[schema]]x].[table]]y]`, BracketQuoteQualifiedIdentifier(`schema]x`, `table]y`))
	require.Equal(t, "`schema``x`.`table``y`", BacktickQuoteQualifiedIdentifier("schema`x", "table`y"))
	require.Equal(t, `"schema\"x"."table\"y"`, ClickHouseQuoteQualifiedIdentifier(`schema"x`, `table"y`))
	require.Equal(t, `"id","evil""x"`, DoubleQuoteAndJoinByComma([]string{"id", `evil"x`}))
	require.Equal(t, `[id],[evil]]x]`, BracketQuoteAndJoinByComma([]string{"id", `evil]x`}))
	require.Equal(t, "`id`,`evil``x`", BacktickQuoteAndJoinByComma([]string{"id", "evil`x"}))
	require.Equal(t, `"id","evil\"x"`, ClickHouseQuoteAndJoinByComma([]string{"id", `evil"x`}))
}

func TestJoinQuotedIdentifiers(t *testing.T) {
	require.Equal(t, `"id","received_at"`, JoinQuotedIdentifiers([]string{"id", "received_at"}, DoubleQuoteIdentifier, ","))
	require.Equal(t, `[id], [evil]]x]`, JoinQuotedIdentifiers([]string{"id", `evil]x`}, BracketQuoteIdentifier, ", "))
	require.Empty(t, JoinQuotedIdentifiers(nil, DoubleQuoteIdentifier, ","))
}

func TestQuoteCommaSeparatedIdentifiers(t *testing.T) {
	require.Equal(t, `"row_id", "column_name", "table_name"`, QuoteCommaSeparatedIdentifiers("row_id, column_name, table_name", DoubleQuoteIdentifier))
	require.Equal(t, `"id"`, QuoteCommaSeparatedIdentifiers("id", DoubleQuoteIdentifier))
	require.Equal(t, `"x""; DROP TABLE users; --"`, QuoteCommaSeparatedIdentifiers(`x"; DROP TABLE users; --`, DoubleQuoteIdentifier))
}

func TestStringLiterals(t *testing.T) {
	t.Run("doubling only", func(t *testing.T) {
		require.Equal(t, `'it''s'`, SQLStringLiteral(`it's`))
		require.Equal(t, `N'it''s'`, UnicodeStringLiteral(`it's`))
		// The backslash is an ordinary character for MSSQL and Azure Synapse.
		require.Equal(t, `'evil\'`, SQLStringLiteral(`evil\`))
		require.Equal(t, "'a\"b`c]d''e\\f'", SQLStringLiteral(sink))
	})
	t.Run("backslash and doubled quote", func(t *testing.T) {
		require.Equal(t, `'it''s'`, SQLStringLiteralBackslash(`it's`))
		require.Equal(t, `'evil\\'`, SQLStringLiteralBackslash(`evil\`))
		// \' would otherwise read as an escaped quote and let the literal run on.
		require.Equal(t, `'evil\\''; DROP TABLE x; --'`, SQLStringLiteralBackslash(`evil\'; DROP TABLE x; --`))
		require.Equal(t, "'a\"b`c]d''e\\\\f'", SQLStringLiteralBackslash(sink))
	})
	t.Run("spark", func(t *testing.T) {
		require.Equal(t, `'it\'s'`, SparkSQLStringLiteral(`it's`))
		require.Equal(t, `'evil\\'`, SparkSQLStringLiteral(`evil\`))
		require.Equal(t, `'evil\\\'; DROP TABLE x; --'`, SparkSQLStringLiteral(`evil\'; DROP TABLE x; --`))
		require.Equal(t, "'a\"b`c]d\\'e\\\\f'", SparkSQLStringLiteral(sink))
	})
}

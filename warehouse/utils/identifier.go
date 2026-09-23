package warehouseutils

import "strings"

// Identifier quoting helpers, one per SQL dialect used by the warehouse integrations.
//
// Quoting an identifier keeps a table, schema or column name that originates from
// event payloads inside the identifier, so it cannot terminate it early and inject
// SQL. Each dialect has its own delimiter and its own way of escaping it:
//
//	Postgres, Redshift, Snowflake  "name"  with " doubled
//	MSSQL, Azure Synapse           [name]  with ] doubled
//	Databricks (Spark SQL)         `name`  with ` doubled
//	BigQuery (GoogleSQL)           `name`  with ` and \ backslash escaped

// bigQueryIdentifierEscaper escapes a GoogleSQL quoted identifier. Quoted identifiers
// share string literal escape sequences, so the backslash is escaped as well as the
// backtick: escaping only the backtick would let a name ending in \ break out.
var bigQueryIdentifierEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

// DoubleQuoteIdentifier quotes an identifier for Postgres, Redshift and Snowflake.
func DoubleQuoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// BracketQuoteIdentifier quotes an identifier for MSSQL and Azure Synapse.
func BracketQuoteIdentifier(identifier string) string {
	return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
}

// BacktickQuoteIdentifier quotes an identifier for Databricks (Spark SQL).
func BacktickQuoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

// BigQueryQuoteIdentifier quotes an identifier for BigQuery (GoogleSQL).
func BigQueryQuoteIdentifier(identifier string) string {
	return "`" + bigQueryIdentifierEscaper.Replace(identifier) + "`"
}

// QuoteQualifiedIdentifier quotes each part with quote and joins them with a dot,
// e.g. "schema"."table".
func QuoteQualifiedIdentifier(quote func(string) string, identifiers ...string) string {
	return JoinQuotedIdentifiers(identifiers, quote, ".")
}

// clickHouseIdentifierEscaper escapes a ClickHouse quoted identifier. ClickHouse applies
// string literal escape rules inside double-quoted identifiers, so the delimiter and the
// backslash are escaped with a backslash instead of being doubled.
var clickHouseIdentifierEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

// ClickHouseQuoteIdentifier quotes an identifier for ClickHouse.
func ClickHouseQuoteIdentifier(identifier string) string {
	return `"` + clickHouseIdentifierEscaper.Replace(identifier) + `"`
}

// DoubleQuoteQualifiedIdentifier quotes each part for Postgres, Redshift and Snowflake
// and joins them with a dot, e.g. "schema"."table".
func DoubleQuoteQualifiedIdentifier(identifiers ...string) string {
	return QuoteQualifiedIdentifier(DoubleQuoteIdentifier, identifiers...)
}

// BracketQuoteQualifiedIdentifier quotes each part for MSSQL and Azure Synapse and
// joins them with a dot, e.g. [schema].[table].
func BracketQuoteQualifiedIdentifier(identifiers ...string) string {
	return QuoteQualifiedIdentifier(BracketQuoteIdentifier, identifiers...)
}

// BacktickQuoteQualifiedIdentifier quotes each part for Databricks and joins them with
// a dot, e.g. `schema`.`table`.
func BacktickQuoteQualifiedIdentifier(identifiers ...string) string {
	return QuoteQualifiedIdentifier(BacktickQuoteIdentifier, identifiers...)
}

// ClickHouseQuoteQualifiedIdentifier quotes each part for ClickHouse and joins them with
// a dot, e.g. "database"."table".
func ClickHouseQuoteQualifiedIdentifier(identifiers ...string) string {
	return QuoteQualifiedIdentifier(ClickHouseQuoteIdentifier, identifiers...)
}

// ClickHouseQuoteAndJoinByComma quotes each identifier for ClickHouse and joins them with
// commas.
func ClickHouseQuoteAndJoinByComma(identifiers []string) string {
	return JoinQuotedIdentifiers(identifiers, ClickHouseQuoteIdentifier, ",")
}

// BracketQuoteAndJoinByComma quotes each identifier for MSSQL and Azure Synapse and
// joins them with commas.
func BracketQuoteAndJoinByComma(identifiers []string) string {
	return JoinQuotedIdentifiers(identifiers, BracketQuoteIdentifier, ",")
}

// BacktickQuoteAndJoinByComma quotes each identifier for Databricks and joins them
// with commas.
func BacktickQuoteAndJoinByComma(identifiers []string) string {
	return JoinQuotedIdentifiers(identifiers, BacktickQuoteIdentifier, ",")
}

// BigQueryQuoteTablePath quotes a dotted BigQuery path such as project.dataset.table
// as a single quoted identifier.
func BigQueryQuoteTablePath(identifiers ...string) string {
	return BigQueryQuoteIdentifier(strings.Join(identifiers, "."))
}

// JoinQuotedIdentifiers quotes each identifier with quote and joins them with sep.
func JoinQuotedIdentifiers(identifiers []string, quote func(string) string, sep string) string {
	quoted := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quoted = append(quoted, quote(identifier))
	}
	return strings.Join(quoted, sep)
}

// QuoteCommaSeparatedIdentifiers quotes a possibly comma-separated list of column
// names (such as a composite partition key) by quoting each column individually, so
// the result stays valid in a PARTITION BY or column list.
func QuoteCommaSeparatedIdentifiers(columns string, quote func(string) string) string {
	parts := strings.Split(columns, ",")
	for i, column := range parts {
		parts[i] = quote(strings.TrimSpace(column))
	}
	return strings.Join(parts, ", ")
}

// String literal helpers. Doubling the single quote is enough for engines that treat
// the backslash as an ordinary character, but Redshift, Snowflake, BigQuery and
// Databricks all interpret backslash escape sequences inside string literals, so a
// value ending in \ (or containing \') could otherwise escape the closing quote.

// sqlStringLiteralBackslashEscaper escapes a literal for engines that honour backslash
// escapes and accept a doubled single quote (Redshift, Snowflake, ClickHouse).
var sqlStringLiteralBackslashEscaper = strings.NewReplacer(`\`, `\\`, `'`, `''`)

// sparkStringLiteralEscaper escapes a literal for Spark SQL, which uses backslash
// escapes and does not treat a doubled single quote as an escaped quote.
var sparkStringLiteralEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// SQLStringLiteral quotes a value as a string literal for MSSQL and Azure Synapse.
func SQLStringLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

// UnicodeStringLiteral quotes a value as an N'...' literal for MSSQL and Azure Synapse.
func UnicodeStringLiteral(value string) string {
	return "N" + SQLStringLiteral(value)
}

// SQLStringLiteralBackslash quotes a value as a string literal for Redshift, Snowflake and
// ClickHouse.
func SQLStringLiteralBackslash(value string) string {
	return `'` + sqlStringLiteralBackslashEscaper.Replace(value) + `'`
}

// SparkSQLStringLiteral quotes a value as a string literal for Databricks (Spark SQL).
func SparkSQLStringLiteral(value string) string {
	return `'` + sparkStringLiteralEscaper.Replace(value) + `'`
}

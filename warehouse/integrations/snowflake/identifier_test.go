package snowflake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, `"schema"";drop schema public;--"`, quoteIdentifier(`schema";drop schema public;--`))
	require.Equal(t, `"schema""x"."table"";drop table x;--"`, quoteQualifiedIdentifier(`schema"x`, `table";drop table x;--`))
	require.Equal(t, `"ROW_ID", "COLUMN""NAME", "TABLE_NAME"`, quoteColumnList(`ROW_ID, COLUMN"NAME, TABLE_NAME`))
	require.Equal(t, `'evil\\'`, quoteStringLiteral(`evil\`))
	require.Equal(t, `'evil\\''; DROP TABLE x; --'`, quoteStringLiteral(`evil\'; DROP TABLE x; --`))
}

func TestTableManagerQuotesIdentifiers(t *testing.T) {
	manager := newStandardTableManager()
	query := manager.createTableQuery(quoteIdentifier(`schema"x`), `table";drop table x;--`, model.TableSchema{
		`id";drop table x;--`: "string",
	})
	require.Contains(t, query, `"schema""x"."table"";drop table x;--"`)
	require.Contains(t, query, `"id"";drop table x;--" varchar`)
	require.False(t, strings.Contains(query, `%q`))
}

func TestEscapeCharacterMatrix(t *testing.T) {
	const sink = "a\"b`c]d'e\\f" // a " b ` c ] d ' e \ f

	// Double-quoted identifiers: only " is doubled, the backslash stays literal.
	require.Equal(t, "\"a\"\"b`c]d'e\\f\"", quoteIdentifier(sink))
	// String literals: Snowflake honours backslash escapes, so both ' and \ are escaped.
	require.Equal(t, "'a\"b`c]d''e\\\\f'", quoteStringLiteral(sink))
}

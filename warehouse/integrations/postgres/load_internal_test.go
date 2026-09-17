package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifiers(t *testing.T) {
	require.Equal(t, `"id","received_at"`, quoteIdentifiers([]string{"id", "received_at"}))
	require.Equal(t,
		`"x"" text);drop table users;--","evil_bs\"`,
		quoteIdentifiers([]string{`x" text);drop table users;--`, `evil_bs\`}),
	)
}

func TestQuoteColumnList(t *testing.T) {
	require.Equal(t, `"row_id", "column_name", "table_name"`, quoteColumnList("row_id, column_name, table_name"))
	require.Equal(t, `"x"" text);drop table users;--"`, quoteColumnList(`x" text);drop table users;--`))
}

func TestEscapeCharacterMatrix(t *testing.T) {
	const sink = "a\"b`c]d'e\\f" // a " b ` c ] d ' e \ f

	// Double-quoted identifiers: only " is doubled, the backslash stays literal.
	require.Equal(t, "\"a\"\"b`c]d'e\\f\"", quoteIdentifiers([]string{sink}))
}

package azuresynapse

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, `[schema]];drop schema public;--]`, quoteIdentifier(`schema];drop schema public;--`))
	require.Equal(t, `[schema]]x].[table]];drop table x;--]`, quoteQualifiedIdentifier(`schema]x`, `table];drop table x;--`))
	require.Equal(t, `[row_id], [column]]name], [table_name]`, quoteColumnList(`row_id, column]name, table_name`))

	columns := columnsWithDataTypes(model.TableSchema{
		`id];drop table x;--`: "string",
		`received]at`:         "datetime",
	}, "")
	require.Contains(t, columns, `[id]];drop table x;--] varchar(512)`)
	require.Contains(t, columns, `[received]]at] datetimeoffset`)
	require.False(t, strings.Contains(columns, `"id];drop table x;--"`))
}

func TestDeleteByRemainsNotImplemented(t *testing.T) {
	err := (&AzureSynapse{}).DeleteBy(context.Background(), []string{`events`}, warehouseutils.DeleteByParams{})
	require.EqualError(t, err, warehouseutils.NotImplementedErrorCode)
}

func TestEscapeCharacterMatrix(t *testing.T) {
	const sink = "a\"b`c]d'e\\f" // a " b ` c ] d ' e \ f

	// Bracket identifiers: only ] is doubled, the backslash stays literal.
	require.Equal(t, "[a\"b`c]]d'e\\f]", quoteIdentifier(sink))
	require.Equal(t, "[ns].[a\"b`c]]d'e\\f]", quoteQualifiedIdentifier("ns", sink))
	// String literals: only ' is doubled, the backslash stays literal.
	require.Equal(t, "'a\"b`c]d''e\\f'", quoteStringLiteral(sink))
	require.Equal(t, "N'a\"b`c]d''e\\f'", quoteUnicodeStringLiteral(sink))
	// OBJECT_ID guard literal: bracket-quote first, then escape as a unicode literal.
	require.Equal(t, "N'[ns].[t''x]]]'", quoteQualifiedIdentifierLiteral("ns", "t'x]"))
}

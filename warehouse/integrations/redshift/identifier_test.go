package redshift

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, `"schema"";drop schema public;--"`, quoteIdentifier(`schema";drop schema public;--`))
	require.Equal(t, `"schema""x"."table"";drop table x;--"`, quoteQualifiedIdentifier(`schema"x`, `table";drop table x;--`))
	require.Equal(t, `"row_id", "column""name", "table_name"`, quoteColumnList(`row_id, column"name, table_name`))
	require.Equal(t, `'s3://bucket/prefix/manifest.json'`, quoteStringLiteral(`s3://bucket/prefix/manifest.json`))
	require.Equal(t, `'evil\\'`, quoteStringLiteral(`evil\`))
	require.Equal(t, `'evil\\''; DROP TABLE x; --'`, quoteStringLiteral(`evil\'; DROP TABLE x; --`))

	columns := ColumnsWithDataTypes(model.TableSchema{
		`id";drop table x;--`: "string",
		`received"at`:         "datetime",
	}, "")
	require.Contains(t, columns, `"id"";drop table x;--" varchar(65535)`)
	require.Contains(t, columns, `"received""at" timestamp`)
	require.False(t, strings.Contains(columns, `%q`))
}

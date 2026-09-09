package deltalake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, "`schema``;drop schema public;--`", quoteIdentifier("schema`;drop schema public;--"))
	require.Equal(t, "`schema``x`.`table``;drop table x;--`", quoteQualifiedIdentifier("schema`x", "table`;drop table x;--"))
	require.Equal(t, "`row_id`,`column``name`,`table_name`", quoteIdentifiers([]string{"row_id", "column`name", "table_name"}))
	require.Equal(t, "`row_id`", primaryKey("rudder_discards"))
}

func TestColumnsWithDataTypesQuotesIdentifiers(t *testing.T) {
	columns := columnsWithDataTypes(model.TableSchema{
		"received_at":             "datetime",
		"id`;drop table x;--":     "string",
		"context`dangerous`value": "string",
	}, "")
	require.Contains(t, columns, "`id``;drop table x;--` STRING")
	require.Contains(t, columns, "`context``dangerous``value` STRING")
	require.Contains(t, columns, "`received_at` TIMESTAMP")
	require.Contains(t, columns, "`event_date` DATE GENERATED ALWAYS AS ( CAST(`received_at` AS DATE) )")
	require.False(t, strings.Contains(columns, "id`;drop table"))
}

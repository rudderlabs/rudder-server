package deltalake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

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

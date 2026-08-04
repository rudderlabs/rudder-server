package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TestColumnsWithDataTypesNeutralizesSQLInjection proves the reason for the
// identifier-quoting change on ClickHouse: a malicious column name that tries to
// break out of the double-quoted identifier in the generated CREATE TABLE column
// list must be neutralized by doubling the embedded double quote.
func TestColumnsWithDataTypesNeutralizesSQLInjection(t *testing.T) {
	ch := &Clickhouse{}

	payloads := map[string]string{
		"drop_table":          `x" String);drop table rudder_secrets;--`,
		"rce_copy_to_program": `x" String);copy (select '') to program 'id>/tmp/rce';--`,
	}

	for name, columnName := range payloads {
		t.Run(name, func(t *testing.T) {
			// A regular (non users/identifies) table, no not-nullable columns.
			fragment := ch.ColumnsWithDataTypes(warehouseutils.DiscardsTable, model.TableSchema{
				columnName: model.StringDataType,
			}, nil)

			require.Contains(t, fragment, warehouseutils.DoubleQuoteIdentifier(columnName))
			require.NotContains(t, fragment, columnName)
		})
	}
}

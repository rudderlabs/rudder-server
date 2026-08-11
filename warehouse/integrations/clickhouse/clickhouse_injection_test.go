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
// list must be neutralized. ClickHouse uses string-literal (backslash) escaping
// inside double-quoted identifiers - so the delimiter and any backslash are
// escaped with a backslash, NOT by doubling. The backslash_breakout case
// specifically guards against the doubling regression, where a trailing backslash
// would escape the closing quote and let the rest of the column list run on.
func TestColumnsWithDataTypesNeutralizesSQLInjection(t *testing.T) {
	ch := &Clickhouse{}

	payloads := map[string]string{
		"drop_table":          `x" String);drop table rudder_secrets;--`,
		"rce_copy_to_program": `x" String);copy (select '') to program 'id>/tmp/rce';--`,
		"backslash_breakout":  `x\" String);drop table rudder_secrets;--`,
	}

	for name, columnName := range payloads {
		t.Run(name, func(t *testing.T) {
			// A regular (non users/identifies) table, no not-nullable columns.
			fragment := ch.ColumnsWithDataTypes(warehouseutils.DiscardsTable, model.TableSchema{
				columnName: model.StringDataType,
			}, nil)

			require.Contains(t, fragment, warehouseutils.ClickhouseQuoteIdentifier(columnName))
			require.NotContains(t, fragment, columnName)
		})
	}
}

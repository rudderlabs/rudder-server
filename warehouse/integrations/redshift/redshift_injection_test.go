package redshift_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TestColumnsWithDataTypesNeutralizesSQLInjection proves the reason for the
// identifier-quoting change: a malicious event property whose name is crafted to
// break out of the double-quoted identifier in the generated CREATE TABLE column
// list must be neutralized. The embedded double quote has to be doubled so the
// injected `);drop table ...` / `copy ... to program ...` payload stays inert
// inside the identifier instead of terminating it.
func TestColumnsWithDataTypesNeutralizesSQLInjection(t *testing.T) {
	// Exact payloads from the reported warehouse identifier-injection: a DDL-drop
	// primitive and the COPY-TO-PROGRAM remote-code-execution escalation.
	payloads := map[string]string{
		"drop_table":          `x" text);drop table rudder_secrets;--`,
		"rce_copy_to_program": `x" text);copy (select '') to program 'id>/tmp/rce';--`,
	}

	for name, columnName := range payloads {
		t.Run(name, func(t *testing.T) {
			fragment := redshift.ColumnsWithDataTypes(model.TableSchema{
				columnName: model.StringDataType,
			}, "")

			// The column name is emitted as a properly double-quoted identifier
			// with the embedded `"` doubled.
			require.Contains(t, fragment, warehouseutils.DoubleQuoteIdentifier(columnName))
			// The raw, unescaped payload must not survive verbatim - if it did, it
			// would close the identifier early and the trailing SQL would execute.
			require.NotContains(t, fragment, columnName)
		})
	}
}

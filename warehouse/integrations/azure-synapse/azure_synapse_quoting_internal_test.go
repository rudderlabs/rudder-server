package azuresynapse

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// TestColumnsWithDataTypesNeutralizesSQLInjection proves the reason for the
// identifier-quoting change on Azure Synapse (SQL Server), where identifiers are
// bracket-delimited. A malicious column name that tries to break out of the
// `[...]` identifier in the generated CREATE TABLE column list must be
// neutralized by doubling the closing bracket, keeping the injected
// `); drop table ...` payload inert.
func TestColumnsWithDataTypesNeutralizesSQLInjection(t *testing.T) {
	// Bracket dialects break out on `]`, so the payload targets the closing
	// bracket rather than a double quote.
	payloads := map[string]string{
		"drop_secrets": `x] bigint); drop table rudder_secrets; --`,
		"drop_users":   `x] bigint); drop table users; --`,
	}

	for name, columnName := range payloads {
		t.Run(name, func(t *testing.T) {
			fragment := columnsWithDataTypes(model.TableSchema{
				columnName: model.StringDataType,
			}, "")

			// Assert the expected text rather than re-deriving it from the helper,
			// so a change to the escaping rule has to be stated here.
			require.Contains(t, fragment, "["+strings.ReplaceAll(columnName, "]", "]]")+"]")
			require.NotContains(t, fragment, columnName)
		})
	}
}

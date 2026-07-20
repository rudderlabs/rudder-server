package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// TestColumnsWithDataTypesQuotesIdentifiers ensures a malicious column name
// cannot break out of the double-quoted identifier in the generated CREATE
// TABLE column fragment. The double quote must be doubled.
func TestColumnsWithDataTypesQuotesIdentifiers(t *testing.T) {
	columnName := `x" NUMBER); DROP TABLE users; --`

	fragment := columnsWithDataTypes(model.TableSchema{
		columnName: model.StringDataType,
	}, map[string]string{model.StringDataType: "varchar"})

	require.Contains(t, fragment, `"x"" NUMBER); DROP TABLE users; --"`)
	require.NotContains(t, fragment, `x" NUMBER); DROP`)
}

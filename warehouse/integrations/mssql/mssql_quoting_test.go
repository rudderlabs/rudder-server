package mssql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// TestColumnsWithDataTypesQuotesBracketIdentifiers ensures a malicious column
// name cannot break out of the bracket-delimited identifier in the generated
// CREATE TABLE column fragment. The closing bracket must be doubled.
func TestColumnsWithDataTypesQuotesBracketIdentifiers(t *testing.T) {
	columnName := `x] int); DROP TABLE users; --`

	fragment := mssql.ColumnsWithDataTypes(model.TableSchema{
		columnName: model.StringDataType,
	}, "")

	require.Contains(t, fragment, `[x]] int); DROP TABLE users; --]`)
	require.NotContains(t, fragment, `x] int); DROP`)
}

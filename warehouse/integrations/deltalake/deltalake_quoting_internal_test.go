package deltalake

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TestColumnsWithDataTypesQuotesBacktickIdentifiers ensures a malicious column
// name cannot break out of the backtick-delimited identifier in the generated
// CREATE TABLE column fragment. The backtick must be doubled.
func TestColumnsWithDataTypesQuotesBacktickIdentifiers(t *testing.T) {
	columnName := "x` STRING); DROP TABLE users; --"

	fragment := columnsWithDataTypes(model.TableSchema{
		columnName: model.StringDataType,
	}, "")

	require.Contains(t, fragment, "`x`` STRING); DROP TABLE users; --`")
	require.NotContains(t, fragment, "x` STRING); DROP")
}

func TestQualifiedTableNamesQuoteBacktickIdentifiers(t *testing.T) {
	tableName := "evil_table`); DROP TABLE victim_secrets; --"

	qualifiedName := warehouseutils.BacktickQuoteQualifiedIdentifier("namespace", tableName)

	require.Equal(t, "`namespace`.`evil_table``); DROP TABLE victim_secrets; --`", qualifiedName)
	require.NotContains(t, qualifiedName, "evil_table`); DROP")
}

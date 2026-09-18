package snowflake

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestTableManagerQuotesIdentifiers(t *testing.T) {
	manager := newStandardTableManager()
	query := manager.createTableQuery(whutils.DoubleQuoteIdentifier(`schema"x`), `table";drop table x;--`, model.TableSchema{
		`id";drop table x;--`: "string",
	})
	require.Contains(t, query, `"schema""x"."table"";drop table x;--"`)
	require.Contains(t, query, `"id"";drop table x;--" varchar`)
	require.False(t, strings.Contains(query, `%q`))
}

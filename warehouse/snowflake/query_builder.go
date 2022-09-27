package snowflake

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func addColumnsSQLStatement(namespace, tableName string, columnsInfo warehouseutils.ColumnsInto) string {
	format := func(_ int, columnInfo warehouseutils.ColumnInfoT) string {
		return fmt.Sprintf(`
		ADD %s %s`,
			columnInfo.Name,
			dataTypesMapToRudder[columnInfo.Type],
		)
	}
	return fmt.Sprintf(`
		ALTER TABLE
		%s.%s %s;`,
		namespace,
		tableName,
		columnsInfo.JoinColumns(format, ","),
	)
}

package azuresynapse

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func addColumnsSQLStatement(namespace, tableName string, columnsInfo warehouseutils.ColumnsInto) string {
	format := func(_ int, columnInfo warehouseutils.ColumnInfoT) string {
		return fmt.Sprintf(`%s %s`, columnInfo.Name, rudderDataTypesMapToMssql[columnInfo.Type])
	}
	return fmt.Sprintf(`
		ALTER TABLE
		%s.%s
		ADD %s;`,
		namespace,
		tableName,
		columnsInfo.JoinColumns(format, ","),
	)
}

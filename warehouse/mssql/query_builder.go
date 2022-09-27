package mssql

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func addColumnsSQLStatement(namespace, tableName string, columnsInfo warehouseutils.ColumnsInto) string {
	sqlStatement := fmt.Sprintf(`
		ALTER TABLE
		%s.%s`,
		namespace,
		tableName,
	)
	format := func(idx int, columnInfo warehouseutils.ColumnInfoT) string {
		return fmt.Sprintf(`
		ADD %s %s`,
			columnInfo.Name,
			rudderDataTypesMapToMssql[columnInfo.Type],
		)
	}
	sqlStatement += columnsInfo.JoinColumns(format, ",")
	return sqlStatement
}

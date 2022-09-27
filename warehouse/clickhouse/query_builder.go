package clickhouse

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func addColumnsSQLStatement(namespace, tableName, clusterClause string, columnsInfo warehouseutils.ColumnsInto) string {
	sqlStatement := fmt.Sprintf(`
		ALTER TABLE
		%q.%q %s`,
		namespace,
		tableName,
		clusterClause,
	)
	format := func(idx int, columnInfo warehouseutils.ColumnInfoT) string {
		return fmt.Sprintf(`
		ADD COLUMN IF NOT EXISTS %q %s`,
			columnInfo.Name,
			getClickHouseColumnTypeForSpecificTable(tableName, columnInfo.Name, rudderDataTypesMapToClickHouse[columnInfo.Type], false),
		)
	}
	sqlStatement += columnsInfo.JoinColumns(format, ",")
	return sqlStatement
}

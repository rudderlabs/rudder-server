package clickhouse

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func addColumnsSQLStatement(namespace, tableName, clusterClause string, columnsInfo warehouseutils.ColumnsInto) string {
	format := func(_ int, columnInfo warehouseutils.ColumnInfoT) string {
		return fmt.Sprintf(`
		ADD COLUMN IF NOT EXISTS %q %s`,
			columnInfo.Name,
			getClickHouseColumnTypeForSpecificTable(tableName, columnInfo.Name, rudderDataTypesMapToClickHouse[columnInfo.Type], false),
		)
	}
	return fmt.Sprintf(`
		ALTER TABLE
		%q.%q %s %s;`,
		namespace,
		tableName,
		clusterClause,
		columnsInfo.JoinColumns(format, ","),
	)
}

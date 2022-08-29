package bigquery

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

func dropDanglingTablesSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		SELECT
		  table_name
		FROM
		  % [1]s.INFORMATION_SCHEMA.TABLES
		WHERE
		  table_schema = '%[1]s'
		  AND table_name LIKE '%[2]s%[3]s'
	`,
		namespace,
		warehouseutils.StagingTablePrefix(provider),
		"%",
	)
}

func fetchSchemaSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		SELECT
		  t.table_name,
		  c.column_name,
		  c.data_type
		FROM
		  %[1]s.INFORMATION_SCHEMA.TABLES as t
		  LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name)
		  and (t.table_type != 'VIEW')
		  and (
			c.column_name != '_PARTITIONTIME'
			OR c.column_name IS NULL
		  )
	`,
		namespace,
	)
}

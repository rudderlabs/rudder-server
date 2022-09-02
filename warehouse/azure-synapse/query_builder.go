package azuresynapse

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

func dropDanglingTablesSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		select
		  table_name
		from
		  information_schema.tables
		where
		  table_schema = '%s'
		  AND table_name like '%s%s';
	`,
		namespace,
		warehouseutils.StagingTablePrefix(provider),
		"%",
	)
}

func fetchSchemaSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		SELECT
		  table_name,
		  column_name,
		  data_type
		FROM
		  INFORMATION_SCHEMA.COLUMNS
		WHERE
		  table_schema = '%s'
		  and table_name not like '%s%s'
	`,
		namespace,
		warehouseutils.StagingTablePrefix(provider),
		"%",
	)
}

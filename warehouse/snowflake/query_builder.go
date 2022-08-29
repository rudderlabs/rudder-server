package snowflake

import (
	"fmt"
)

func fetchSchemaSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		SELECT
		  t.table_name,
		  c.column_name,
		  c.data_type
		FROM
		  INFORMATION_SCHEMA.TABLES as t
		  JOIN INFORMATION_SCHEMA.COLUMNS as c ON t.table_schema = c.table_schema
		  and t.table_name = c.table_name
		WHERE
		  t.table_schema = '%s'
	`,
		namespace,
	)
}

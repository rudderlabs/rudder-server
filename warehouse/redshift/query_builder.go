package redshift

import (
	"fmt"
)

func addColumnSQLStatement(tableName, columnName, columnType string) string {
	return fmt.Sprintf(`
		ALTER TABLE
		  %v
		ADD
		  COLUMN %q %s
	`,
		tableName,
		columnName,
		getRSDataType(columnType),
	)
}

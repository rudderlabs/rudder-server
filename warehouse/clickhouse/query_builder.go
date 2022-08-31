package clickhouse

import "fmt"

func createTemporaryTableSQLStatement(namespace, tableName, stagingTableName string) string {
	return fmt.Sprintf(`CREATE TABLE %[1]q.%[2]q AS %[1]q.%[3]q`, namespace, stagingTableName, tableName)
}

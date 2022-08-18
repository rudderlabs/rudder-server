package deltalake

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

func primaryKey(tableName string) string {
	key := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		key = column
	}
	return key
}

func stagingSqlStatement(namespace, tableName, stagingTableName string, columnKeys []string) (sqlStatement string) {
	pk := primaryKey(tableName)
	if tableName == warehouseutils.UsersTable {
		sqlStatement = fmt.Sprintf(`SELECT %[3]s FROM %[1]s.%[2]s`,
			namespace,
			stagingTableName,
			columnNames(columnKeys),
		)
	} else {
		sqlStatement = fmt.Sprintf(`SELECT * FROM ( SELECT *, row_number() OVER (PARTITION BY %[3]s ORDER BY RECEIVED_AT DESC) AS _rudder_staging_row_number FROM %[1]s.%[2]s ) AS q WHERE _rudder_staging_row_number = 1`,
			namespace,
			stagingTableName,
			pk,
		)
	}
	return
}

// mergeableLTSQLStatement merge load table sql statement
func mergeableLTSQLStatement(namespace, tableName, stagingTableName string, columnKeys []string, partitionQuery string) (sqlStatement string) {
	pk := primaryKey(tableName)
	if partitionQuery != "" {
		partitionQuery += " AND"
	}
	stagingTableSqlStatement := stagingSqlStatement(namespace, tableName, stagingTableName, columnKeys)
	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN
                                       USING ( %[3]s ) AS STAGING
									   ON %[8]s MAIN.%[4]s = STAGING.%[4]s
									   WHEN MATCHED THEN UPDATE SET %[5]s
									   WHEN NOT MATCHED THEN INSERT (%[6]s) VALUES (%[7]s);`,
		namespace,
		tableName,
		stagingTableSqlStatement,
		pk,
		columnsWithValues(columnKeys),
		columnNames(columnKeys),
		stagingColumnNames(columnKeys),
		partitionQuery,
	)
	return
}

// appendableLTSQLStatement append load table sql statement
func appendableLTSQLStatement(namespace, tableName, stagingTableName string, columnKeys []string) (sqlStatement string) {
	stagingTableSqlStatement := stagingSqlStatement(namespace, tableName, stagingTableName, columnKeys)
	sqlStatement = fmt.Sprintf(`INSERT INTO %[1]s.%[2]s (%[4]s) SELECT %[4]s FROM ( %[5]s );`,
		namespace,
		tableName,
		stagingTableName,
		columnNames(columnKeys),
		stagingTableSqlStatement,
	)
	return
}

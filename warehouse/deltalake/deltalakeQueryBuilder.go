package deltalake

import "fmt"

type LoadTableStrategy interface {
	GenerateLoadSQLStatement(namespace string, tableName string, stagingTableName string, columnKeys []string) (sqlStatement string)
}

type ByMerge struct{}
type ByAppend struct{}

func (*ByMerge) GenerateLoadSQLStatement(namespace string, tableName string, stagingTableName string, columnKeys []string) (sqlStatement string) {
	// Getting the primary key for the merge sql statement
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	// Creating merge sql statement to copy from staging table to the main table
	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN
                                       USING ( SELECT * FROM ( SELECT *, row_number() OVER (PARTITION BY %[4]s ORDER BY RECEIVED_AT DESC) AS _rudder_staging_row_number FROM %[1]s.%[3]s ) AS q WHERE _rudder_staging_row_number = 1) AS STAGING
									   ON MAIN.%[4]s = STAGING.%[4]s
									   WHEN MATCHED THEN UPDATE SET %[5]s
									   WHEN NOT MATCHED THEN INSERT (%[6]s) VALUES (%[7]s);`,
		namespace,
		tableName,
		stagingTableName,
		primaryKey,
		columnsWithValues(columnKeys),
		columnNames(columnKeys),
		stagingColumnNames(columnKeys),
	)
	return
}

func (*ByAppend) GenerateLoadSQLStatement(namespace string, tableName string, stagingTableName string, columnKeys []string) (sqlStatement string) {
	// Creating insert sql statement to copy from staging table to the main table
	sqlStatement = fmt.Sprintf(`INSERT INTO %[1]s.%[2]s (%[4]s)
                                       SELECT %[4]s FROM ( SELECT * FROM ( SELECT *, row_number() OVER (PARTITION BY %[4]s ORDER BY RECEIVED_AT DESC) AS _rudder_staging_row_number FROM %[1]s.%[3]s ) AS q WHERE _rudder_staging_row_number = 1) AS r;`,
		namespace,
		tableName,
		stagingTableName,
		columnNames(columnKeys),
	)
	return sqlStatement
}

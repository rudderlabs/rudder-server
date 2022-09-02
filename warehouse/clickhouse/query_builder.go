package clickhouse

import "fmt"

func createTemporaryTableWithS3EngineSQLStatement(namespace, stagingTableName, loadFolder, accessKeyID, secretAccessKey, columnsWithName string) string {
	return fmt.Sprintf(`
		CREATE TABLE %[1]q.%[2]q (
		  %[3]s
		) ENGINE = S3(
		  '%[4]s',
		  '%[5]s',
		  '%[6]s',
		  'CSV',
          'gzip'
		) settings date_time_input_format = 'best_effort';
		`,
		namespace,        // 1
		stagingTableName, // 2
		columnsWithName,  // 3
		loadFolder,       // 4
		accessKeyID,      // 5
		secretAccessKey,  // 6
	)
}

func insertIntoTemporaryTableSQLStatement(namespace, tableName, stagingTableName, columnsWithName string) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (
			%[4]s
		)
		select
		  %[4]s
		from
		  %[1]q.%[3]q;
		`,
		namespace,        // 1
		tableName,        // 2
		stagingTableName, // 3
		columnsWithName,  // 4
	)
}

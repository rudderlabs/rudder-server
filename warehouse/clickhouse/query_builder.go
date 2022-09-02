package clickhouse

import "fmt"

func loadTableWithS3EngineSQLStatement(namespace, tableName, loadFolder, accessKeyID, secretAccessKey, columns, columnsWithDataTypes string) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (
			%[3]s
		)
		SELECT
		  *
		FROM
		  s3(
			'%[4]s',
		  	'%[5]s',
		  	'%[6]s',
			'CSV',
			'%[7]s',
			'gz'
		  ) settings date_time_input_format = 'best_effort';
		`,
		namespace,            // 1
		tableName,            // 2
		columns,              // 3
		loadFolder,           // 4
		accessKeyID,          // 5
		secretAccessKey,      // 6
		columnsWithDataTypes, // 7
	)
}

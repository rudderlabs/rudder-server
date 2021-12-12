package deltalake

import (
	"context"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/proto/databricks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/grpc"
	"strings"
	"time"
)

// Database configuration
const (
	DLHost          = "host"
	DLPort          = "port"
	DLPath          = "path"
	DLToken         = "token"
	AWSAccessKey    = "accessKey"
	AWSAccessSecret = "accessKeyID"
)

// Reference: https://docs.oracle.com/cd/E17952_01/connector-odbc-en/connector-odbc-reference-errorcodes.html
const (
	tableOrViewNotFound = "42S02"
	databaseNotFound    = "42000"
)

var (
	stagingTablePrefix string
	pkgLogger          logger.LoggerI
	schema             string
	sparkServerType    string
	authMech           string
	uid                string
	thriftTransport    string
	ssl                string
	userAgent          string
	grpcTimeout        time.Duration
)

// Rudder data type mapping with Delta lake mappings.
var dataTypesMap = map[string]string{
	"boolean":  "BOOLEAN",
	"int":      "BIGINT",
	"float":    "DOUBLE",
	"string":   "STRING",
	"datetime": "TIMESTAMP",
}

// Delta Lake mapping with rudder data types mappings.
// Reference: https://docs.databricks.com/sql/language-manual/sql-ref-datatype-rules.html
var dataTypesMapToRudder = map[string]string{
	"TINYINT":   "int",
	"SMALLINT":  "int",
	"INT":       "int",
	"BIGINT":    "int",
	"DECIMAL":   "float",
	"FLOAT":     "float",
	"DOUBLE":    "float",
	"BOOLEAN":   "boolean",
	"STRING":    "string",
	"DATE":      "datetime",
	"TIMESTAMP": "datetime",
	"tinyint":   "int",
	"smallint":  "int",
	"int":       "int",
	"bigint":    "int",
	"decimal":   "float",
	"float":     "float",
	"double":    "float",
	"boolean":   "boolean",
	"string":    "string",
	"date":      "datetime",
	"timestamp": "datetime",
}

// Primary Key mappings for tables
var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

type HandleT struct {
	dbHandleT     *databricks.DBHandleT
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

// Init initializes the delta lake warehouse
func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("deltalake")
	databricks.Init()
}

// loadConfig loads config
func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
	config.RegisterStringConfigVariable("default", &schema, false, "Warehouse.deltalake.schema")
	config.RegisterStringConfigVariable("3", &sparkServerType, false, "Warehouse.deltalake.sparkServerType")
	config.RegisterStringConfigVariable("3", &authMech, false, "Warehouse.deltalake.authMech")
	config.RegisterStringConfigVariable("token", &uid, false, "Warehouse.deltalake.uid")
	config.RegisterStringConfigVariable("2", &thriftTransport, false, "Warehouse.deltalake.thriftTransport")
	config.RegisterStringConfigVariable("1", &ssl, false, "Warehouse.deltalake.ssl")
	config.RegisterStringConfigVariable("RudderStack", &userAgent, false, "Warehouse.deltalake.userAgent")
	config.RegisterDurationConfigVariable(time.Duration(2), &grpcTimeout, false, time.Minute, "Warehouse.deltalake.grpcTimeout")
}

// getDeltaLakeDataType returns datatype for delta lake which is mapped with rudder stack datatype
func getDeltaLakeDataType(columnType string) string {
	return dataTypesMap[columnType]
}

// columnsWithDataTypes returns columns with specified prefix and data type
func columnsWithDataTypes(columns map[string]string, prefix string) string {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	format := func(idx int, name string) string {
		return fmt.Sprintf(`%s%s %s`, prefix, name, getDeltaLakeDataType(columns[name]))
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// columnNames returns joined column with comma separated
func columnNames(keys []string) string {
	return strings.Join(keys[:], ",")
}

// stagingColumnNames returns staging column names
func stagingColumnNames(keys []string) string {
	format := func(idx int, str string) string {
		return fmt.Sprintf(`STAGING.%s`, str)
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// columnsWithValues returns main and staging column values.
func columnsWithValues(keys []string) string {
	format := func(idx int, str string) string {
		return fmt.Sprintf(`MAIN.%[1]s = STAGING.%[1]s`, str)
	}
	return warehouseutils.JoinWithFormatting(keys, format, ",")
}

// GetDatabricksConnectorURL returns databricks connector url.
func GetDatabricksConnectorURL() string {
	return config.GetEnv("DATABRICKS_CONNECTOR_URL", "localhost:50051")
}

// checkAndIgnoreAlreadyExistError checks and ignores native errors.
func checkAndIgnoreAlreadyExistError(errorCode string, ignoreError string) bool {
	if errorCode == "" || errorCode == ignoreError {
		return true
	}
	return false
}

// connect creates database connection with CredentialsT
func (dl *HandleT) connect(cred *databricks.CredentialsT) (dbHandleT *databricks.DBHandleT, err error) {
	connStat := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "Connect",
	})
	connStat.Start()
	defer connStat.End()

	closeConnStat := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "Close",
	})

	ctx := context.Background()
	identifier := uuid.Must(uuid.NewV4()).String()
	connConfig := &proto.ConnectionConfig{
		Host:            cred.Host,
		Port:            cred.Port,
		HttpPath:        cred.Path,
		Pwd:             cred.Token,
		Schema:          cred.Schema,
		SparkServerType: cred.SparkServerType,
		AuthMech:        cred.AuthMech,
		Uid:             cred.UID,
		ThriftTransport: cred.ThriftTransport,
		Ssl:             cred.SSL,
		UserAgentEntry:  cred.UserAgentEntry,
	}

	// Getting timeout context
	tCtx, cancel := context.WithTimeout(ctx, grpcTimeout)
	defer cancel()

	// Creating grpc connection using timeout context
	conn, err := grpc.DialContext(tCtx, GetDatabricksConnectorURL(), grpc.WithInsecure(), grpc.WithBlock())
	if err == context.DeadlineExceeded {
		execTimeouts := stats.NewStat("warehouse.clickhouse.grpcTimeouts", stats.CountType)
		execTimeouts.Count(1)

		err = fmt.Errorf("%s Connection timed out to Delta lake: %v", dl.GetLogIdentifier(), err)
		return
	}
	if err != nil {
		err = fmt.Errorf("%s Error while creating grpc connection to Delta lake: %v", dl.GetLogIdentifier(), err)
		return
	}

	dbClient := proto.NewDatabricksClient(conn)
	connectionResponse, err := dbClient.Connect(ctx, &proto.ConnectRequest{
		Config:     connConfig,
		Identifier: identifier,
	})
	if err != nil {
		err = fmt.Errorf("%s Error connecting to Delta lake: %v", dl.GetLogIdentifier(), err)
		return
	}
	if connectionResponse.GetErrorCode() != "" {
		err = fmt.Errorf("%s Error connecting to Delta lake with response:%v", dl.GetLogIdentifier(), connectionResponse.GetErrorMessage())
		return
	}

	dbHandleT = &databricks.DBHandleT{
		CredConfig:     connConfig,
		CredIdentifier: identifier,
		Conn:           conn,
		Client:         dbClient,
		Context:        ctx,
		CloseStats:     closeConnStat,
	}
	return
}

// fetchTables fetch tables with tableNames
func (dl *HandleT) fetchTables(dbT *databricks.DBHandleT, sqlStatement string) (tableNames []string, err error) {
	fetchTablesExecTime := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "FetchTables",
	})
	fetchTablesExecTime.Start()
	defer fetchTablesExecTime.End()

	fetchTableResponse, err := dbT.Client.FetchTables(dbT.Context, &proto.FetchTablesRequest{
		Config:     dl.dbHandleT.CredConfig,
		Identifier: dbT.CredIdentifier,
		Schema:     sqlStatement,
	})
	if err != nil {
		return tableNames, fmt.Errorf("%s Error while fetching tables: %v", dl.GetLogIdentifier(), err)
	}
	if !checkAndIgnoreAlreadyExistError(fetchTableResponse.GetErrorCode(), databaseNotFound) {
		err = fmt.Errorf("%s Error while fetching tables with response: %v", dl.GetLogIdentifier(), fetchTableResponse.GetErrorMessage())
		return
	}
	tableNames = append(tableNames, fetchTableResponse.GetTables()...)
	return
}

// ExecuteSQL executes sql using grpc Client
func (dl *HandleT) ExecuteSQL(sqlStatement string, queryType string) (err error) {
	execSqlStatTime := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   queryType,
	})
	execSqlStatTime.Start()
	defer execSqlStatTime.End()

	executeResponse, err := dl.dbHandleT.Client.Execute(dl.dbHandleT.Context, &proto.ExecuteRequest{
		Config:       dl.dbHandleT.CredConfig,
		Identifier:   dl.dbHandleT.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return fmt.Errorf("%s Error while executing: %v", dl.GetLogIdentifier(), err)
	}
	if !checkAndIgnoreAlreadyExistError(executeResponse.GetErrorCode(), databaseNotFound) || !checkAndIgnoreAlreadyExistError(executeResponse.GetErrorCode(), tableOrViewNotFound) {
		err = fmt.Errorf("%s Error while executing with response: %v", dl.GetLogIdentifier(), executeResponse.GetErrorMessage())
		return
	}
	return
}

// schemaExists checks it schema exists or not.
func (dl *HandleT) schemaExists(schemaName string) (exists bool, err error) {
	fetchSchemasExecTime := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "FetchSchemas",
	})
	fetchSchemasExecTime.Start()
	defer fetchSchemasExecTime.End()

	sqlStatement := fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, schemaName)
	fetchSchemasResponse, err := dl.dbHandleT.Client.FetchSchemas(dl.dbHandleT.Context, &proto.FetchSchemasRequest{
		Config:       dl.dbHandleT.CredConfig,
		Identifier:   dl.dbHandleT.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return exists, fmt.Errorf("%s Error while fetching schemas: %v", dl.GetLogIdentifier(), err)
	}
	if !checkAndIgnoreAlreadyExistError(fetchSchemasResponse.GetErrorCode(), databaseNotFound) {
		err = fmt.Errorf("%s Error while fetching schemas with response: %v", dl.GetLogIdentifier(), fetchSchemasResponse.GetErrorMessage())
		return
	}
	exists = len(fetchSchemasResponse.GetDatabases()) == 1 && strings.Compare(fetchSchemasResponse.GetDatabases()[0], schemaName) == 0
	return
}

// createSchema creates schema
func (dl *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, dl.Namespace)
	pkgLogger.Infof("%s Creating schema in delta lake with SQL:%v", dl.GetLogIdentifier(), sqlStatement)
	err = dl.ExecuteSQL(sqlStatement, "CreateSchema")
	return
}

// dropStagingTables drops staging tables
func (dl *HandleT) dropStagingTables(tableNames []string) {
	dropTablesExecTime := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "DropStagingTables",
	})
	dropTablesExecTime.Start()
	defer dropTablesExecTime.End()

	for _, stagingTableName := range tableNames {
		pkgLogger.Infof("%s Dropping table %+v\n", dl.GetLogIdentifier(), stagingTableName)
		sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s.%[2]s;`, dl.Namespace, stagingTableName)
		dropTableResponse, err := dl.dbHandleT.Client.Execute(dl.dbHandleT.Context, &proto.ExecuteRequest{
			Config:       dl.dbHandleT.CredConfig,
			Identifier:   dl.dbHandleT.CredIdentifier,
			SqlStatement: sqlStatement,
		})
		if err == nil && !checkAndIgnoreAlreadyExistError(dropTableResponse.GetErrorCode(), tableOrViewNotFound) {
			continue
		}
		if err != nil {
			pkgLogger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), err)
		}
	}
}

// sortedColumnNames returns sorted column names
func (dl *HandleT) sortedColumnNames(tableSchemaInUpload warehouseutils.TableSchemaT, sortedColumnKeys []string) (sortedColumnNames string) {
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sortedColumnNames = strings.Join(sortedColumnKeys[:], ",")
	} else {
		// TODO: Explore adding headers to csv.
		format := func(index int, value string) string {
			csvColumnIndex := fmt.Sprintf(`%s%d`, "_c", index)
			columnName := value
			columnType := getDeltaLakeDataType(tableSchemaInUpload[columnName])
			return fmt.Sprintf(`CAST ( %s AS %s ) AS %s`, csvColumnIndex, columnType, columnName)
		}
		return warehouseutils.JoinWithFormatting(sortedColumnKeys, format, ",")
	}
	return
}

// credentialsStr return authentication for AWS STS and SSE-C encryption
// STS authentication is only supported with S3A client.
func (dl *HandleT) credentialsStr() (auth string, s3aClient bool, err error) {
	awsAccessKey := warehouseutils.GetConfigValue(AWSAccessKey, dl.Warehouse)
	awsSecretKey := warehouseutils.GetConfigValue(AWSAccessSecret, dl.Warehouse)

	if awsAccessKey == "" && awsSecretKey != "" {
		var tempAccessKeyId, tempSecretAccessKey, token string
		tempAccessKeyId, tempSecretAccessKey, token, err = warehouseutils.GetTemporaryS3Cred(awsSecretKey, awsAccessKey)
		if err != nil {
			return
		}
		s3aClient = true
		auth = fmt.Sprintf(`CREDENTIALS ( 'awsKeyId' = '%s', 'awsSecretKey' = '%s', 'awsSessionToken' = '%s' )`, tempAccessKeyId, tempSecretAccessKey, token)
	}
	return
}

// loadTable Loads table with table name
func (dl *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	// Getting sorted column keys from tableSchemaInUpload
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

	// Creating staging table
	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), tableName), 127)
	err = dl.CreateTable(stagingTableName, tableSchemaAfterUpload)
	if err != nil {
		return
	}

	// Dropping staging tables if required
	if !skipTempTableDelete {
		defer dl.dropStagingTables([]string{stagingTableName})
	}

	// Get the credentials string to copy from the staging location to table
	auth, s3aClient, err := dl.credentialsStr()
	if err != nil {
		return
	}

	// Getting the load folder where the load files are present
	csvObjectLocation, err := dl.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return
	}
	loadFolder := warehouseutils.GetObjectFolder(dl.ObjectStorage, csvObjectLocation)
	if s3aClient {
		loadFolder = strings.Replace(loadFolder, "s3://", "s3a://", 1)
	}

	// Creating copy sql statement to copy from load folder to the staging table
	var sortedColumnNames = dl.sortedColumnNames(tableSchemaInUpload, sortedColumnKeys)
	var sqlStatement string
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = PARQUET "+
			"PATTERN = '*.parquet' "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s;",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder, auth)
	} else {
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = CSV "+
			"PATTERN = '*.gz' "+
			"FORMAT_OPTIONS ( 'compression' = 'gzip', 'quote' = '\"', 'escape' = '\"' ) "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s;",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder, auth)
	}

	// Sanitising copy sql statement for logging
	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("%s Running COPY command with SQL: %s\n", dl.GetLogIdentifier(tableName), sanitisedSQLStmt)
	}

	// Executing copy sql statement
	err = dl.ExecuteSQL(sqlStatement, "LT::Copy")
	if err != nil {
		pkgLogger.Errorf("%s Error running COPY command with SQL: %s\n error: %v", dl.GetLogIdentifier(tableName), sqlStatement, err)
		return
	}

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
		dl.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		columnsWithValues(sortedColumnKeys),
		columnNames(sortedColumnKeys),
		stagingColumnNames(sortedColumnKeys),
	)
	pkgLogger.Infof("%v Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(tableName), sqlStatement)

	// Executing merge sql statement
	err = dl.ExecuteSQL(sqlStatement, "LT::Merge")
	if err != nil {
		pkgLogger.Errorf("%v Error inserting into original table: %v\n", dl.GetLogIdentifier(tableName), err)
		return
	}

	pkgLogger.Infof("%v Complete load for table\n", dl.GetLogIdentifier(tableName))
	return
}

// loadUserTables Loads users table
func (dl *HandleT) loadUserTables() (errorMap map[string]error) {
	// Creating errorMap
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("%s Starting load for identifies and users tables\n", dl.GetLogIdentifier())

	// Loading identifies tables
	identifyStagingTable, err := dl.loadTable(warehouseutils.IdentifiesTable, dl.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), dl.Uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	// dropping identifies staging table
	defer dl.dropStagingTables([]string{identifyStagingTable})

	// Checking if users schema is present in GetTableSchemaInUpload
	if len(dl.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	// Creating userColNames and firstValProps for create table sql statement
	userColMap := dl.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		// do not reference uuid in queries as it can be an auto incrementing field set by segment compatible tables
		if colName == "id" || colName == "user_id" || colName == "uuid" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s , TRUE) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}
	quotedUserColNames := warehouseutils.DoubleQuoteAndJoinByComma(userColNames)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), warehouseutils.UsersTable), 127)
	// Creating create table sql statement for staging users table
	sqlStatement := fmt.Sprintf(`CREATE TABLE %[1]s.%[2]s USING DELTA AS (SELECT DISTINCT * FROM
										(
											SELECT
											id, %[3]s
											FROM (
												(
													SELECT id, %[6]s FROM %[1]s.%[4]s WHERE id in (SELECT DISTINCT(user_id) FROM %[1]s.%[5]s WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[6]s FROM %[1]s.%[5]s WHERE user_id IS NOT NULL
												)
											)
										)
									);`,
		dl.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		warehouseutils.UsersTable,
		identifyStagingTable,
		quotedUserColNames,
	)

	// Executing create sql statement
	err = dl.ExecuteSQL(sqlStatement, "LUT::Create")
	if err != nil {
		pkgLogger.Errorf("%s Creating staging table for users failed with SQL: %s\n", dl.GetLogIdentifier(), sqlStatement)
		pkgLogger.Errorf("%s Error creating users staging table from original table and identifies staging table: %v\n", dl.GetLogIdentifier(), err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// Dropping staging users table
	defer dl.dropStagingTables([]string{stagingTableName})

	// Creating the Primary Key
	primaryKey := "id"

	// Creating the column Keys
	columnKeys := append([]string{`id`}, userColNames...)

	// Creating the merge sql statement to copy from staging users table to the main users table
	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s.%[2]s AS MAIN
									   USING ( SELECT %[5]s FROM %[1]s.%[3]s ) AS STAGING
									   ON MAIN.%[4]s = STAGING.%[4]s
									   WHEN MATCHED THEN UPDATE SET %[6]s
									   WHEN NOT MATCHED THEN INSERT (%[5]s) VALUES (%[7]s);`,
		dl.Namespace,
		warehouseutils.UsersTable,
		stagingTableName,
		primaryKey,
		columnNames(columnKeys),
		columnsWithValues(columnKeys),
		stagingColumnNames(columnKeys),
	)
	pkgLogger.Infof("%s Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(warehouseutils.UsersTable), sqlStatement)

	// Executing the merge sql statement
	err = dl.ExecuteSQL(sqlStatement, "LUT::Merge")
	if err != nil {
		pkgLogger.Errorf("%s Error inserting into users table from staging table: %v\n", err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

// dropDanglingStagingTables drop dandling staging tables.
func (dl *HandleT) dropDanglingStagingTables() {
	// Fetching the staging tables
	tableNames, err := dl.fetchTables(dl.dbHandleT, dl.Namespace)
	if err != nil {
		return
	}

	// Filtering tables based on not part of staging tables
	var filteredTablesNames []string
	for _, tableName := range tableNames {
		// Ignoring the staging tables
		if !strings.HasPrefix(tableName, stagingTablePrefix) {
			continue
		}
		filteredTablesNames = append(filteredTablesNames, tableName)
	}

	// Drop staging tables
	dl.dropStagingTables(filteredTablesNames)
	return
}

// connectToWarehouse returns the database connection configured with CredentialsT
func (dl *HandleT) connectToWarehouse() (*databricks.DBHandleT, error) {
	credT := &databricks.CredentialsT{
		Host:            warehouseutils.GetConfigValue(DLHost, dl.Warehouse),
		Port:            warehouseutils.GetConfigValue(DLPort, dl.Warehouse),
		Path:            warehouseutils.GetConfigValue(DLPath, dl.Warehouse),
		Token:           warehouseutils.GetConfigValue(DLToken, dl.Warehouse),
		Schema:          schema,
		SparkServerType: sparkServerType,
		AuthMech:        authMech,
		UID:             uid,
		ThriftTransport: thriftTransport,
		SSL:             ssl,
		UserAgentEntry:  userAgent,
	}
	return dl.connect(credT)
}

// CreateTable creates tables with table name and columns
func (dl *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`%s.%s`, dl.Namespace, tableName)
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v ) USING DELTA;`, name, columnsWithDataTypes(columns, ""))
	pkgLogger.Infof("%s Creating table in delta lake with SQL: %v", dl.GetLogIdentifier(tableName), sqlStatement)
	err = dl.ExecuteSQL(sqlStatement, "CreateTable")
	return
}

// AddColumn adds column for column name and type
func (dl *HandleT) AddColumn(name string, columnName string, columnType string) (err error) {
	tableName := fmt.Sprintf(`%s.%s`, dl.Namespace, name)
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ADD COLUMNS ( %s %s );`, tableName, columnName, getDeltaLakeDataType(columnType))
	pkgLogger.Infof("%s Adding column in delta lake with SQL:%v", dl.GetLogIdentifier(tableName, columnName), sqlStatement)
	err = dl.ExecuteSQL(sqlStatement, "AddColumn")
	return
}

// CreateSchema checks if schema exists or not. If it does not exist, it creates the schema.
func (dl *HandleT) CreateSchema() (err error) {
	// Checking if schema exists or not
	var schemaExists bool
	schemaExists, err = dl.schemaExists(dl.Namespace)
	if err != nil {
		pkgLogger.Errorf("%s Error checking if schema exists: %s, error: %v", dl.GetLogIdentifier(), dl.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("%s Skipping creating schema: %s since it already exists", dl.GetLogIdentifier(), dl.Namespace)
		return
	}

	// Creating schema
	return dl.createSchema()
}

// AlterColumn alter table with column name and type
func (dl *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

// FetchSchema queries delta lake and returns the schema associated with provided namespace
func (dl *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandle, err := dl.connectToWarehouse()
	if err != nil {
		return
	}
	defer dbHandle.Close()

	// Schema Initialization
	schema = make(warehouseutils.SchemaT)

	// Fetching the tables
	tableNames, err := dl.fetchTables(dbHandle, dl.Namespace)
	if err != nil {
		return
	}

	// Filtering tables based on not part of staging tables
	var filteredTablesNames []string
	for _, tableName := range tableNames {
		// Ignoring the staging tables
		if strings.HasPrefix(tableName, stagingTablePrefix) {
			continue
		}
		filteredTablesNames = append(filteredTablesNames, tableName)
	}

	fetchTablesAttributesExecTime := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "FetchTableAttributes",
	})
	fetchTablesAttributesExecTime.Start()
	defer fetchTablesAttributesExecTime.End()

	// For each table we are generating schema
	for _, tableName := range filteredTablesNames {
		fetchTableAttributesResponse, err := dbHandle.Client.FetchTableAttributes(dbHandle.Context, &proto.FetchTableAttributesRequest{
			Config:     dbHandle.CredConfig,
			Identifier: dbHandle.CredIdentifier,
			Schema:     dl.Namespace,
			Table:      tableName,
		})
		if err != nil {
			return schema, fmt.Errorf("%s Error while fetching table attributes: %v", dl.GetLogIdentifier(), err)
		}
		if !checkAndIgnoreAlreadyExistError(fetchTableAttributesResponse.GetErrorCode(), tableOrViewNotFound) {
			return schema, fmt.Errorf("%s Error while fetching table attributes with response: %v", dl.GetLogIdentifier(), fetchTableAttributesResponse.GetErrorMessage())
		}

		// Populating the schema for the table
		for _, item := range fetchTableAttributesResponse.GetAttributes() {
			if _, ok := schema[tableName]; !ok {
				schema[tableName] = make(map[string]string)
			}
			if datatype, ok := dataTypesMapToRudder[item.GetDataType()]; ok {
				schema[tableName][item.GetColName()] = datatype
			}
		}
	}
	return
}

// Setup populate the HandleT
func (dl *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Uploader = uploader
	dl.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.DELTALAKE, warehouse.Destination.Config, dl.Uploader.UseRudderStorage())

	dl.dbHandleT, err = dl.connectToWarehouse()
	return err
}

// TestConnection test the connection for the warehouse
func (dl *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	dl.Warehouse = warehouse
	dl.dbHandleT, err = dl.connectToWarehouse()
	return
}

// Cleanup handle cleanup when upload is done.
func (dl *HandleT) Cleanup() {
	if dl.dbHandleT != nil {
		dl.dropDanglingStagingTables()
		dl.dbHandleT.Close()
	}
}

// CrashRecover handle crash recover scenarios
func (dl *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.dbHandleT, err = dl.connectToWarehouse()
	if err != nil {
		return err
	}
	defer dl.dbHandleT.Close()
	dl.dropDanglingStagingTables()
	return
}

// IsEmpty checks if the warehouse is empty or not
func (dl *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

// LoadUserTables loads user tables
func (dl *HandleT) LoadUserTables() map[string]error {
	return dl.loadUserTables()
}

// LoadTable loads table for table name
func (dl *HandleT) LoadTable(tableName string) error {
	_, err := dl.loadTable(tableName, dl.Uploader.GetTableSchemaInUpload(tableName), dl.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
}

// LoadIdentityMergeRulesTable loads identifies merge rules tables
func (dl *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

// LoadIdentityMappingsTable loads identifies mappings table
func (dl *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

// DownloadIdentityRules download identity rules
func (dl *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

// GetTotalCountInTable returns total count in tables.
func (dl *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	fetchTotalCountExecTime := stats.NewTaggedStat("warehouse.deltalake.grpcExecTime", stats.TimerType, map[string]string{
		"destination": dl.Warehouse.Destination.ID,
		"destType":    dl.Warehouse.Type,
		"source":      dl.Warehouse.Source.ID,
		"namespace":   dl.Warehouse.Namespace,
		"identifier":  dl.Warehouse.Identifier,
		"queryType":   "FetchTotalCountInTable",
	})
	fetchTotalCountExecTime.Start()
	defer fetchTotalCountExecTime.End()

	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s.%[2]s;`, dl.Namespace, tableName)
	response, err := dl.dbHandleT.Client.FetchTotalCountInTable(dl.dbHandleT.Context, &proto.FetchTotalCountInTableRequest{
		Config:       dl.dbHandleT.CredConfig,
		Identifier:   dl.dbHandleT.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		err = fmt.Errorf("%s Error while fetching table count: %v", dl.GetLogIdentifier(), err)
		return
	}
	if !checkAndIgnoreAlreadyExistError(response.GetErrorCode(), tableOrViewNotFound) {
		err = fmt.Errorf("%s Error while fetching table count with response: %v", dl.GetLogIdentifier(), response.GetErrorMessage())
		return
	}
	total = response.GetCount()
	return
}

// Connect returns Client
func (dl *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dbHandleT, err := dl.connectToWarehouse()

	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.DBClient, DBHandleT: dbHandleT}, err
}

// GetLogIdentifier returns log identifier
func (dl *HandleT) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace, strings.Join(args, "]["))
}

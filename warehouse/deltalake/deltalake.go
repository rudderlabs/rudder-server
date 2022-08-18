package deltalake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/iancoleman/strcase"

	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	proto "github.com/rudderlabs/rudder-server/proto/databricks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake/databricks"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Database configuration
const (
	DLHost                 = "host"
	DLPort                 = "port"
	DLPath                 = "path"
	DLToken                = "token"
	AWSTokens              = "useSTSTokens"
	AWSAccessKey           = "accessKey"
	AWSAccessSecret        = "accessKeyID"
	EnableExternalLocation = "enableExternalLocation"
	ExternalLocation       = "externalLocation"
)

// Reference: https://docs.oracle.com/cd/E17952_01/connector-odbc-en/connector-odbc-reference-errorcodes.html
const (
	tableOrViewNotFound = "42S02"
	databaseNotFound    = "42000"
	partitionNotFound   = "42000"
)

var (
	stagingTablePrefix     string
	pkgLogger              logger.LoggerI
	schema                 string
	sparkServerType        string
	authMech               string
	uid                    string
	thriftTransport        string
	ssl                    string
	userAgent              string
	grpcTimeout            time.Duration
	healthTimeout          time.Duration
	loadTableStrategy      string
	enablePartitionPruning bool
)

// Rudder data type mapping with Delta lake mappings.
var dataTypesMap = map[string]string{
	"boolean":  "BOOLEAN",
	"int":      "BIGINT",
	"float":    "DOUBLE",
	"string":   "STRING",
	"datetime": "TIMESTAMP",
	"date":     "DATE",
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
	"DATE":      "date",
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
	"date":      "date",
	"timestamp": "datetime",
}

// excludeColumnsMap Columns you need to exclude
// Since event_date is an auto generated column in order to support partitioning.
// We need to ignore it during query generation.
var excludeColumnsMap = map[string]bool{
	"event_date": true,
}

// Primary Key mappings for tables
var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

type HandleT struct {
	dbHandleT      *databricks.DBHandleT
	Namespace      string
	ObjectStorage  string
	Warehouse      warehouseutils.WarehouseT
	Uploader       warehouseutils.UploaderI
	ConnectTimeout time.Duration
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
	config.RegisterDurationConfigVariable(2, &grpcTimeout, false, time.Minute, "Warehouse.deltalake.grpcTimeout")
	config.RegisterDurationConfigVariable(15, &healthTimeout, false, time.Second, "Warehouse.deltalake.healthTimeout")
	config.RegisterStringConfigVariable("MERGE", &loadTableStrategy, true, "Warehouse.deltalake.loadTableStrategy")
	config.RegisterBoolConfigVariable(true, &enablePartitionPruning, true, "Warehouse.deltalake.enablePartitionPruning")
}

// getDeltaLakeDataType returns datatype for delta lake which is mapped with rudder stack datatype
func getDeltaLakeDataType(columnType string) string {
	return dataTypesMap[columnType]
}

// ColumnsWithDataTypes returns columns with specified prefix and data type
func ColumnsWithDataTypes(columns map[string]string, prefix string) string {
	keys := warehouseutils.SortColumnKeysFromColumnMap(columns)
	format := func(idx int, name string) string {
		if _, ok := excludeColumnsMap[name]; ok {
			return ""
		}
		if name == "received_at" {
			generatedColumnSQL := "DATE GENERATED ALWAYS AS ( CAST(received_at AS DATE) )"
			return fmt.Sprintf(`%s%s %s, %s%s %s`, prefix, name, getDeltaLakeDataType(columns[name]), prefix, "event_date", generatedColumnSQL)
		}

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
func checkAndIgnoreAlreadyExistError(errorCode, ignoreError string) bool {
	if errorCode == "" || errorCode == ignoreError {
		return true
	}
	return false
}

// Connect creates database connection with CredentialsT
func Connect(cred *databricks.CredentialsT, connectTimeout time.Duration) (dbHandleT *databricks.DBHandleT, err error) {
	if err := checkHealth(); err != nil {
		return nil, fmt.Errorf("error connecting to databricks related deployement. Please contact Rudderstack support team")
	}

	ctx := context.Background()
	identifier := uuid.Must(uuid.NewV4()).String()
	connConfig := &proto.ConnectionConfig{
		Host:            cred.Host,
		Port:            cred.Port,
		HttpPath:        cred.Path,
		Pwd:             cred.Token,
		Schema:          schema,
		SparkServerType: sparkServerType,
		AuthMech:        authMech,
		Uid:             uid,
		ThriftTransport: thriftTransport,
		Ssl:             ssl,
		UserAgentEntry:  userAgent,
	}

	// Getting timeout context
	timeout := grpcTimeout
	if connectTimeout != 0 {
		timeout = connectTimeout
	}
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Creating grpc connection using timeout context
	conn, err := grpc.DialContext(tCtx, GetDatabricksConnectorURL(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err == context.DeadlineExceeded {
		execTimeouts := stats.DefaultStats.NewStat("warehouse.deltalake.grpcTimeouts", stats.CountType)
		execTimeouts.Count(1)

		err = fmt.Errorf("connection timed out to Delta lake: %w", err)
		return
	}
	if err != nil {
		err = fmt.Errorf("error while creating grpc connection to Delta lake: %w", err)
		return
	}

	dbClient := proto.NewDatabricksClient(conn)
	connectionResponse, err := dbClient.Connect(ctx, &proto.ConnectRequest{
		Config:     connConfig,
		Identifier: identifier,
	})
	if err != nil {
		err = fmt.Errorf("error connecting to Delta lake: %w", err)
		return
	}
	if connectionResponse.GetErrorCode() != "" {
		err = fmt.Errorf("error connecting to Delta lake with response: %s", connectionResponse.GetErrorMessage())
		return
	}

	dbHandleT = &databricks.DBHandleT{
		CredConfig:     connConfig,
		CredIdentifier: identifier,
		Conn:           conn,
		Client:         dbClient,
		Context:        ctx,
	}
	return
}

// fetchTables fetch tables with tableNames
func (dl *HandleT) fetchTables(dbT *databricks.DBHandleT, schema string) (tableNames []string, err error) {
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
		Config:     dbT.CredConfig,
		Identifier: dbT.CredIdentifier,
		Schema:     schema,
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

// fetchPartitionColumns return the partition columns for the corresponding tables
func (dl *HandleT) fetchPartitionColumns(dbT *databricks.DBHandleT, tableName string) ([]string, error) {
	sqlStatement := fmt.Sprintf(`SHOW PARTITIONS %s.%s`, dl.Warehouse.Namespace, tableName)

	columnsResponse, err := dbT.Client.FetchPartitionColumns(dbT.Context, &proto.FetchPartitionColumnsRequest{
		Config:       dbT.CredConfig,
		Identifier:   dbT.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch partition columns, error: %w", err)
	}
	if !checkAndIgnoreAlreadyExistError(columnsResponse.GetErrorCode(), partitionNotFound) {
		return nil, fmt.Errorf("failed to fetch partition for response, error: %v", columnsResponse.GetErrorMessage())
	}

	return columnsResponse.GetColumns(), nil
}

func isPartitionedByEventDate(partitionedColumns []string) bool {
	return misc.ContainsString(partitionedColumns, "event_date")
}

// partitionQuery
// Checks whether the table is partition with event_date column
// If specified, then calculates the date range from first and last event at and add it IN predicate query for event_date
// If not specified, them returns empty string
func (dl *HandleT) partitionQuery(tableName string) (string, error) {
	if !enablePartitionPruning {
		return "", nil
	}

	partitionColumns, err := dl.fetchPartitionColumns(dl.dbHandleT, tableName)
	if err != nil {
		return "", fmt.Errorf("failed to prepare partition query, error: %w", err)
	}

	if !isPartitionedByEventDate(partitionColumns) {
		return "", nil
	}

	firstEvent, lastEvent := dl.Uploader.GetFirstLastEvent()

	dateRange := warehouseutils.GetDateRangeList(firstEvent, lastEvent, "2006-01-02")
	if len(dateRange) == 0 {
		return "", nil
	}

	dateRangeString := warehouseutils.JoinWithFormatting(dateRange, func(idx int, str string) string {
		return fmt.Sprintf(`'%s'`, str)
	}, ",")
	query := fmt.Sprintf(`CAST ( MAIN.event_date AS string) IN (%s)`, dateRangeString)
	return query, nil
}

// ExecuteSQL executes sql using grpc Client
func (dl *HandleT) ExecuteSQL(sqlStatement, queryType string) (err error) {
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

	err = dl.ExecuteSQLClient(dl.dbHandleT, sqlStatement)
	return
}

// ExecuteSQLClient executes sql client using grpc Client
func (dl *HandleT) ExecuteSQLClient(dbClient *databricks.DBHandleT, sqlStatement string) (err error) {
	executeResponse, err := dbClient.Client.Execute(dbClient.Context, &proto.ExecuteRequest{
		Config:       dbClient.CredConfig,
		Identifier:   dbClient.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return fmt.Errorf("error while executing: %v", err)
	}
	if !checkAndIgnoreAlreadyExistError(executeResponse.GetErrorCode(), databaseNotFound) || !checkAndIgnoreAlreadyExistError(executeResponse.GetErrorCode(), tableOrViewNotFound) {
		err = fmt.Errorf("error while executing with response: %v", executeResponse.GetErrorMessage())
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
		if err != nil {
			pkgLogger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), err)
			continue
		}
		if !checkAndIgnoreAlreadyExistError(dropTableResponse.GetErrorCode(), tableOrViewNotFound) {
			pkgLogger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), dropTableResponse.GetErrorMessage())
		}
	}
}

// sortedColumnNames returns sorted column names
func (dl *HandleT) sortedColumnNames(tableSchemaInUpload warehouseutils.TableSchemaT, sortedColumnKeys []string, diff warehouseutils.TableSchemaDiffT) (sortedColumnNames string) {
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
		formatString := warehouseutils.JoinWithFormatting(sortedColumnKeys, format, ",")
		if len(diff.ColumnMap) > 0 {
			diffCols := make([]string, 0, len(diff.ColumnMap))
			for key := range diff.ColumnMap {
				diffCols = append(diffCols, key)
			}
			diffFormat := func(index int, value string) string {
				return fmt.Sprintf(`NULL AS %s`, value)
			}
			diffString := warehouseutils.JoinWithFormatting(diffCols, diffFormat, ",")
			return fmt.Sprintf("%s, %s", formatString, diffString)
		}
		return formatString
	}
	return
}

// credentialsStr return authentication for AWS STS and SSE-C encryption
// STS authentication is only supported with S3A client.
func (dl *HandleT) credentialsStr() (auth string, err error) {
	switch dl.ObjectStorage {
	case "S3":
		useSTSTokens := warehouseutils.GetConfigValueBoolString(AWSTokens, dl.Warehouse)
		if useSTSTokens == "true" {
			awsAccessKey := warehouseutils.GetConfigValue(AWSAccessKey, dl.Warehouse)
			awsSecretKey := warehouseutils.GetConfigValue(AWSAccessSecret, dl.Warehouse)
			if awsAccessKey != "" && awsSecretKey != "" {
				var tempAccessKeyId, tempSecretAccessKey, token string
				tempAccessKeyId, tempSecretAccessKey, token, err = warehouseutils.GetTemporaryS3Cred(awsSecretKey, awsAccessKey)
				if err != nil {
					return
				}
				auth = fmt.Sprintf(`CREDENTIALS ( 'awsKeyId' = '%s', 'awsSecretKey' = '%s', 'awsSessionToken' = '%s' )`, tempAccessKeyId, tempSecretAccessKey, token)
			}
		}
	}
	return
}

// getLoadFolder return the load folder where the load files are present
func (dl *HandleT) getLoadFolder(location string) (loadFolder string, err error) {
	loadFolder = warehouseutils.GetObjectFolderForDeltalake(dl.ObjectStorage, location)
	if dl.ObjectStorage == "S3" {
		awsAccessKey := warehouseutils.GetConfigValue(AWSAccessKey, dl.Warehouse)
		awsSecretKey := warehouseutils.GetConfigValue(AWSAccessSecret, dl.Warehouse)
		if awsAccessKey != "" && awsSecretKey != "" {
			loadFolder = strings.Replace(loadFolder, "s3://", "s3a://", 1)
		}
	}
	return
}

func getTableSchemaDiff(tableSchemaInUpload, tableSchemaAfterUpload warehouseutils.TableSchemaT) (diff warehouseutils.TableSchemaDiffT) {
	diff = warehouseutils.TableSchemaDiffT{
		ColumnMap: make(map[string]string),
	}
	diff.ColumnMap = make(map[string]string)
	for columnName, columnType := range tableSchemaAfterUpload {
		if _, ok := tableSchemaInUpload[columnName]; !ok {
			diff.ColumnMap[columnName] = columnType
		}
	}
	return diff
}

// loadTable Loads table with table name
func (dl *HandleT) loadTable(tableName string, tableSchemaInUpload, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
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
	auth, err := dl.credentialsStr()
	if err != nil {
		return
	}

	// Getting objects location
	objectsLocation, err := dl.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		return
	}

	loadFolder, err := dl.getLoadFolder(objectsLocation)
	if err != nil {
		return
	}

	// Creating copy sql statement to copy from load folder to the staging table
	tableSchemaDiff := getTableSchemaDiff(tableSchemaInUpload, tableSchemaAfterUpload)
	sortedColumnNames := dl.sortedColumnNames(tableSchemaInUpload, sortedColumnKeys, tableSchemaDiff)
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
			"FORMAT_OPTIONS ( 'compression' = 'gzip', 'quote' = '\"', 'escape' = '\"', 'multiLine' = 'true' ) "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s;",
			fmt.Sprintf(`%s.%s`, dl.Namespace, stagingTableName), sortedColumnNames, loadFolder, auth)
	}

	// Sanitising copy sql statement for logging
	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"'awsKeyId' = '[^']*'":        "'awsKeyId' = '***'",
		"'awsSecretKey' = '[^']*'":    "'awsSecretKey' = '***'",
		"'awsSessionToken' = '[^']*'": "'awsSessionToken' = '***'",
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

	if loadTableStrategy == "APPEND" {
		sqlStatement = appendableLTSQLStatement(
			dl.Namespace,
			tableName,
			stagingTableName,
			warehouseutils.SortColumnKeysFromColumnMap(tableSchemaAfterUpload),
		)
	} else {
		// Partition query
		var partitionQuery string
		partitionQuery, err = dl.partitionQuery(tableName)
		if err != nil {
			err = fmt.Errorf("failed getting partition query during load table, error: %w", err)
			return
		}

		sqlStatement = mergeableLTSQLStatement(
			dl.Namespace,
			tableName,
			stagingTableName,
			sortedColumnKeys,
			partitionQuery,
		)
	}
	pkgLogger.Infof("%v Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(tableName), sqlStatement)

	// Executing load table sql statement
	err = dl.ExecuteSQL(sqlStatement, fmt.Sprintf("LT::%s", strcase.ToCamel(loadTableStrategy)))
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
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), warehouseutils.UsersTable), 127)

	tableLocationSql := dl.getTableLocationSql(stagingTableName)

	// Creating create table sql statement for staging users table
	sqlStatement := fmt.Sprintf(`CREATE TABLE %[1]s.%[2]s USING DELTA %[7]s AS (SELECT DISTINCT * FROM
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
		columnNames(userColNames),
		tableLocationSql,
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

	// Creating the column Keys
	columnKeys := append([]string{`id`}, userColNames...)

	if loadTableStrategy == "APPEND" {
		sqlStatement = appendableLTSQLStatement(
			dl.Namespace,
			warehouseutils.UsersTable,
			stagingTableName,
			columnKeys,
		)
	} else {
		// Partition query
		// Partition query
		var partitionQuery string
		partitionQuery, err = dl.partitionQuery(warehouseutils.UsersTable)
		if err != nil {
			err = fmt.Errorf("failed getting partition query during load users table, error: %w", err)
			return
		}

		sqlStatement = mergeableLTSQLStatement(
			dl.Namespace,
			warehouseutils.UsersTable,
			stagingTableName,
			columnKeys,
			partitionQuery,
		)
	}
	pkgLogger.Infof("%s Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(warehouseutils.UsersTable), sqlStatement)

	// Executing the load users table sql statement
	err = dl.ExecuteSQL(sqlStatement, fmt.Sprintf("LUT::%s", strcase.ToCamel(loadTableStrategy)))
	if err != nil {
		pkgLogger.Errorf("%s Error inserting into users table from staging table: %v\n", err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

// getExternalLocation returns external location where we need to create the tables
func (dl *HandleT) getExternalLocation() (externalLocation string) {
	enableExternalLocation := warehouseutils.GetConfigValueBoolString(EnableExternalLocation, dl.Warehouse)
	if enableExternalLocation == "true" {
		externalLocation := warehouseutils.GetConfigValue(ExternalLocation, dl.Warehouse)
		return externalLocation
	}
	return
}

// getTableLocationSql returns external external table location
func (dl *HandleT) getTableLocationSql(tableName string) (tableLocation string) {
	externalLocation := dl.getExternalLocation()
	if externalLocation == "" {
		return
	}
	return fmt.Sprintf("LOCATION '%s/%s/%s'", externalLocation, dl.Namespace, tableName)
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
}

// connectToWarehouse returns the database connection configured with CredentialsT
func (dl *HandleT) connectToWarehouse() (dbHandleT *databricks.DBHandleT, err error) {
	credT := &databricks.CredentialsT{
		Host:  warehouseutils.GetConfigValue(DLHost, dl.Warehouse),
		Port:  warehouseutils.GetConfigValue(DLPort, dl.Warehouse),
		Path:  warehouseutils.GetConfigValue(DLPath, dl.Warehouse),
		Token: warehouseutils.GetConfigValue(DLToken, dl.Warehouse),
	}
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

	dbHandleT, err = Connect(credT, dl.ConnectTimeout)
	if err != nil {
		return
	}

	dbHandleT.CloseStats = closeConnStat
	return
}

// CreateTable creates tables with table name and columns
func (dl *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`%s.%s`, dl.Namespace, tableName)

	tableLocationSql := dl.getTableLocationSql(tableName)
	var partitionedSql string
	if _, ok := columns["received_at"]; ok {
		partitionedSql = `PARTITIONED BY(event_date)`
	}

	createTableClauseSql := "CREATE TABLE IF NOT EXISTS"
	if tableLocationSql != "" {
		createTableClauseSql = "CREATE OR REPLACE TABLE"
	}

	sqlStatement := fmt.Sprintf(`%s %s ( %v ) USING DELTA %s %s;`, createTableClauseSql, name, ColumnsWithDataTypes(columns, ""), tableLocationSql, partitionedSql)
	pkgLogger.Infof("%s Creating table in delta lake with SQL: %v", dl.GetLogIdentifier(tableName), sqlStatement)
	err = dl.ExecuteSQL(sqlStatement, "CreateTable")
	return
}

func (dl *HandleT) DropTable(tableName string) (err error) {
	pkgLogger.Infof("%s Dropping table %s", dl.GetLogIdentifier(), tableName)
	sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s.%[2]s;`, dl.Namespace, tableName)
	dropTableResponse, err := dl.dbHandleT.Client.Execute(dl.dbHandleT.Context, &proto.ExecuteRequest{
		Config:       dl.dbHandleT.CredConfig,
		Identifier:   dl.dbHandleT.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return
	}
	if !checkAndIgnoreAlreadyExistError(dropTableResponse.GetErrorCode(), tableOrViewNotFound) {
		err = fmt.Errorf("%s Error while droping table with response: %v", dl.GetLogIdentifier(), dropTableResponse.GetErrorMessage())
		return
	}
	return
}

// AddColumn adds column for column name and type
func (dl *HandleT) AddColumn(name, columnName, columnType string) (err error) {
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
func (dl *HandleT) AlterColumn(_, _, _ string) (err error) {
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
			if _, ok := excludeColumnsMap[item.GetColName()]; ok {
				continue
			}

			if _, ok := schema[tableName]; !ok {
				schema[tableName] = make(map[string]string)
			}
			if datatype, ok := dataTypesMapToRudder[item.GetDataType()]; ok {
				schema[tableName][item.GetColName()] = datatype
			} else {
				warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &dl.Warehouse, warehouseutils.Tag{Name: "datatype", Value: item.GetDataType()}).Count(1)
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
func (dl *HandleT) IsEmpty(warehouseutils.WarehouseT) (empty bool, err error) {
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
	dl.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.DELTALAKE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(dl.Warehouse.Destination.Config),
	)
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

// GetDatabricksVersion Gets the databricks version by making a grpc call to Version stub.
func GetDatabricksVersion() (databricksBuildVersion string) {
	databricksBuildVersion = "Not an official release. Get the latest release from dockerhub."

	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, GetDatabricksConnectorURL(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		pkgLogger.Errorf("Error while creating grpc connection to databricks with error: %s", err.Error())
		databricksBuildVersion = "Unable to create grpc connection to databricks."
		return
	}

	versionClient := proto.NewVersionClient(conn)
	versionResponse, err := versionClient.GetVersion(ctx, &proto.VersionRequest{})
	if err != nil {
		pkgLogger.Errorf("Error while getting version response from databricks with error : %s", err.Error())
		databricksBuildVersion = "Unable to read response from databricks."
		return
	}
	databricksBuildVersion = versionResponse.GetVersion()
	return
}

// GetDatabricksVersion Gets the databricks version by making a grpc call to Version stub.
func checkHealth() (err error) {
	ctx := context.Background()
	defer func() {
		if err != nil {
			healthTimeouts := stats.DefaultStats.NewStat("warehouse.deltalake.healthTimeouts", stats.CountType)
			healthTimeouts.Count(1)
		}
	}()

	// Getting health timeout context
	tCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	// Creating grpc connection using timeout context
	conn, err := grpc.DialContext(tCtx, GetDatabricksConnectorURL(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return
	}

	healthClient := grpc_health_v1.NewHealthClient(conn)
	healthResponse, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: "",
	})
	if err != nil {
		return
	}
	if healthResponse.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		err = fmt.Errorf("databricks Service is not up")
	}
	return
}

func (dl *HandleT) LoadTestTable(location, tableName string, _ map[string]interface{}, format string) (err error) {
	// Get the credentials string to copy from the staging location to table
	auth, err := dl.credentialsStr()
	if err != nil {
		return
	}

	loadFolder, err := dl.getLoadFolder(location)
	if err != nil {
		return
	}

	var sqlStatement string
	if format == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = PARQUET "+
			"PATTERN = '*.parquet' "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s;",
			fmt.Sprintf(`%s.%s`, dl.Namespace, tableName),
			fmt.Sprintf(`%s, %s`, "id", "val"),
			loadFolder,
			auth,
		)
	} else {
		sqlStatement = fmt.Sprintf("COPY INTO %v FROM ( SELECT %v FROM '%v' ) "+
			"FILEFORMAT = CSV "+
			"PATTERN = '*.gz' "+
			"FORMAT_OPTIONS ( 'compression' = 'gzip', 'quote' = '\"', 'escape' = '\"', 'multiLine' = 'true' ) "+
			"COPY_OPTIONS ('force' = 'true') "+
			"%s;",
			fmt.Sprintf(`%s.%s`, dl.Namespace, tableName),
			"CAST ( '_c0' AS BIGINT ) AS id, CAST ( '_c1' AS STRING ) AS val",
			loadFolder,
			auth,
		)
	}

	err = dl.ExecuteSQLClient(dl.dbHandleT, sqlStatement)
	return
}

func (dl *HandleT) SetConnectionTimeout(timeout time.Duration) {
	dl.ConnectTimeout = timeout
}

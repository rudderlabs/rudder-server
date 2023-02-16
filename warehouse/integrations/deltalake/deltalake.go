package deltalake

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake/client"

	"github.com/rudderlabs/rudder-server/config"
	proto "github.com/rudderlabs/rudder-server/proto/databricks"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Database configuration
const (
	Host                   = "host"
	Port                   = "port"
	Path                   = "path"
	Token                  = "token"
	Catalog                = "catalog"
	UseSTSTokens           = "useSTSTokens"
	EnableExternalLocation = "enableExternalLocation"
	ExternalLocation       = "externalLocation"
)

const (
	provider       = warehouseutils.DELTALAKE
	tableNameLimit = 127
)

// Reference: https://docs.oracle.com/cd/E17952_01/connector-odbc-en/connector-odbc-reference-errorcodes.html
const (
	tableOrViewNotFound = "42S02"
	databaseNotFound    = "42000"
	partitionNotFound   = "42000"
)

var pkgLogger logger.Logger

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

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`UnauthorizedAccessException: PERMISSION_DENIED: User does not have READ FILES on External Location`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`SecurityException: User does not have permission CREATE on CATALOG`),
	},
}

type Deltalake struct {
	Client                 *client.Client
	Namespace              string
	ObjectStorage          string
	Warehouse              warehouseutils.Warehouse
	Uploader               warehouseutils.UploaderI
	ConnectTimeout         time.Duration
	Logger                 logger.Logger
	Stats                  stats.Stats
	Schema                 string
	SparkServerType        string
	AuthMech               string
	UID                    string
	ThriftTransport        string
	SSL                    string
	UserAgent              string
	GrpcTimeout            time.Duration
	HealthTimeout          time.Duration
	LoadTableStrategy      string
	EnablePartitionPruning bool
	ConnectorURL           string
}

// Init initializes the delta lake warehouse
func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("deltalake")
}

func NewDeltalake() *Deltalake {
	return &Deltalake{
		Logger: pkgLogger,
		Stats:  stats.Default,
	}
}

func WithConfig(h *Deltalake, config *config.Config) {
	h.Schema = config.GetString("Warehouse.deltalake.schema", "default")
	h.SparkServerType = config.GetString("Warehouse.deltalake.sparkServerType", "3")
	h.AuthMech = config.GetString("Warehouse.deltalake.authMech", "3")
	h.UID = config.GetString("Warehouse.deltalake.uid", "token")
	h.ThriftTransport = config.GetString("Warehouse.deltalake.thriftTransport", "2")
	h.SSL = config.GetString("Warehouse.deltalake.ssl", "1")
	h.UserAgent = config.GetString("Warehouse.deltalake.userAgent", "RudderStack")
	h.GrpcTimeout = config.GetDuration("Warehouse.deltalake.grpcTimeout", 2, time.Minute)
	h.HealthTimeout = config.GetDuration("Warehouse.deltalake.healthTimeout", 15, time.Second)
	h.LoadTableStrategy = config.GetString("Warehouse.deltalake.loadTableStrategy", "MERGE")
	h.EnablePartitionPruning = config.GetBool("Warehouse.deltalake.enablePartitionPruning", true)
	h.ConnectorURL = config.GetString("DATABRICKS_CONNECTOR_URL", "localhost:50051")
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
	return strings.Join(keys, ",")
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

// checkAndIgnoreAlreadyExistError checks and ignores native errors.
func checkAndIgnoreAlreadyExistError(errorCode, ignoreError string) bool {
	if errorCode == "" || errorCode == ignoreError {
		return true
	}
	return false
}

// NewClient creates deltalake client
func (dl *Deltalake) NewClient(cred *client.Credentials, connectTimeout time.Duration) (Client *client.Client, err error) {
	ctx := context.Background()
	identifier := misc.FastUUID().String()
	connConfig := &proto.ConnectionConfig{
		Host:            cred.Host,
		Port:            cred.Port,
		HttpPath:        cred.Path,
		Pwd:             cred.Token,
		Schema:          dl.Schema,
		SparkServerType: dl.SparkServerType,
		AuthMech:        dl.AuthMech,
		Uid:             dl.UID,
		ThriftTransport: dl.ThriftTransport,
		Ssl:             dl.SSL,
		UserAgentEntry:  dl.UserAgent,
	}

	// Getting timeout context
	timeout := dl.GrpcTimeout
	if connectTimeout != 0 {
		timeout = connectTimeout
	}
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Creating grpc connection using timeout context
	conn, err := grpc.DialContext(tCtx, dl.ConnectorURL, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err == context.DeadlineExceeded {
		execTimeouts := dl.Stats.NewStat("warehouse.deltalake.grpcTimeouts", stats.CountType)
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

	Client = &client.Client{
		Logger:         dl.Logger,
		CredConfig:     connConfig,
		CredIdentifier: identifier,
		Conn:           conn,
		Client:         dbClient,
		Context:        ctx,
	}

	// Setting up catalog at the client level
	if catalog := warehouseutils.GetConfigValue(Catalog, dl.Warehouse); catalog != "" {
		sqlStatement := fmt.Sprintf("USE CATALOG `%s`;", catalog)

		if err = dl.ExecuteSQLClient(Client, sqlStatement); err != nil {
			return
		}
	}
	return
}

func (*Deltalake) DeleteBy([]string, warehouseutils.DeleteByParams) error {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

// fetchTables fetch tables with tableNames
func (dl *Deltalake) fetchTables(dbT *client.Client, schema string) (tableNames []string, err error) {
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
func (dl *Deltalake) fetchPartitionColumns(dbT *client.Client, tableName string) ([]string, error) {
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
	return misc.Contains(partitionedColumns, "event_date")
}

// partitionQuery
// Checks whether the table is partition with event_date column
// If specified, then calculates the date range from first and last event at and add it IN predicate query for event_date
// If not specified, them returns empty string
func (dl *Deltalake) partitionQuery(tableName string) (string, error) {
	if !dl.EnablePartitionPruning {
		return "", nil
	}

	partitionColumns, err := dl.fetchPartitionColumns(dl.Client, tableName)
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

// ExecuteSQLClient executes sql client using grpc Client
func (*Deltalake) ExecuteSQLClient(client *client.Client, sqlStatement string) (err error) {
	executeResponse, err := client.Client.Execute(client.Context, &proto.ExecuteRequest{
		Config:       client.CredConfig,
		Identifier:   client.CredIdentifier,
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
func (dl *Deltalake) schemaExists(schemaName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SHOW SCHEMAS LIKE '%s';`, schemaName)
	fetchSchemasResponse, err := dl.Client.Client.FetchSchemas(dl.Client.Context, &proto.FetchSchemasRequest{
		Config:       dl.Client.CredConfig,
		Identifier:   dl.Client.CredIdentifier,
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
func (dl *Deltalake) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, dl.Namespace)
	dl.Logger.Infof("%s Creating schema in delta lake with SQL:%v", dl.GetLogIdentifier(), sqlStatement)
	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	return
}

// dropStagingTables drops staging tables
func (dl *Deltalake) dropStagingTables(tableNames []string) {
	for _, stagingTableName := range tableNames {
		dl.Logger.Infof("%s Dropping table %+v\n", dl.GetLogIdentifier(), stagingTableName)
		sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s.%[2]s;`, dl.Namespace, stagingTableName)
		dropTableResponse, err := dl.Client.Client.Execute(dl.Client.Context, &proto.ExecuteRequest{
			Config:       dl.Client.CredConfig,
			Identifier:   dl.Client.CredIdentifier,
			SqlStatement: sqlStatement,
		})
		if err != nil {
			dl.Logger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), err)
			continue
		}
		if !checkAndIgnoreAlreadyExistError(dropTableResponse.GetErrorCode(), tableOrViewNotFound) {
			dl.Logger.Errorf("%s Error dropping staging tables in delta lake: %v", dl.GetLogIdentifier(), dropTableResponse.GetErrorMessage())
		}
	}
}

// sortedColumnNames returns sorted column names
func (dl *Deltalake) sortedColumnNames(tableSchemaInUpload warehouseutils.TableSchemaT, sortedColumnKeys []string, diff warehouseutils.TableSchemaDiffT) (sortedColumnNames string) {
	if dl.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		sortedColumnNames = strings.Join(sortedColumnKeys, ",")
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
func (dl *Deltalake) credentialsStr() (string, error) {
	if dl.ObjectStorage == warehouseutils.S3 {
		canUseRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(dl.Warehouse.Destination.Config)
		canUseSTSTokens := warehouseutils.GetConfigValueBoolString(UseSTSTokens, dl.Warehouse) == "true"
		if canUseRudderStorage || canUseSTSTokens {
			tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&dl.Warehouse.Destination)
			if err != nil {
				return "", fmt.Errorf("temporary s3 credentials: %w", err)
			}
			auth := fmt.Sprintf(`CREDENTIALS ( 'awsKeyId' = '%s', 'awsSecretKey' = '%s', 'awsSessionToken' = '%s' )`, tempAccessKeyId, tempSecretAccessKey, token)
			return auth, nil
		}
	}
	return "", nil
}

// getLoadFolder return the load folder where the load files are present
func (dl *Deltalake) getLoadFolder(location string) (loadFolder string) {
	loadFolder = warehouseutils.GetObjectFolderForDeltalake(dl.ObjectStorage, location)
	if dl.ObjectStorage == warehouseutils.S3 {
		awsAccessKey := warehouseutils.GetConfigValue(warehouseutils.AWSAccessKey, dl.Warehouse)
		awsSecretKey := warehouseutils.GetConfigValue(warehouseutils.AWSAccessSecret, dl.Warehouse)
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
func (dl *Deltalake) loadTable(tableName string, tableSchemaInUpload, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	// Getting sorted column keys from tableSchemaInUpload
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

	// Creating staging table
	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
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

	loadFolder := dl.getLoadFolder(objectsLocation)

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
		dl.Logger.Infof("%s Running COPY command with SQL: %s\n", dl.GetLogIdentifier(tableName), sanitisedSQLStmt)
	}

	// Executing copy sql statement
	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	if err != nil {
		dl.Logger.Errorf("%s Error running COPY command with SQL: %s\n error: %v", dl.GetLogIdentifier(tableName), sqlStatement, err)
		return
	}

	if dl.LoadTableStrategy == "APPEND" {
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
	dl.Logger.Infof("%v Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(tableName), sqlStatement)

	// Executing load table sql statement
	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	if err != nil {
		dl.Logger.Errorf("%v Error inserting into original table: %v\n", dl.GetLogIdentifier(tableName), err)
		return
	}

	dl.Logger.Infof("%v Complete load for table\n", dl.GetLogIdentifier(tableName))
	return
}

// loadUserTables Loads users table
func (dl *Deltalake) loadUserTables() (errorMap map[string]error) {
	// Creating errorMap
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	dl.Logger.Infof("%s Starting load for identifies and users tables\n", dl.GetLogIdentifier())

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
	stagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)

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
	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	if err != nil {
		dl.Logger.Errorf("%s Creating staging table for users failed with SQL: %s\n", dl.GetLogIdentifier(), sqlStatement)
		dl.Logger.Errorf("%s Error creating users staging table from original table and identifies staging table: %v\n", dl.GetLogIdentifier(), err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// Dropping staging users table
	defer dl.dropStagingTables([]string{stagingTableName})

	// Creating the column Keys
	columnKeys := append([]string{`id`}, userColNames...)

	if dl.LoadTableStrategy == "APPEND" {
		sqlStatement = appendableLTSQLStatement(
			dl.Namespace,
			warehouseutils.UsersTable,
			stagingTableName,
			columnKeys,
		)
	} else {
		// Partition query
		var partitionQuery string
		partitionQuery, err = dl.partitionQuery(warehouseutils.UsersTable)
		if err != nil {
			err = fmt.Errorf("failed getting partition query during load users table, error: %w", err)
			errorMap[warehouseutils.UsersTable] = err
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
	dl.Logger.Infof("%s Inserting records using staging table with SQL: %s\n", dl.GetLogIdentifier(warehouseutils.UsersTable), sqlStatement)

	// Executing the load users table sql statement
	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	if err != nil {
		dl.Logger.Errorf("%s Error inserting into users table from staging table: %v\n", err)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

// getExternalLocation returns external location where we need to create the tables
func (dl *Deltalake) getExternalLocation() (externalLocation string) {
	enableExternalLocation := warehouseutils.GetConfigValueBoolString(EnableExternalLocation, dl.Warehouse)
	if enableExternalLocation == "true" {
		externalLocation := warehouseutils.GetConfigValue(ExternalLocation, dl.Warehouse)
		return externalLocation
	}
	return
}

// getTableLocationSql returns external external table location
func (dl *Deltalake) getTableLocationSql(tableName string) (tableLocation string) {
	externalLocation := dl.getExternalLocation()
	if externalLocation == "" {
		return
	}
	return fmt.Sprintf("LOCATION '%s/%s/%s'", externalLocation, dl.Namespace, tableName)
}

// dropDanglingStagingTables drop dandling staging tables.
func (dl *Deltalake) dropDanglingStagingTables() {
	// Fetching the staging tables
	tableNames, err := dl.fetchTables(dl.Client, dl.Namespace)
	if err != nil {
		return
	}

	// Filtering tables based on not part of staging tables
	var filteredTablesNames []string
	for _, tableName := range tableNames {
		// Ignoring the staging tables
		if !strings.HasPrefix(tableName, warehouseutils.StagingTablePrefix(provider)) {
			continue
		}
		filteredTablesNames = append(filteredTablesNames, tableName)
	}

	// Drop staging tables
	dl.dropStagingTables(filteredTablesNames)
}

// connectToWarehouse returns the database connection configured with Credentials
func (dl *Deltalake) connectToWarehouse() (Client *client.Client, err error) {
	credT := &client.Credentials{
		Host:  warehouseutils.GetConfigValue(Host, dl.Warehouse),
		Port:  warehouseutils.GetConfigValue(Port, dl.Warehouse),
		Path:  warehouseutils.GetConfigValue(Path, dl.Warehouse),
		Token: warehouseutils.GetConfigValue(Token, dl.Warehouse),
	}
	return dl.NewClient(credT, dl.ConnectTimeout)
}

// CreateTable creates tables with table name and columns
func (dl *Deltalake) CreateTable(tableName string, columns map[string]string) (err error) {
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
	dl.Logger.Infof("%s Creating table in delta lake with SQL: %v", dl.GetLogIdentifier(tableName), sqlStatement)
	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	return
}

func (dl *Deltalake) DropTable(tableName string) (err error) {
	dl.Logger.Infof("%s Dropping table %s", dl.GetLogIdentifier(), tableName)
	sqlStatement := fmt.Sprintf(`DROP TABLE %[1]s.%[2]s;`, dl.Namespace, tableName)
	dropTableResponse, err := dl.Client.Client.Execute(dl.Client.Context, &proto.ExecuteRequest{
		Config:       dl.Client.CredConfig,
		Identifier:   dl.Client.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return
	}
	if !checkAndIgnoreAlreadyExistError(dropTableResponse.GetErrorCode(), tableOrViewNotFound) {
		err = fmt.Errorf("%s Error while dropping table with response: %v", dl.GetLogIdentifier(), dropTableResponse.GetErrorMessage())
		return
	}
	return
}

func (dl *Deltalake) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s
		ADD COLUMNS(`,
		dl.Namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` %s %s,`, columnInfo.Name, getDeltaLakeDataType(columnInfo.Type)))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ");"

	dl.Logger.Infof("DL: Adding columns for destinationID: %s, tableName: %s with query: %v", dl.Warehouse.Destination.ID, tableName, query)
	err = dl.ExecuteSQLClient(dl.Client, query)
	return
}

// CreateSchema checks if schema exists or not. If it does not exist, it creates the schema.
func (dl *Deltalake) CreateSchema() (err error) {
	// Checking if schema exists or not
	var schemaExists bool
	schemaExists, err = dl.schemaExists(dl.Namespace)
	if err != nil {
		dl.Logger.Errorf("%s Error checking if schema exists: %s, error: %v", dl.GetLogIdentifier(), dl.Namespace, err)
		return err
	}
	if schemaExists {
		dl.Logger.Infof("%s Skipping creating schema: %s since it already exists", dl.GetLogIdentifier(), dl.Namespace)
		return
	}

	// Creating schema
	return dl.createSchema()
}

// AlterColumn alter table with column name and type
func (*Deltalake) AlterColumn(_, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// FetchSchema queries delta lake and returns the schema associated with provided namespace
func (dl *Deltalake) FetchSchema(warehouse warehouseutils.Warehouse) (schema, unrecognizedSchema warehouseutils.SchemaT, err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	Client, err := dl.connectToWarehouse()
	if err != nil {
		return
	}
	defer Client.Close()

	// Schema Initialization
	schema = make(warehouseutils.SchemaT)
	unrecognizedSchema = make(warehouseutils.SchemaT)

	// Fetching the tables
	tableNames, err := dl.fetchTables(Client, dl.Namespace)
	if err != nil {
		return
	}

	// Filtering tables based on not part of staging tables
	var filteredTablesNames []string
	for _, tableName := range tableNames {
		// Ignoring the staging tables
		if strings.HasPrefix(tableName, warehouseutils.StagingTablePrefix(provider)) {
			continue
		}
		filteredTablesNames = append(filteredTablesNames, tableName)
	}

	// For each table we are generating schema
	for _, tableName := range filteredTablesNames {
		fetchTableAttributesResponse, err := Client.Client.FetchTableAttributes(Client.Context, &proto.FetchTableAttributesRequest{
			Config:     Client.CredConfig,
			Identifier: Client.CredIdentifier,
			Schema:     dl.Namespace,
			Table:      tableName,
		})
		if err != nil {
			return schema, unrecognizedSchema, fmt.Errorf("%s Error while fetching table attributes: %v", dl.GetLogIdentifier(), err)
		}
		if !checkAndIgnoreAlreadyExistError(fetchTableAttributesResponse.GetErrorCode(), tableOrViewNotFound) {
			return schema, unrecognizedSchema, fmt.Errorf("%s Error while fetching table attributes with response: %v", dl.GetLogIdentifier(), fetchTableAttributesResponse.GetErrorMessage())
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
				if _, ok := unrecognizedSchema[tableName]; !ok {
					unrecognizedSchema[tableName] = make(map[string]string)
				}
				unrecognizedSchema[tableName][item.GetColName()] = warehouseutils.MISSING_DATATYPE

				warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &dl.Warehouse, warehouseutils.Tag{Name: "datatype", Value: item.GetDataType()}).Count(1)
			}
		}
	}
	return
}

// Setup populate the Deltalake
func (dl *Deltalake) Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Uploader = uploader
	dl.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.DELTALAKE, warehouse.Destination.Config, dl.Uploader.UseRudderStorage())

	dl.Client, err = dl.connectToWarehouse()
	return err
}

// TestConnection test the connection for the warehouse
func (dl *Deltalake) TestConnection(warehouse warehouseutils.Warehouse) (err error) {
	dl.Warehouse = warehouse
	dl.Client, err = dl.connectToWarehouse()
	return
}

// Cleanup cleanup when upload is done.
func (dl *Deltalake) Cleanup() {
	if dl.Client != nil {
		dl.dropDanglingStagingTables()
		dl.Client.Close()
	}
}

// CrashRecover crash recover scenarios
func (dl *Deltalake) CrashRecover(warehouse warehouseutils.Warehouse) (err error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.Client, err = dl.connectToWarehouse()
	if err != nil {
		return err
	}
	defer dl.Client.Close()
	dl.dropDanglingStagingTables()
	return
}

// IsEmpty checks if the warehouse is empty or not
func (*Deltalake) IsEmpty(warehouseutils.Warehouse) (empty bool, err error) {
	return
}

// LoadUserTables loads user tables
func (dl *Deltalake) LoadUserTables() map[string]error {
	return dl.loadUserTables()
}

// LoadTable loads table for table name
func (dl *Deltalake) LoadTable(tableName string) error {
	_, err := dl.loadTable(tableName, dl.Uploader.GetTableSchemaInUpload(tableName), dl.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
}

// LoadIdentityMergeRulesTable loads identifies merge rules tables
func (*Deltalake) LoadIdentityMergeRulesTable() (err error) {
	return
}

// LoadIdentityMappingsTable loads identifies mappings table
func (*Deltalake) LoadIdentityMappingsTable() (err error) {
	return
}

// DownloadIdentityRules download identity rules
func (*Deltalake) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

// GetTotalCountInTable returns the total count in the table
func (dl *Deltalake) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT COUNT(*) FROM %[1]s.%[2]s;
	`,
		dl.Namespace,
		tableName,
	)
	response, err := dl.Client.Client.FetchTotalCountInTable(ctx, &proto.FetchTotalCountInTableRequest{
		Config:       dl.Client.CredConfig,
		Identifier:   dl.Client.CredIdentifier,
		SqlStatement: sqlStatement,
	})
	if err != nil {
		return 0, fmt.Errorf("fetching table count: %w", err)
	}
	if response.GetErrorCode() != "" {
		return 0, fmt.Errorf("fetching table count: %s", response.GetErrorMessage())
	}
	return response.GetCount(), nil
}

// Connect returns Client
func (dl *Deltalake) Connect(warehouse warehouseutils.Warehouse) (warehouseclient.Client, error) {
	dl.Warehouse = warehouse
	dl.Namespace = warehouse.Namespace
	dl.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.DELTALAKE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(dl.Warehouse.Destination.Config),
	)
	Client, err := dl.connectToWarehouse()
	if err != nil {
		return warehouseclient.Client{}, err
	}

	return warehouseclient.Client{Type: warehouseclient.DeltalakeClient, DeltalakeClient: Client}, err
}

// GetLogIdentifier returns log identifier
func (dl *Deltalake) GetLogIdentifier(args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("[%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace)
	}
	return fmt.Sprintf("[%s][%s][%s][%s][%s]", dl.Warehouse.Type, dl.Warehouse.Source.ID, dl.Warehouse.Destination.ID, dl.Warehouse.Namespace, strings.Join(args, "]["))
}

// GetDatabricksVersion Gets the databricks version by making a grpc call to Version stub.
func GetDatabricksVersion() (databricksBuildVersion string) {
	databricksBuildVersion = "Not an official release. Get the latest release from dockerhub."

	ctx := context.Background()
	connectorURL := config.GetString("DATABRICKS_CONNECTOR_URL", "localhost:50051")

	conn, err := grpc.DialContext(ctx, connectorURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func (dl *Deltalake) LoadTestTable(location, tableName string, _ map[string]interface{}, format string) (err error) {
	// Get the credentials string to copy from the staging location to table
	auth, err := dl.credentialsStr()
	if err != nil {
		return
	}

	loadFolder := dl.getLoadFolder(location)

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

	err = dl.ExecuteSQLClient(dl.Client, sqlStatement)
	return
}

func (dl *Deltalake) SetConnectionTimeout(timeout time.Duration) {
	dl.ConnectTimeout = timeout
}

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
		sqlStatement = fmt.Sprintf(`
			SELECT
			  %[3]s
			FROM
			  %[1]s.%[2]s
		`,
			namespace,
			stagingTableName,
			columnNames(columnKeys),
		)
	} else {
		sqlStatement = fmt.Sprintf(`
			SELECT
			  *
			FROM
			  (
				SELECT
				  *,
				  row_number() OVER (
					PARTITION BY %[3]s
					ORDER BY
					  RECEIVED_AT DESC
				  ) AS _rudder_staging_row_number
				FROM
				  %[1]s.%[2]s
			  ) AS q
			WHERE
			  _rudder_staging_row_number = 1
		`,
			namespace,
			stagingTableName,
			pk,
		)
	}
	return
}

func mergeableLTSQLStatement(namespace, tableName, stagingTableName string, columnKeys []string, partitionQuery string) string {
	pk := primaryKey(tableName)
	if partitionQuery != "" {
		partitionQuery += " AND"
	}
	stagingTableSqlStatement := stagingSqlStatement(namespace, tableName, stagingTableName, columnKeys)
	sqlStatement := fmt.Sprintf(`
		MERGE INTO %[1]s.%[2]s AS MAIN USING (%[3]s) AS STAGING ON %[8]s MAIN.%[4]s = STAGING.%[4]s
		WHEN MATCHED THEN
		UPDATE
		SET
		  %[5]s
		  WHEN NOT MATCHED THEN
		INSERT
		  (%[6]s)
		VALUES
		  (%[7]s);
		`,
		namespace,
		tableName,
		stagingTableSqlStatement,
		pk,
		columnsWithValues(columnKeys),
		columnNames(columnKeys),
		stagingColumnNames(columnKeys),
		partitionQuery,
	)
	return sqlStatement
}

func appendableLTSQLStatement(namespace, tableName, stagingTableName string, columnKeys []string) string {
	stagingTableSqlStatement := stagingSqlStatement(namespace, tableName, stagingTableName, columnKeys)
	sqlStatement := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s (%[4]s)
		SELECT
		  %[4]s
		FROM
		  (%[5]s);
		`,
		namespace,
		tableName,
		stagingTableName,
		columnNames(columnKeys),
		stagingTableSqlStatement,
	)
	return sqlStatement
}

func (dl *Deltalake) ErrorMappings() []model.JobError {
	return errorsMappings
}

package azuresynapse

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	stringLengthLimit = 512
	provider          = warehouseutils.AzureSynapse
	tableNameLimit    = 127
)

var errorsMappings []model.JobError

var rudderDataTypesMapToAzureSynapse = map[string]string{
	"int":      "bigint",
	"float":    "decimal(28,10)",
	"string":   "varchar(512)",
	"datetime": "datetimeoffset",
	"boolean":  "bit",
	"json":     "jsonb",
}

var azureSynapseDataTypesMapToRudder = map[string]string{
	"integer":                  "int",
	"smallint":                 "int",
	"bigint":                   "int",
	"tinyint":                  "int",
	"double precision":         "float",
	"numeric":                  "float",
	"decimal":                  "float",
	"real":                     "float",
	"float":                    "float",
	"text":                     "string",
	"varchar":                  "string",
	"nvarchar":                 "string",
	"ntext":                    "string",
	"nchar":                    "string",
	"char":                     "string",
	"datetimeoffset":           "datetime",
	"date":                     "datetime",
	"datetime2":                "datetime",
	"timestamp with time zone": "datetime",
	"timestamp":                "datetime",
	"jsonb":                    "json",
	"bit":                      "boolean",
}

type AzureSynapse struct {
	DB                 *sqlmw.DB
	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	LoadFileDownLoader downloader.Downloader

	stats  stats.Stats
	conf   *config.Config
	logger logger.Logger

	config struct {
		numWorkersDownloadLoadFiles int
		slowQueryThreshold          time.Duration
	}
}

type credentials struct {
	host     string
	dbName   string
	user     string
	password string
	port     string
	sslMode  string
	timeout  time.Duration
}

var primaryKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

var partitionKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id, column_name, table_name",
}

func New(conf *config.Config, log logger.Logger, stats stats.Stats) *AzureSynapse {
	az := &AzureSynapse{
		stats:  stats,
		conf:   conf,
		logger: log.Child("integrations").Child("synapse"),
	}

	az.config.numWorkersDownloadLoadFiles = conf.GetInt("Warehouse.azure_synapse.numWorkersDownloadLoadFiles", 1)
	az.config.slowQueryThreshold = conf.GetDuration("Warehouse.azure_synapse.slowQueryThreshold", 5, time.Minute)

	return az
}

// connect to the azure synapse database
// if TrustServerCertificate is set to true, all options(disable, false, true) works.
// if forceSSL is set to 1, disable option doesn't work.
// If forceSSL is set to true or false, it works alongside with TrustServerCertificate=true
// more about combinations in here: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/connection-string-keywords-and-data-source-names-dsns?view=sql-server-ver15
func (as *AzureSynapse) connect() (*sqlmw.DB, error) {
	cred := as.connectionCredentials()

	port, err := strconv.Atoi(cred.port)
	if err != nil {
		return nil, fmt.Errorf("invalid port %q: %w", cred.port, err)
	}

	query := url.Values{}
	query.Add("database", cred.dbName)
	query.Add("encrypt", cred.sslMode)
	query.Add("TrustServerCertificate", "true")
	if cred.timeout > 0 {
		query.Add("dial timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}

	connUrl := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cred.user, cred.password),
		Host:     net.JoinHostPort(cred.host, strconv.Itoa(port)),
		RawQuery: query.Encode(),
	}

	db, err := sql.Open("sqlserver", connUrl.String())
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	middleware := sqlmw.New(
		db,
		sqlmw.WithStats(as.stats),
		sqlmw.WithLogger(as.logger),
		sqlmw.WithKeyAndValues([]any{
			logfield.SourceID, as.Warehouse.Source.ID,
			logfield.SourceType, as.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, as.Warehouse.Destination.ID,
			logfield.DestinationType, as.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, as.Warehouse.WorkspaceID,
			logfield.Namespace, as.Namespace,
		}),
		sqlmw.WithQueryTimeout(as.connectTimeout),
		sqlmw.WithSlowQueryThreshold(as.config.slowQueryThreshold),
	)
	return middleware, nil
}

func (as *AzureSynapse) connectionCredentials() *credentials {
	return &credentials{
		host:     as.Warehouse.GetStringDestinationConfig(as.conf, model.HostSetting),
		dbName:   as.Warehouse.GetStringDestinationConfig(as.conf, model.DatabaseSetting),
		user:     as.Warehouse.GetStringDestinationConfig(as.conf, model.UserSetting),
		password: as.Warehouse.GetStringDestinationConfig(as.conf, model.PasswordSetting),
		port:     as.Warehouse.GetStringDestinationConfig(as.conf, model.PortSetting),
		sslMode:  as.Warehouse.GetStringDestinationConfig(as.conf, model.SSLModeSetting),
		timeout:  as.connectTimeout,
	}
}

func columnsWithDataTypes(columns model.TableSchema, prefix string) string {
	formattedColumns := lo.MapToSlice(columns, func(name, dataType string) string {
		return fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToAzureSynapse[dataType])
	})
	return strings.Join(formattedColumns, ",")
}

func (*AzureSynapse) IsEmpty(_ context.Context, _ model.Warehouse) (empty bool, err error) {
	return
}

func (as *AzureSynapse) loadTable(
	ctx context.Context,
	tableName string,
	tableSchemaInUpload model.TableSchema,
	skipTempTableDelete bool,
) (*types.LoadTableStats, string, error) {
	log := as.logger.With(
		logfield.SourceID, as.Warehouse.Source.ID,
		logfield.SourceType, as.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, as.Warehouse.Destination.ID,
		logfield.DestinationType, as.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, as.Warehouse.WorkspaceID,
		logfield.Namespace, as.Namespace,
		logfield.TableName, tableName,
	)
	log.Infow("started loading")

	fileNames, err := as.LoadFileDownLoader.Download(ctx, tableName)
	if err != nil {
		return nil, "", fmt.Errorf("downloading load files: %w", err)
	}
	defer func() {
		misc.RemoveFilePaths(fileNames...)
	}()

	stagingTableName := warehouseutils.StagingTableName(
		provider,
		tableName,
		tableNameLimit,
	)

	// The use of prepared statements for creating temporary tables is not suitable in this context.
	// Temporary tables in SQL Server have a limited scope and are automatically purged after the transaction commits.
	// Therefore, creating normal tables is chosen as an alternative.
	//
	// For more information on this behavior:
	// - See the discussion at https://github.com/denisenkom/go-mssqldb/issues/149 regarding prepared statements.
	// - Refer to Microsoft's documentation on temporary tables at
	//   https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175528(v=sql.105)?redirectedfrom=MSDN.
	log.Debugw("creating staging table")
	createStagingTableStmt := fmt.Sprintf(`
		SELECT
		  TOP 0 * INTO %[1]s.%[2]s
		FROM
		  %[1]s.%[3]s;`,
		as.Namespace,
		stagingTableName,
		tableName,
	)
	if _, err = as.DB.ExecContext(ctx, createStagingTableStmt); err != nil {
		return nil, "", fmt.Errorf("creating temporary table: %w", err)
	}

	if !skipTempTableDelete {
		defer func() {
			as.dropStagingTable(ctx, stagingTableName)
		}()
	}

	txn, err := as.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(
		tableSchemaInUpload,
	)
	previousColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(
		as.Uploader.GetTableSchemaInWarehouse(
			tableName,
		),
	)
	extraColumns := lo.Filter(previousColumnKeys, func(item string, index int) bool {
		return !slices.Contains(sortedColumnKeys, item)
	})

	log.Debugw("creating prepared stmt for loading data")
	copyInStmt := mssql.CopyIn(as.Namespace+"."+stagingTableName, mssql.BulkOptions{CheckConstraints: false},
		append(sortedColumnKeys, extraColumns...)...,
	)
	stmt, err := txn.PrepareContext(ctx, copyInStmt)
	if err != nil {
		return nil, "", fmt.Errorf("preparing copyIn statement: %w", err)
	}

	log.Infow("loading data into staging table")
	for _, fileName := range fileNames {
		err = as.loadDataIntoStagingTable(
			ctx, log, stmt,
			fileName, sortedColumnKeys,
			extraColumns, tableSchemaInUpload,
		)
		if err != nil {
			return nil, "", fmt.Errorf("loading data into staging table from file %s: %w", fileName, err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return nil, "", fmt.Errorf("executing copyIn statement: %w", err)
	}

	log.Infow("deleting from load table")
	rowsDeleted, err := as.deleteFromLoadTable(
		ctx, txn, tableName,
		stagingTableName,
	)
	if err != nil {
		return nil, "", fmt.Errorf("delete from load table: %w", err)
	}

	log.Infow("inserting into load table")
	rowsInserted, err := as.insertIntoLoadTable(
		ctx, txn, tableName,
		stagingTableName, sortedColumnKeys,
	)
	if err != nil {
		return nil, "", fmt.Errorf("insert into load table: %w", err)
	}

	log.Debugw("committing transaction")
	if err = txn.Commit(); err != nil {
		return nil, "", fmt.Errorf("commit transaction: %w", err)
	}

	log.Infow("completed loading")

	return &types.LoadTableStats{
		RowsInserted: rowsInserted - rowsDeleted,
		RowsUpdated:  rowsDeleted,
	}, stagingTableName, nil
}

func (as *AzureSynapse) loadDataIntoStagingTable(
	ctx context.Context,
	log logger.Logger,
	stmt *sql.Stmt,
	fileName string,
	sortedColumnKeys []string,
	extraColumns []string,
	tableSchemaInUpload model.TableSchema,
) error {
	gzipFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer func() {
		_ = gzipFile.Close()
	}()

	gzipReader, err := gzip.NewReader(gzipFile)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}
	defer func() {
		_ = gzipReader.Close()
	}()

	csvReader := csv.NewReader(gzipReader)

	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading record from: %w", err)
		}
		if len(sortedColumnKeys) != len(record) {
			return fmt.Errorf("mismatch in number of columns: actual count: %d, expected count: %d",
				len(record),
				len(sortedColumnKeys),
			)
		}

		recordInterface := make([]interface{}, 0, len(record))
		for _, value := range record {
			if strings.TrimSpace(value) == "" {
				recordInterface = append(recordInterface, nil)
			} else {
				recordInterface = append(recordInterface, value)
			}
		}

		finalColumnValues := make([]interface{}, 0, len(record))
		for index, value := range recordInterface {
			valueType := tableSchemaInUpload[sortedColumnKeys[index]]
			if value == nil {
				log.Debugw("found nil value",
					logfield.ColumnType, valueType,
					logfield.ColumnName, sortedColumnKeys[index],
				)

				finalColumnValues = append(finalColumnValues, nil)
				continue
			}

			processedVal, err := as.ProcessColumnValue(
				value.(string),
				valueType,
			)
			if err != nil {
				log.Warnw("mismatch in datatype",
					logfield.ColumnType, valueType,
					logfield.ColumnName, sortedColumnKeys[index],
					logfield.Error, err,
				)
				finalColumnValues = append(finalColumnValues, nil)
			} else {
				finalColumnValues = append(finalColumnValues, processedVal)
			}
		}

		// To ensure the successful execution of the 'copyIn' operation in Azure Synapse,
		// it is necessary to handle the scenario where new columns are added to the target table.
		// Without this adjustment, attempting to perform 'copyIn' when the column count in the
		// target table does not match the column count specified in the input data will result
		// in an error like:
		//
		//   mssql: Column count in target table does not match column count specified in input.
		//
		// If this error is encountered, it is important to verify that the column structure in
		// the source data matches the destination table's structure. If you are using the BCP command,
		// ensure that the format file's column count matches the destination table. For SSIS data imports,
		// double-check that the column mappings are consistent with the target table.
		for range extraColumns {
			finalColumnValues = append(finalColumnValues, nil)
		}

		_, err = stmt.ExecContext(ctx, finalColumnValues...)
		if err != nil {
			return fmt.Errorf("exec statement record: %w", err)
		}
	}
	return nil
}

func (as *AzureSynapse) ProcessColumnValue(
	value string,
	valueType string,
) (interface{}, error) {
	switch valueType {
	case model.IntDataType:
		return strconv.Atoi(value)
	case model.FloatDataType:
		return strconv.ParseFloat(value, 64)
	case model.DateTimeDataType:
		return time.Parse(time.RFC3339, value)
	case model.BooleanDataType:
		return strconv.ParseBool(value)
	case model.StringDataType:
		if len(value) > stringLengthLimit {
			value = value[:stringLengthLimit]
		}
		if !hasDiacritics(value) {
			return value, nil
		} else {
			byteArr := str2ucs2(value)
			if len(byteArr) > stringLengthLimit {
				byteArr = byteArr[:stringLengthLimit]
			}
			return byteArr, nil
		}
	default:
		return value, nil
	}
}

func (as *AzureSynapse) deleteFromLoadTable(
	ctx context.Context,
	txn *sqlmw.Tx,
	tableName string,
	stagingTableName string,
) (int64, error) {
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	var additionalJoinClause string
	if tableName == warehouseutils.DiscardsTable {
		additionalJoinClause = fmt.Sprintf(`AND _source.%[3]s = %[1]q.%[2]q.%[3]q AND _source.%[4]s = %[1]q.%[2]q.%[4]q`,
			as.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	deleteStmt := fmt.Sprintf(`
		DELETE FROM
		  %[1]q.%[2]q
		FROM
		  %[1]q.%[3]q AS _source
		WHERE
		  (
			_source.%[4]s = %[1]q.%[2]q.%[4]q %[5]s
		  );`,
		as.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		additionalJoinClause,
	)

	r, err := txn.ExecContext(ctx, deleteStmt)
	if err != nil {
		return 0, fmt.Errorf("deleting from main table: %w", err)
	}
	return r.RowsAffected()
}

func (as *AzureSynapse) insertIntoLoadTable(
	ctx context.Context,
	txn *sqlmw.Tx,
	tableName string,
	stagingTableName string,
	sortedColumnKeys []string,
) (int64, error) {
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(
		sortedColumnKeys,
	)

	insertStmt := fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (%[3]s)
		SELECT
		  %[3]s
		FROM
		  (
			SELECT
			  *,
			  ROW_NUMBER() OVER (
				PARTITION BY %[5]s
				ORDER BY
				  received_at DESC
			  ) AS _rudder_staging_row_number
			FROM
			  %[1]q.%[4]q
		  ) AS _
		WHERE
		  _rudder_staging_row_number = 1;`,
		as.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	r, err := txn.ExecContext(ctx, insertStmt)
	if err != nil {
		return 0, fmt.Errorf("inserting intomain table: %w", err)
	}
	return r.RowsAffected()
}

// Taken from https://github.com/denisenkom/go-mssqldb/blob/master/tds.go
func str2ucs2(s string) []byte {
	res := utf16.Encode([]rune(s))
	ucs2 := make([]byte, 2*len(res))
	for i := 0; i < len(res); i++ {
		ucs2[2*i] = byte(res[i])
		ucs2[2*i+1] = byte(res[i] >> 8)
	}
	return ucs2
}

func hasDiacritics(str string) bool {
	for _, x := range str {
		if utf8.RuneLen(x) > 1 {
			return true
		}
	}
	return false
}

func (as *AzureSynapse) loadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	as.logger.Infof("AZ: Starting load for identifies and users tables\n")
	_, identifyStagingTable, err := as.loadTable(ctx, warehouseutils.IdentifiesTable, as.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(as.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	unionStagingTableName := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	stagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	defer as.dropStagingTable(ctx, stagingTableName)
	defer as.dropStagingTable(ctx, unionStagingTableName)
	defer as.dropStagingTable(ctx, identifyStagingTable)

	userColMap := as.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`%q`, colName))
		caseSubQuery := fmt.Sprintf(`case
						  when (exists(select 1)) then (
						  	select top 1 %[1]q from %[2]s
						  	where x.id = %[2]s.id
							  and %[1]q is not null
							order by received_at desc
							)
						  end as %[1]q`, colName, as.Namespace+"."+unionStagingTableName)
		// IGNORE NULLS only supported in Azure SQL edge, in which case the query can be shortened to below
		// https://docs.microsoft.com/en-us/sql/t-sql/functions/first-value-transact-sql?view=sql-server-ver15
		// caseSubQuery := fmt.Sprintf(`FIRST_VALUE(%[1]s) IGNORE NULLS OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	// TODO: skipped top level temporary table for now
	sqlStatement := fmt.Sprintf(`SELECT * into %[5]s FROM
												((
													SELECT id, %[4]s FROM %[2]s WHERE id in (SELECT user_id FROM %[3]s WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[4]s FROM %[3]s  WHERE user_id IS NOT NULL
												)) a
											`, as.Namespace, as.Namespace+"."+warehouseutils.UsersTable, as.Namespace+"."+identifyStagingTable, strings.Join(userColNames, ","), as.Namespace+"."+unionStagingTableName)

	as.logger.Debugf("AZ: Creating staging table for union of users table with identify staging table: %s\n", sqlStatement)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`SELECT * INTO %[1]s FROM (SELECT DISTINCT * FROM
										(
											SELECT
											x.id, %[2]s
											FROM %[3]s as x
										) as xyz
									) a`,
		as.Namespace+"."+stagingTableName,
		strings.Join(firstValProps, ","),
		as.Namespace+"."+unionStagingTableName,
	)

	as.logger.Debugf("AZ: Creating staging table for users: %s\n", sqlStatement)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		as.logger.Errorf("AZ: Error Creating staging table for users: %s\n", sqlStatement)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// BEGIN TRANSACTION
	tx, err := as.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" FROM %[3]s _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, as.Namespace, warehouseutils.UsersTable, as.Namespace+"."+stagingTableName, primaryKey)
	as.logger.Infof("AZ: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		as.logger.Errorf("AZ: Error deleting from main table for dedup: %v\n", err)
		_ = tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  %[3]s`, as.Namespace, warehouseutils.UsersTable, as.Namespace+"."+stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	as.logger.Infof("AZ: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		as.logger.Errorf("AZ: Error inserting into users table from staging table: %v\n", err)
		_ = tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		as.logger.Errorf("AZ: Error in transaction commit for users table: %v\n", err)
		_ = tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (*AzureSynapse) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

func (as *AzureSynapse) CreateSchema(ctx context.Context) (err error) {
	sqlStatement := fmt.Sprintf(`IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'%s' )
    EXEC('CREATE SCHEMA [%s]');`, as.Namespace, as.Namespace)
	as.logger.Infof("SYNAPSE: Creating schema name in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return
}

func (as *AzureSynapse) dropStagingTable(ctx context.Context, stagingTableName string) {
	as.logger.Infof("AZ: dropping table %+v\n", stagingTableName)
	_, err := as.DB.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID ('%[1]s','U') IS NOT NULL DROP TABLE %[1]s;`, as.Namespace+"."+stagingTableName))
	if err != nil {
		as.logger.Errorf("AZ:  Error dropping staging table %s in synapse: %v", as.Namespace+"."+stagingTableName, err)
	}
}

func (as *AzureSynapse) createTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sqlStatement := fmt.Sprintf(`IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
	CREATE TABLE %[1]s ( %v )`, name, columnsWithDataTypes(columns, ""))

	as.logger.Infof("AZ: Creating table in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	return
}

func (as *AzureSynapse) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	// Search paths doesn't exist unlike Postgres, default is dbo. Hence, use namespace wherever possible
	err = as.createTable(ctx, as.Namespace+"."+tableName, columnMap)
	return err
}

func (as *AzureSynapse) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	as.logger.Infof("AZ: Dropping table in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.ExecContext(ctx, fmt.Sprintf(sqlStatement, as.Namespace, tableName))
	return
}

func (as *AzureSynapse) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	if len(columnsInfo) == 1 {
		queryBuilder.WriteString(fmt.Sprintf(`
			IF NOT EXISTS (
			  SELECT
				1
			  FROM
				SYS.COLUMNS
			  WHERE
				OBJECT_ID = OBJECT_ID(N'%[1]s.%[2]s')
				AND name = '%[3]s'
			)`,
			as.Namespace,
			tableName,
			columnsInfo[0].Name,
		))
	}

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s
		ADD`,
		as.Namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` %q %s,`, columnInfo.Name, rudderDataTypesMapToAzureSynapse[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	as.logger.Infof("AZ: Adding columns for destinationID: %s, tableName: %s with query: %v", as.Warehouse.Destination.ID, tableName, query)
	_, err = as.DB.ExecContext(ctx, query)
	return
}

func (*AzureSynapse) AlterColumn(_ context.Context, _, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

func (as *AzureSynapse) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := as.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (as *AzureSynapse) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	as.Warehouse = warehouse
	as.Namespace = warehouse.Namespace
	as.Uploader = uploader
	as.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.AzureSynapse, warehouse.Destination.Config, as.Uploader.UseRudderStorage())
	as.LoadFileDownLoader = downloader.NewDownloader(&warehouse, uploader, as.config.numWorkersDownloadLoadFiles)

	if as.DB, err = as.connect(); err != nil {
		return fmt.Errorf("connecting to azure synapse: %w", err)
	}
	return err
}

func (as *AzureSynapse) dropDanglingStagingTables(ctx context.Context) error {
	sqlStatement := fmt.Sprintf(`
		select
		  table_name
		from
		  information_schema.tables
		where
		  table_schema = '%s'
		  AND table_name like '%s';
	`,
		as.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	rows, err := as.DB.QueryContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("querying for dangling staging tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return fmt.Errorf("scanning for dangling staging tables: %w", err)
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating for dangling staging tables: %w", err)
	}
	as.logger.Infof("WH: SYNAPSE: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	for _, stagingTableName := range stagingTableNames {
		_, err := as.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, as.Namespace, stagingTableName))
		if err != nil {
			return fmt.Errorf("dropping dangling staging table %q.%q: %w", as.Namespace, stagingTableName, err)
		}
	}
	return nil
}

// FetchSchema returns the schema of the warehouse
func (as *AzureSynapse) FetchSchema(ctx context.Context) (model.Schema, error) {
	schema := make(model.Schema)

	sqlStatement := `
		SELECT
			table_name,
			column_name,
			data_type
		FROM
			INFORMATION_SCHEMA.COLUMNS
		WHERE
			table_schema = @schema
			and table_name not like @prefix
`
	rows, err := as.DB.QueryContext(
		ctx,
		sqlStatement,
		sql.Named("schema", as.Namespace),
		sql.Named("prefix", fmt.Sprintf("%s%%", warehouseutils.StagingTablePrefix(provider))),
	)
	if errors.Is(err, io.EOF) {
		return schema, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string

		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := azureSynapseDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			warehouseutils.WHCounterStat(as.stats, warehouseutils.RudderMissingDatatype, &as.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, nil
}

func (as *AzureSynapse) LoadUserTables(ctx context.Context) map[string]error {
	return as.loadUserTables(ctx)
}

func (as *AzureSynapse) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStat, _, err := as.loadTable(
		ctx,
		tableName,
		as.Uploader.GetTableSchemaInUpload(tableName),
		false,
	)
	return loadTableStat, err
}

func (as *AzureSynapse) Cleanup(ctx context.Context) {
	if as.DB != nil {
		// extra check aside dropStagingTable(table)
		err := as.dropDanglingStagingTables(ctx)
		if err != nil {
			as.logger.Warnw("Error dropping dangling staging tables",
				logfield.DestinationID, as.Warehouse.Destination.ID,
				logfield.DestinationType, as.Warehouse.Destination.DestinationDefinition.Name,
				logfield.SourceID, as.Warehouse.Source.ID,
				logfield.SourceType, as.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, as.Warehouse.Destination.ID,
				logfield.DestinationType, as.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, as.Warehouse.WorkspaceID,
				logfield.Namespace, as.Warehouse.Namespace,

				logfield.Error, err,
			)
		}
		_ = as.DB.Close()
	}
}

func (*AzureSynapse) LoadIdentityMergeRulesTable(_ context.Context) (err error) {
	return
}

func (*AzureSynapse) LoadIdentityMappingsTable(_ context.Context) (err error) {
	return
}

func (*AzureSynapse) DownloadIdentityRules(context.Context, *misc.GZipWriter) (err error) {
	return
}

func (as *AzureSynapse) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	as.Warehouse = warehouse
	as.Namespace = warehouse.Namespace

	db, err := as.connect()
	if err != nil {
		return client.Client{}, fmt.Errorf("connecting to azure synapse: %w", err)
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, err
}

func (as *AzureSynapse) LoadTestTable(ctx context.Context, _, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		as.Namespace,
		tableName,
		fmt.Sprintf(`%q, %q`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	return
}

func (as *AzureSynapse) SetConnectionTimeout(timeout time.Duration) {
	as.connectTimeout = timeout
}

func (*AzureSynapse) ErrorMappings() []model.JobError {
	return errorsMappings
}

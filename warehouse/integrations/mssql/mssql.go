package mssql

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
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"

	"github.com/rudderlabs/rudder-go-kit/stats"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	stringLengthLimit = 512
	provider          = warehouseutils.MSSQL
	tableNameLimit    = 127
)

var rudderDataTypesMapToMssql = map[string]string{
	"int":      "bigint",
	"float":    "decimal(28,10)",
	"string":   "nvarchar(512)",
	"datetime": "datetimeoffset",
	"boolean":  "bit",
	"json":     "jsonb",
}

var mssqlDataTypesMapToRudder = map[string]string{
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

type MSSQL struct {
	DB                 *sqlmw.DB
	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	LoadFileDownLoader downloader.Downloader

	conf   *config.Config
	stats  stats.Stats
	logger logger.Logger

	config struct {
		enableDeleteByJobs          bool
		numWorkersDownloadLoadFiles int
		slowQueryThreshold          time.Duration
	}
}

type credentials struct {
	host     string
	database string
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

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`unable to open tcp connection with host .*: dial tcp .*: i/o timeout`),
	},
}

func New(conf *config.Config, log logger.Logger, stats stats.Stats) *MSSQL {
	ms := &MSSQL{
		conf:   conf,
		stats:  stats,
		logger: log.Child("integrations").Child("mssql"),
	}
	ms.config.enableDeleteByJobs = conf.GetBool("Warehouse.mssql.enableDeleteByJobs", false)
	ms.config.numWorkersDownloadLoadFiles = conf.GetInt("Warehouse.mssql.numWorkersDownloadLoadFiles", 1)
	ms.config.slowQueryThreshold = conf.GetDuration("Warehouse.mssql.slowQueryThreshold", 5, time.Minute)

	return ms
}

// connect to mssql database
// if TrustServerCertificate is set to true, all options(disable, false, true) works.
// if forceSSL is set to 1, disable option doesn't work.
// If forceSSL is set to true or false, it works alongside with TrustServerCertificate=true
// more about combinations in here: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/connection-string-keywords-and-data-source-names-dsns?view=sql-server-ver15
func (ms *MSSQL) connect() (*sqlmw.DB, error) {
	cred := ms.connectionCredentials()

	port, err := strconv.Atoi(cred.port)
	if err != nil {
		return nil, fmt.Errorf("invalid port %q: %w", cred.port, err)
	}

	query := url.Values{}
	query.Add("database", cred.database)
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
		sqlmw.WithStats(ms.stats),
		sqlmw.WithLogger(ms.logger),
		sqlmw.WithKeyAndValues([]any{
			logfield.SourceID, ms.Warehouse.Source.ID,
			logfield.SourceType, ms.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, ms.Warehouse.Destination.ID,
			logfield.DestinationType, ms.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, ms.Warehouse.WorkspaceID,
			logfield.Namespace, ms.Namespace,
		}),
		sqlmw.WithQueryTimeout(ms.connectTimeout),
		sqlmw.WithSlowQueryThreshold(ms.config.slowQueryThreshold),
	)
	return middleware, nil
}

func (ms *MSSQL) connectionCredentials() *credentials {
	return &credentials{
		host:     ms.Warehouse.GetStringDestinationConfig(ms.conf, model.HostSetting),
		database: ms.Warehouse.GetStringDestinationConfig(ms.conf, model.DatabaseSetting),
		user:     ms.Warehouse.GetStringDestinationConfig(ms.conf, model.UserSetting),
		password: ms.Warehouse.GetStringDestinationConfig(ms.conf, model.PasswordSetting),
		port:     ms.Warehouse.GetStringDestinationConfig(ms.conf, model.PortSetting),
		sslMode:  ms.Warehouse.GetStringDestinationConfig(ms.conf, model.SSLModeSetting),
		timeout:  ms.connectTimeout,
	}
}

func ColumnsWithDataTypes(columns model.TableSchema, prefix string) string {
	formattedColumns := lo.MapToSlice(columns, func(name, dataType string) string {
		return fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToMssql[dataType])
	})
	return strings.Join(formattedColumns, ",")
}

func (*MSSQL) IsEmpty(context.Context, model.Warehouse) (empty bool, err error) {
	return
}

func (ms *MSSQL) DeleteBy(ctx context.Context, tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	for _, tb := range tableNames {
		ms.logger.Infof("MSSQL: Cleaning up the table %q ", tb)
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
		context_sources_job_run_id <> @jobrunid AND
		context_sources_task_run_id <> @taskrunid AND
		context_source_id = @sourceid AND
		received_at < @starttime`,
			ms.Namespace,
			tb,
		)

		ms.logger.Debugf("MSSQL: Deleting rows in table in mysql for MSSQL:%s ", ms.Warehouse.Destination.ID)
		ms.logger.Infof("MSSQL: Executing the statement %v", sqlStatement)

		if ms.config.enableDeleteByJobs {
			_, err = ms.DB.ExecContext(ctx, sqlStatement,
				sql.Named("jobrunid", params.JobRunId),
				sql.Named("taskrunid", params.TaskRunId),
				sql.Named("sourceid", params.SourceId),
				sql.Named("starttime", params.StartTime),
			)
			if err != nil {
				ms.logger.Errorf("Error %s", err)
				return err
			}
		}

	}
	return nil
}

func (ms *MSSQL) loadTable(
	ctx context.Context,
	tableName string,
	tableSchemaInUpload model.TableSchema,
	skipTempTableDelete bool,
) (*types.LoadTableStats, string, error) {
	log := ms.logger.With(
		logfield.SourceID, ms.Warehouse.Source.ID,
		logfield.SourceType, ms.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, ms.Warehouse.Destination.ID,
		logfield.DestinationType, ms.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, ms.Warehouse.WorkspaceID,
		logfield.Namespace, ms.Namespace,
		logfield.TableName, tableName,
	)
	log.Infow("started loading")

	fileNames, err := ms.LoadFileDownLoader.Download(ctx, tableName)
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
		ms.Namespace,
		stagingTableName,
		tableName,
	)
	if _, err = ms.DB.ExecContext(ctx, createStagingTableStmt); err != nil {
		return nil, "", fmt.Errorf("creating temporary table: %w", err)
	}

	if !skipTempTableDelete {
		defer func() {
			ms.dropStagingTable(ctx, stagingTableName)
		}()
	}

	txn, err := ms.DB.BeginTx(ctx, &sql.TxOptions{})
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

	log.Debugw("creating prepared stmt for loading data")
	copyInStmt := mssql.CopyIn(ms.Namespace+"."+stagingTableName, mssql.BulkOptions{CheckConstraints: false},
		sortedColumnKeys...,
	)
	stmt, err := txn.PrepareContext(ctx, copyInStmt)
	if err != nil {
		return nil, "", fmt.Errorf("preparing copyIn statement: %w", err)
	}

	log.Infow("loading data into staging table")
	for _, fileName := range fileNames {
		err = ms.loadDataIntoStagingTable(
			ctx, log, stmt,
			fileName, sortedColumnKeys,
			tableSchemaInUpload,
		)
		if err != nil {
			return nil, "", fmt.Errorf("loading data into staging table: %w", err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return nil, "", fmt.Errorf("executing copyIn statement: %w", err)
	}

	log.Infow("deleting from load table")
	rowsDeleted, err := ms.deleteFromLoadTable(
		ctx, txn, tableName,
		stagingTableName,
	)
	if err != nil {
		return nil, "", fmt.Errorf("delete from load table: %w", err)
	}

	log.Infow("inserting into load table")
	rowsInserted, err := ms.insertIntoLoadTable(
		ctx, txn, tableName,
		stagingTableName, sortedColumnKeys,
	)
	if err != nil {
		return nil, "", fmt.Errorf("insert into: %w", err)
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

func (ms *MSSQL) loadDataIntoStagingTable(
	ctx context.Context,
	log logger.Logger,
	stmt *sql.Stmt,
	fileName string,
	sortedColumnKeys []string,
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
		var record []string
		record, err = csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading file: %w", err)
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
				log.Warnw("found nil value",
					logfield.ColumnType, valueType,
					logfield.ColumnName, sortedColumnKeys[index],
				)

				finalColumnValues = append(finalColumnValues, nil)
				continue
			}

			processedVal, err := ms.ProcessColumnValue(
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

		_, err = stmt.ExecContext(ctx, finalColumnValues...)
		if err != nil {
			return fmt.Errorf("exec statement error: %w", err)
		}
	}
	return nil
}

func (ms *MSSQL) ProcessColumnValue(
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

func (ms *MSSQL) deleteFromLoadTable(
	ctx context.Context,
	txn *sqlmw.Tx,
	tableName string,
	stagingTableName string,
) (int64, error) {
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	var additionalDeleteStmtClause string
	if tableName == warehouseutils.DiscardsTable {
		additionalDeleteStmtClause = fmt.Sprintf(`AND _source.%[3]s = %[1]q.%[2]q.%[3]q AND _source.%[4]s = %[1]q.%[2]q.%[4]q`,
			ms.Namespace,
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
		ms.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
		additionalDeleteStmtClause,
	)

	r, err := txn.ExecContext(ctx, deleteStmt)
	if err != nil {
		return 0, fmt.Errorf("deleting from main table: %w", err)
	}
	return r.RowsAffected()
}

func (ms *MSSQL) insertIntoLoadTable(
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
		ms.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	r, err := txn.ExecContext(ctx, insertStmt)
	if err != nil {
		return 0, fmt.Errorf("inserting into main table: %w", err)
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

func (ms *MSSQL) loadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	ms.logger.Infof("MSSQL: Starting load for identifies and users tables\n")
	_, identifyStagingTable, err := ms.loadTable(ctx, warehouseutils.IdentifiesTable, ms.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(ms.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	unionStagingTableName := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	stagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	defer ms.dropStagingTable(ctx, stagingTableName)
	defer ms.dropStagingTable(ctx, unionStagingTableName)
	defer ms.dropStagingTable(ctx, identifyStagingTable)

	userColMap := ms.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`%q`, colName))
		caseSubQuery := fmt.Sprintf(`case
						  when (exists(select 1)) then (
						  	select "%[1]s" from %[2]s
						  	where x.id = %[2]s.id
							  and "%[1]s" is not null
							  order by received_at desc
						  	OFFSET 0 ROWS
							FETCH NEXT 1 ROWS ONLY)
						  end as "%[1]s"`, colName, ms.Namespace+"."+unionStagingTableName)

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
											`, ms.Namespace, ms.Namespace+"."+warehouseutils.UsersTable, ms.Namespace+"."+identifyStagingTable, strings.Join(userColNames, ","), ms.Namespace+"."+unionStagingTableName)

	ms.logger.Debugf("MSSQL: Creating staging table for union of users table with identify staging table: %s\n", sqlStatement)
	_, err = ms.DB.ExecContext(ctx, sqlStatement)
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
		ms.Namespace+"."+stagingTableName,
		strings.Join(firstValProps, ","),
		ms.Namespace+"."+unionStagingTableName,
	)

	ms.logger.Debugf("MSSQL: Creating staging table for users: %s\n", sqlStatement)
	_, err = ms.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		ms.logger.Errorf("MSSQL: Error Creating staging table for users: %s\n", sqlStatement)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// BEGIN TRANSACTION
	tx, err := ms.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" FROM %[3]s _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, ms.Namespace, warehouseutils.UsersTable, ms.Namespace+"."+stagingTableName, primaryKey)
	ms.logger.Infof("MSSQL: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		ms.logger.Errorf("MSSQL: Error deleting from main table for dedup: %v\n", err)
		_ = tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  %[3]s`, ms.Namespace, warehouseutils.UsersTable, ms.Namespace+"."+stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	ms.logger.Infof("MSSQL: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.ExecContext(ctx, sqlStatement)
	if err != nil {
		ms.logger.Errorf("MSSQL: Error inserting into users table from staging table: %v\n", err)
		_ = tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		ms.logger.Errorf("MSSQL: Error in transaction commit for users table: %v\n", err)
		_ = tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (ms *MSSQL) CreateSchema(ctx context.Context) (err error) {
	sqlStatement := fmt.Sprintf(`IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'%s' )
    EXEC('CREATE SCHEMA [%s]');`, ms.Namespace, ms.Namespace)
	ms.logger.Infof("MSSQL: Creating schema name in mssql for MSSQL:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.DB.ExecContext(ctx, sqlStatement)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return
}

func (ms *MSSQL) dropStagingTable(ctx context.Context, stagingTableName string) {
	ms.logger.Infof("MSSQL: dropping table %+v\n", stagingTableName)
	_, err := ms.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, ms.Namespace+"."+stagingTableName))
	if err != nil {
		ms.logger.Errorf("MSSQL:  Error dropping staging table %s in mssql: %v", ms.Namespace+"."+stagingTableName, err)
	}
}

func (ms *MSSQL) createTable(ctx context.Context, name string, columns model.TableSchema) (err error) {
	sqlStatement := fmt.Sprintf(`IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
	CREATE TABLE %[1]s ( %v )`, name, ColumnsWithDataTypes(columns, ""))

	ms.logger.Infof("MSSQL: Creating table in mssql for MSSQL:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.DB.ExecContext(ctx, sqlStatement)
	return
}

func (ms *MSSQL) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	// Search paths doesn't exist unlike Postgres, default is dbo. Hence, use namespace wherever possible
	err = ms.createTable(ctx, ms.Namespace+"."+tableName, columnMap)
	return err
}

func (ms *MSSQL) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	ms.logger.Infof("AZ: Dropping table in synapse for AZ:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.DB.ExecContext(ctx, fmt.Sprintf(sqlStatement, ms.Namespace, tableName))
	return
}

func (ms *MSSQL) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
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
			ms.Namespace,
			tableName,
			columnsInfo[0].Name,
		))
	}

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s
		ADD`,
		ms.Namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` %q %s,`, columnInfo.Name, rudderDataTypesMapToMssql[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	ms.logger.Infof("MSSQL: Adding columns for destinationID: %s, tableName: %s with query: %v", ms.Warehouse.Destination.ID, tableName, query)
	_, err = ms.DB.ExecContext(ctx, query)
	return
}

func (*MSSQL) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

func (ms *MSSQL) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := ms.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (ms *MSSQL) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	ms.Warehouse = warehouse
	ms.Namespace = warehouse.Namespace
	ms.Uploader = uploader
	ms.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.MSSQL, warehouse.Destination.Config, ms.Uploader.UseRudderStorage())
	ms.LoadFileDownLoader = downloader.NewDownloader(&warehouse, uploader, ms.config.numWorkersDownloadLoadFiles)

	if ms.DB, err = ms.connect(); err != nil {
		return fmt.Errorf("connecting to mssql: %w", err)
	}
	return err
}

func (ms *MSSQL) dropDanglingStagingTables(ctx context.Context) error {
	sqlStatement := fmt.Sprintf(`
		select
		  table_name
		from
		  information_schema.tables
		where
		  table_schema = '%s'
		  AND table_name like '%s';
	`,
		ms.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	rows, err := ms.DB.QueryContext(ctx, sqlStatement)
	if err != nil {
		return fmt.Errorf("querying for dangling staging tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return fmt.Errorf("scanning dangling staging tables: %w", err)
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating dangling staging tables: %w", err)
	}
	ms.logger.Infof("WH: MSSQL: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	for _, stagingTableName := range stagingTableNames {
		_, err := ms.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, ms.Namespace, stagingTableName))
		if err != nil {
			return fmt.Errorf("dropping dangling staging tables %q.%q : %w", ms.Namespace, stagingTableName, err)
		}
	}
	return nil
}

// FetchSchema queries mssql and returns the schema associated with provided namespace
func (ms *MSSQL) FetchSchema(ctx context.Context) (model.Schema, error) {
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
			and table_name not like @prefix`
	rows, err := ms.DB.QueryContext(ctx, sqlStatement,
		sql.Named("schema", ms.Namespace),
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
		if datatype, ok := mssqlDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			warehouseutils.WHCounterStat(ms.stats, warehouseutils.RudderMissingDatatype, &ms.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, nil
}

func (ms *MSSQL) LoadUserTables(ctx context.Context) map[string]error {
	return ms.loadUserTables(ctx)
}

func (ms *MSSQL) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStat, _, err := ms.loadTable(
		ctx,
		tableName,
		ms.Uploader.GetTableSchemaInUpload(tableName),
		false,
	)
	return loadTableStat, err
}

func (ms *MSSQL) Cleanup(ctx context.Context) {
	if ms.DB != nil {
		// extra check aside dropStagingTable(table)
		err := ms.dropDanglingStagingTables(ctx)
		if err != nil {
			ms.logger.Warnw("Error dropping dangling staging tables",
				logfield.DestinationID, ms.Warehouse.Destination.ID,
				logfield.DestinationType, ms.Warehouse.Destination.DestinationDefinition.Name,
				logfield.SourceID, ms.Warehouse.Source.ID,
				logfield.SourceType, ms.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, ms.Warehouse.Destination.ID,
				logfield.DestinationType, ms.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, ms.Warehouse.WorkspaceID,
				logfield.Namespace, ms.Warehouse.Namespace,

				logfield.Error, err,
			)
		}

		_ = ms.DB.Close()
	}
}

func (*MSSQL) LoadIdentityMergeRulesTable(context.Context) (err error) {
	return
}

func (*MSSQL) LoadIdentityMappingsTable(context.Context) (err error) {
	return
}

func (*MSSQL) DownloadIdentityRules(context.Context, *misc.GZipWriter) (err error) {
	return
}

func (ms *MSSQL) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	ms.Warehouse = warehouse
	ms.Namespace = warehouse.Namespace
	ms.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.MSSQL,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(ms.Warehouse.Destination.Config),
	)

	db, err := ms.connect()
	if err != nil {
		return client.Client{}, fmt.Errorf("connecting to mssql: %w", err)
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, err
}

func (ms *MSSQL) LoadTestTable(ctx context.Context, _, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		ms.Namespace,
		tableName,
		fmt.Sprintf(`%q, %q`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = ms.DB.ExecContext(ctx, sqlStatement)
	return
}

func (ms *MSSQL) SetConnectionTimeout(timeout time.Duration) {
	ms.connectTimeout = timeout
}

func (*MSSQL) ErrorMappings() []model.JobError {
	return errorsMappings
}

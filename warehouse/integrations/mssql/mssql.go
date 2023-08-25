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
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
)

const (
	mssqlStringLengthLimit = 512
	provider               = warehouseutils.MSSQL
	tableNameLimit         = 127
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
		sqlmw.WithKeyAndValues(ms.defaultLogFields()),
		sqlmw.WithQueryTimeout(ms.connectTimeout),
		sqlmw.WithSlowQueryThreshold(ms.config.slowQueryThreshold),
	)
	return middleware, nil
}

func (ms *MSSQL) connectionCredentials() *credentials {
	return &credentials{
		host:     warehouseutils.GetConfigValue(host, ms.Warehouse),
		database: warehouseutils.GetConfigValue(dbName, ms.Warehouse),
		user:     warehouseutils.GetConfigValue(user, ms.Warehouse),
		password: warehouseutils.GetConfigValue(password, ms.Warehouse),
		port:     warehouseutils.GetConfigValue(port, ms.Warehouse),
		sslMode:  warehouseutils.GetConfigValue(sslMode, ms.Warehouse),
		timeout:  ms.connectTimeout,
	}
}

func (ms *MSSQL) defaultLogFields() []any {
	return []any{
		logfield.SourceID, ms.Warehouse.Source.ID,
		logfield.SourceType, ms.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, ms.Warehouse.Destination.ID,
		logfield.DestinationType, ms.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, ms.Warehouse.WorkspaceID,
		logfield.Namespace, ms.Namespace,
	}
}

func ColumnsWithDataTypes(columns model.TableSchema, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToMssql[dataType]))
	}
	return strings.Join(arr, ",")
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

func (ms *MSSQL) loadTable(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema, skipTempTableDelete bool) (stagingTableName string, err error) {
	ms.logger.Infof("MSSQL: Starting load for table:%s", tableName)

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

	fileNames, err := ms.LoadFileDownLoader.Download(ctx, tableName)
	defer misc.RemoveFilePaths(fileNames...)
	if err != nil {
		return
	}

	txn, err := ms.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		ms.logger.Errorf("MSSQL: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		return
	}
	// create temporary table
	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	// prepared stmts cannot be used to create temp objects here. Will work in a txn, but will be purged after commit.
	// https://github.com/denisenkom/go-mssqldb/issues/149, https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175528(v=sql.105)?redirectedfrom=MSDN
	// sqlStatement := fmt.Sprintf(`CREATE  TABLE ##%[2]s like %[1]s.%[3]s`, ms.Namespace, stagingTableName, tableName)
	// Hence falling back to creating normal tables
	sqlStatement := fmt.Sprintf(`select top 0 * into %[1]s.%[2]s from %[1]s.%[3]s`, ms.Namespace, stagingTableName, tableName)

	ms.logger.Debugf("MSSQL: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = txn.ExecContext(ctx, sqlStatement)
	if err != nil {
		ms.logger.Errorf("MSSQL: Error creating temporary table for table:%s: %v\n", tableName, err)
		_ = txn.Rollback()
		return
	}
	if !skipTempTableDelete {
		defer ms.dropStagingTable(ctx, stagingTableName)
	}

	stmt, err := txn.PrepareContext(ctx, mssql.CopyIn(ms.Namespace+"."+stagingTableName, mssql.BulkOptions{CheckConstraints: false}, sortedColumnKeys...))
	if err != nil {
		ms.logger.Errorf("MSSQL: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		_ = txn.Rollback()
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			ms.logger.Errorf("MSSQL: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			_ = txn.Rollback()
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			ms.logger.Errorf("MSSQL: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			gzipFile.Close()
			_ = txn.Rollback()
			return

		}
		csvReader := csv.NewReader(gzipReader)
		var csvRowsProcessedCount int
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					ms.logger.Debugf("MSSQL: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
					break
				}
				ms.logger.Errorf("MSSQL: Error while reading csv file %s for loading in staging table:%s: %v", objectFileName, stagingTableName, err)
				_ = txn.Rollback()
				return
			}
			if len(sortedColumnKeys) != len(record) {
				err = fmt.Errorf(`load file CSV columns for a row mismatch number found in upload schema. Columns in CSV row: %d, Columns in upload schema of table-%s: %d. Processed rows in csv file until mismatch: %d`, len(record), tableName, len(sortedColumnKeys), csvRowsProcessedCount)
				ms.logger.Error(err)
				_ = txn.Rollback()
				return
			}
			var recordInterface []interface{}
			for _, value := range record {
				if strings.TrimSpace(value) == "" {
					recordInterface = append(recordInterface, nil)
				} else {
					recordInterface = append(recordInterface, value)
				}
			}
			var finalColumnValues []interface{}
			for index, value := range recordInterface {
				valueType := tableSchemaInUpload[sortedColumnKeys[index]]
				if value == nil {
					ms.logger.Debugf("MS : Found nil value for type : %s, column : %s", valueType, sortedColumnKeys[index])
					finalColumnValues = append(finalColumnValues, nil)
					continue
				}
				strValue := value.(string)
				switch valueType {
				case "int":
					var convertedValue int
					if convertedValue, err = strconv.Atoi(strValue); err != nil {
						ms.logger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
				case "float":
					var convertedValue float64
					if convertedValue, err = strconv.ParseFloat(strValue, 64); err != nil {
						ms.logger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
				case "datetime":
					var convertedValue time.Time
					// TODO : handling milli?
					if convertedValue, err = time.Parse(time.RFC3339, strValue); err != nil {
						ms.logger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
					// TODO : handling all cases?
				case "boolean":
					var convertedValue bool
					if convertedValue, err = strconv.ParseBool(strValue); err != nil {
						ms.logger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
				case "string":
					// This is needed to enable diacritic support Ex: Ü,ç Ç,©,∆,ß,á,ù,ñ,ê
					// A substitute to this PR; https://github.com/denisenkom/go-mssqldb/pull/576/files
					// An alternate to this approach is to use nvarchar(instead of varchar)
					if len(strValue) > mssqlStringLengthLimit {
						strValue = strValue[:mssqlStringLengthLimit]
					}
					var byteArr []byte
					if hasDiacritics(strValue) {
						ms.logger.Debug("diacritics " + strValue)
						byteArr = str2ucs2(strValue)
						// This is needed as with above operation every character occupies 2 bytes
						if len(byteArr) > mssqlStringLengthLimit {
							byteArr = byteArr[:mssqlStringLengthLimit]
						}
						finalColumnValues = append(finalColumnValues, byteArr)
					} else {
						ms.logger.Debug("non-diacritic : " + strValue)
						finalColumnValues = append(finalColumnValues, strValue)
					}
				default:
					finalColumnValues = append(finalColumnValues, value)
				}
			}

			_, err = stmt.ExecContext(ctx, finalColumnValues...)
			if err != nil {
				ms.logger.Errorf("MSSQL: Error in exec statement for loading in staging table:%s: %v", stagingTableName, err)
				_ = txn.Rollback()
				return
			}
			csvRowsProcessedCount++
		}
		_ = gzipReader.Close()
		gzipFile.Close()
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		_ = txn.Rollback()
		ms.logger.Errorf("MSSQL: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		return

	}
	// deduplication process
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}
	var additionalJoinClause string
	if tableName == warehouseutils.DiscardsTable {
		additionalJoinClause = fmt.Sprintf(`AND _source.%[3]s = "%[1]s"."%[2]s"."%[3]s" AND _source.%[4]s = "%[1]s"."%[2]s"."%[4]s"`, ms.Namespace, tableName, "table_name", "column_name")
	}
	sqlStatement = fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" FROM "%[1]s"."%[3]s" as  _source where (_source.%[4]s = "%[1]s"."%[2]s"."%[4]s" %[5]s)`, ms.Namespace, tableName, stagingTableName, primaryKey, additionalJoinClause)
	ms.logger.Infof("MSSQL: Deduplicate records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.ExecContext(ctx, sqlStatement)
	if err != nil {
		ms.logger.Errorf("MSSQL: Error deleting from original table for dedup: %v\n", err)
		_ = txn.Rollback()
		return
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s)
									SELECT %[3]s FROM (
										SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s"
									) AS _ where _rudder_staging_row_number = 1
									`, ms.Namespace, tableName, quotedColumnNames, stagingTableName, partitionKey)
	ms.logger.Infof("MSSQL: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.ExecContext(ctx, sqlStatement)

	if err != nil {
		ms.logger.Errorf("MSSQL: Error inserting into original table: %v\n", err)
		_ = txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		ms.logger.Errorf("MSSQL: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		_ = txn.Rollback()
		return
	}

	ms.logger.Infof("MSSQL: Complete load for table:%s", tableName)
	return
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
	identifyStagingTable, err := ms.loadTable(ctx, warehouseutils.IdentifiesTable, ms.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
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
		ms.logger.Errorf("MSSQL: Error deleting from original table for dedup: %v\n", err)
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
    EXEC('CREATE SCHEMA [%s]');
`, ms.Namespace, ms.Namespace)
	ms.logger.Infof("MSSQL: Creating schema name in mssql for MSSQL:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.DB.ExecContext(ctx, sqlStatement)
	if err == io.EOF {
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
			)
`,
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

func (ms *MSSQL) CrashRecover(ctx context.Context) {
	ms.dropDanglingStagingTables(ctx)
}

func (ms *MSSQL) dropDanglingStagingTables(ctx context.Context) bool {
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
		ms.logger.Errorf("WH: MSSQL: Error dropping dangling staging tables in MSSQL: %v\nQuery: %s\n", err, sqlStatement)
		return false
	}
	defer func() { _ = rows.Close() }()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			panic(fmt.Errorf("failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		panic(fmt.Errorf("iterating result from query: %s\nwith Error : %w", sqlStatement, err))
	}
	ms.logger.Infof("WH: MSSQL: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := ms.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, ms.Namespace, stagingTableName))
		if err != nil {
			ms.logger.Errorf("WH: MSSQL:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

// FetchSchema queries mssql and returns the schema associated with provided namespace
func (ms *MSSQL) FetchSchema(ctx context.Context) (model.Schema, model.Schema, error) {
	schema := make(model.Schema)
	unrecognizedSchema := make(model.Schema)

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
	rows, err := ms.DB.QueryContext(ctx, sqlStatement,
		sql.Named("schema", ms.Namespace),
		sql.Named("prefix", fmt.Sprintf("%s%%", warehouseutils.StagingTablePrefix(provider))),
	)
	if errors.Is(err, io.EOF) {
		return schema, unrecognizedSchema, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string

		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := mssqlDataTypesMapToRudder[columnType]; ok {
			schema[tableName][columnName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tableName]; !ok {
				unrecognizedSchema[tableName] = make(model.TableSchema)
			}
			unrecognizedSchema[tableName][columnName] = warehouseutils.MissingDatatype

			warehouseutils.WHCounterStat(warehouseutils.RudderMissingDatatype, &ms.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, unrecognizedSchema, nil
}

func (ms *MSSQL) LoadUserTables(ctx context.Context) map[string]error {
	return ms.loadUserTables(ctx)
}

func (ms *MSSQL) LoadTable(ctx context.Context, tableName string) error {
	_, err := ms.loadTable(ctx, tableName, ms.Uploader.GetTableSchemaInUpload(tableName), false)
	return err
}

func (ms *MSSQL) Cleanup(ctx context.Context) {
	if ms.DB != nil {
		// extra check aside dropStagingTable(table)
		ms.dropDanglingStagingTables(ctx)
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

func (ms *MSSQL) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		ms.Namespace,
		tableName,
	)
	err = ms.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
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

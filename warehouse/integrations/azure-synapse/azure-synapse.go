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
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/rudderlabs/rudder-go-kit/stats"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	mssql "github.com/denisenkom/go-mssqldb"

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
	provider               = warehouseutils.AzureSynapse
	tableNameLimit         = 127
)

var errorsMappings []model.JobError

var rudderDataTypesMapToMssql = map[string]string{
	"int":      "bigint",
	"float":    "decimal(28,10)",
	"string":   "varchar(512)",
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

type AzureSynapse struct {
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
		sqlmw.WithKeyAndValues(as.defaultLogFields()),
		sqlmw.WithQueryTimeout(as.connectTimeout),
		sqlmw.WithSlowQueryThreshold(as.config.slowQueryThreshold),
	)
	return middleware, nil
}

func (as *AzureSynapse) connectionCredentials() *credentials {
	return &credentials{
		host:     warehouseutils.GetConfigValue(host, as.Warehouse),
		dbName:   warehouseutils.GetConfigValue(dbName, as.Warehouse),
		user:     warehouseutils.GetConfigValue(user, as.Warehouse),
		password: warehouseutils.GetConfigValue(password, as.Warehouse),
		port:     warehouseutils.GetConfigValue(port, as.Warehouse),
		sslMode:  warehouseutils.GetConfigValue(sslMode, as.Warehouse),
		timeout:  as.connectTimeout,
	}
}

func (as *AzureSynapse) defaultLogFields() []any {
	return []any{
		logfield.SourceID, as.Warehouse.Source.ID,
		logfield.SourceType, as.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, as.Warehouse.Destination.ID,
		logfield.DestinationType, as.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, as.Warehouse.WorkspaceID,
		logfield.Namespace, as.Namespace,
	}
}

func columnsWithDataTypes(columns model.TableSchema, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToMssql[dataType]))
	}
	return strings.Join(arr, ",")
}

func (*AzureSynapse) IsEmpty(context.Context, model.Warehouse) (empty bool, err error) {
	return
}

func (as *AzureSynapse) loadTable(ctx context.Context, tableName string, tableSchemaInUpload model.TableSchema, skipTempTableDelete bool) (stagingTableName string, err error) {
	as.logger.Infof("AZ: Starting load for table:%s", tableName)

	previousColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(as.Uploader.GetTableSchemaInWarehouse(tableName))
	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

	var extraColumns []string
	for _, column := range previousColumnKeys {
		if !slices.Contains(sortedColumnKeys, column) {
			extraColumns = append(extraColumns, column)
		}
	}
	fileNames, err := as.LoadFileDownLoader.Download(ctx, tableName)
	defer misc.RemoveFilePaths(fileNames...)
	if err != nil {
		return
	}

	// create temporary table
	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	// prepared stmts cannot be used to create temp objects here. Will work in a txn, but will be purged after commit.
	// https://github.com/denisenkom/go-mssqldb/issues/149, https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175528(v=sql.105)?redirectedfrom=MSDN
	// sqlStatement := fmt.Sprintf(`CREATE  TABLE ##%[2]s like %[1]s.%[3]s`, AZ.Namespace, stagingTableName, tableName)
	// Hence falling back to creating normal tables
	sqlStatement := fmt.Sprintf(`select top 0 * into %[1]s.%[2]s from %[1]s.%[3]s`, as.Namespace, stagingTableName, tableName)

	as.logger.Debugf("AZ: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	if err != nil {
		as.logger.Errorf("AZ: Error creating temporary table for table:%s: %v\n", tableName, err)
		return
	}

	txn, err := as.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		as.logger.Errorf("AZ: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		return
	}

	if !skipTempTableDelete {
		defer as.dropStagingTable(ctx, stagingTableName)
	}

	stmt, err := txn.PrepareContext(ctx, mssql.CopyIn(as.Namespace+"."+stagingTableName, mssql.BulkOptions{CheckConstraints: false}, append(sortedColumnKeys, extraColumns...)...))
	if err != nil {
		as.logger.Errorf("AZ: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			as.logger.Errorf("AZ: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			as.logger.Errorf("AZ: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			gzipFile.Close()
			return

		}
		csvReader := csv.NewReader(gzipReader)
		var csvRowsProcessedCount int
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					as.logger.Debugf("AZ: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
					break
				}
				as.logger.Errorf("AZ: Error while reading csv file %s for loading in staging table:%s: %v", objectFileName, stagingTableName, err)
				txn.Rollback()
				return
			}
			if len(sortedColumnKeys) != len(record) {
				err = fmt.Errorf(`load file CSV columns for a row mismatch number found in upload schema. Columns in CSV row: %d, Columns in upload schema of table-%s: %d. Processed rows in csv file until mismatch: %d`, len(record), tableName, len(sortedColumnKeys), csvRowsProcessedCount)
				as.logger.Error(err)
				txn.Rollback()
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
					as.logger.Debugf("AZ : Found nil value for type : %s, column : %s", valueType, sortedColumnKeys[index])
					finalColumnValues = append(finalColumnValues, nil)
					continue
				}
				strValue := value.(string)
				switch valueType {
				case "int":
					var convertedValue int
					if convertedValue, err = strconv.Atoi(strValue); err != nil {
						as.logger.Errorf("AZ : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
				case "float":
					var convertedValue float64
					if convertedValue, err = strconv.ParseFloat(strValue, 64); err != nil {
						as.logger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
				case "datetime":
					var convertedValue time.Time
					// TODO : handling milli?
					if convertedValue, err = time.Parse(time.RFC3339, strValue); err != nil {
						as.logger.Errorf("AZ : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
						finalColumnValues = append(finalColumnValues, nil)
					} else {
						finalColumnValues = append(finalColumnValues, convertedValue)
					}
					// TODO : handling all cases?
				case "boolean":
					var convertedValue bool
					if convertedValue, err = strconv.ParseBool(strValue); err != nil {
						as.logger.Errorf("AZ : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
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
						as.logger.Debug("diacritics " + strValue)
						byteArr = str2ucs2(strValue)
						// This is needed as with above operation every character occupies 2 bytes
						if len(byteArr) > mssqlStringLengthLimit {
							byteArr = byteArr[:mssqlStringLengthLimit]
						}
						finalColumnValues = append(finalColumnValues, byteArr)
					} else {
						as.logger.Debug("non-diacritic : " + strValue)
						finalColumnValues = append(finalColumnValues, strValue)
					}
				default:
					finalColumnValues = append(finalColumnValues, value)
				}
			}
			// This is needed for the copyIn to proceed successfully for azure synapse else will face below err for missing old columns
			// mssql: Column count in target table does not match column count specified in input.
			// If BCP command, ensure format file column count matches destination table. If SSIS data import, check column mappings are consistent with target.
			for range extraColumns {
				finalColumnValues = append(finalColumnValues, nil)
			}
			_, err = stmt.ExecContext(ctx, finalColumnValues...)
			if err != nil {
				as.logger.Errorf("AZ: Error in exec statement for loading in staging table:%s: %v", stagingTableName, err)
				txn.Rollback()
				return
			}
			csvRowsProcessedCount++
		}
		gzipReader.Close()
		gzipFile.Close()
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		txn.Rollback()
		as.logger.Errorf("AZ: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
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
		additionalJoinClause = fmt.Sprintf(`AND _source.%[3]s = "%[1]s"."%[2]s"."%[3]s" AND _source.%[4]s = "%[1]s"."%[2]s"."%[4]s"`, as.Namespace, tableName, "table_name", "column_name")
	}
	sqlStatement = fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" FROM "%[1]s"."%[3]s" as  _source where (_source.%[4]s = "%[1]s"."%[2]s"."%[4]s" %[5]s)`, as.Namespace, tableName, stagingTableName, primaryKey, additionalJoinClause)
	as.logger.Infof("AZ: Deduplicate records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.ExecContext(ctx, sqlStatement)
	if err != nil {
		as.logger.Errorf("AZ: Error deleting from original table for dedup: %v\n", err)
		txn.Rollback()
		return
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, as.Namespace, tableName, quotedColumnNames, stagingTableName, partitionKey)
	as.logger.Infof("AZ: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.ExecContext(ctx, sqlStatement)

	if err != nil {
		as.logger.Errorf("AZ: Error inserting into original table: %v\n", err)
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		as.logger.Errorf("AZ: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		return
	}

	as.logger.Infof("AZ: Complete load for table:%s", tableName)
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

func (as *AzureSynapse) loadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	as.logger.Infof("AZ: Starting load for identifies and users tables\n")
	identifyStagingTable, err := as.loadTable(ctx, warehouseutils.IdentifiesTable, as.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
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
							order by X.received_at desc
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
		as.logger.Errorf("AZ: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  %[3]s`, as.Namespace, warehouseutils.UsersTable, as.Namespace+"."+stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	as.logger.Infof("AZ: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.ExecContext(ctx, sqlStatement)

	if err != nil {
		as.logger.Errorf("AZ: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		as.logger.Errorf("AZ: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
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
    EXEC('CREATE SCHEMA [%s]');
`, as.Namespace, as.Namespace)
	as.logger.Infof("SYNAPSE: Creating schema name in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.ExecContext(ctx, sqlStatement)
	if err == io.EOF {
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
			)
`,
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
		queryBuilder.WriteString(fmt.Sprintf(` %q %s,`, columnInfo.Name, rudderDataTypesMapToMssql[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	as.logger.Infof("AZ: Adding columns for destinationID: %s, tableName: %s with query: %v", as.Warehouse.Destination.ID, tableName, query)
	_, err = as.DB.ExecContext(ctx, query)
	return
}

func (*AzureSynapse) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
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

func (as *AzureSynapse) CrashRecover(ctx context.Context) {
	as.dropDanglingStagingTables(ctx)
}

func (as *AzureSynapse) dropDanglingStagingTables(ctx context.Context) bool {
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
		as.logger.Errorf("WH: SYNAPSE: Error dropping dangling staging tables in synapse: %v\nQuery: %s\n", err, sqlStatement)
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
	as.logger.Infof("WH: SYNAPSE: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := as.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, as.Namespace, stagingTableName))
		if err != nil {
			as.logger.Errorf("WH: SYNAPSE:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

// FetchSchema returns the schema of the warehouse
func (as *AzureSynapse) FetchSchema(ctx context.Context) (model.Schema, model.Schema, error) {
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
	rows, err := as.DB.QueryContext(
		ctx,
		sqlStatement,
		sql.Named("schema", as.Namespace),
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

			warehouseutils.WHCounterStat(warehouseutils.RudderMissingDatatype, &as.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, unrecognizedSchema, nil
}

func (as *AzureSynapse) LoadUserTables(ctx context.Context) map[string]error {
	return as.loadUserTables(ctx)
}

func (as *AzureSynapse) LoadTable(ctx context.Context, tableName string) error {
	_, err := as.loadTable(ctx, tableName, as.Uploader.GetTableSchemaInUpload(tableName), false)
	return err
}

func (as *AzureSynapse) Cleanup(ctx context.Context) {
	if as.DB != nil {
		// extra check aside dropStagingTable(table)
		as.dropDanglingStagingTables(ctx)
		as.DB.Close()
	}
}

func (*AzureSynapse) LoadIdentityMergeRulesTable(context.Context) (err error) {
	return
}

func (*AzureSynapse) LoadIdentityMappingsTable(context.Context) (err error) {
	return
}

func (*AzureSynapse) DownloadIdentityRules(context.Context, *misc.GZipWriter) (err error) {
	return
}

func (as *AzureSynapse) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		as.Namespace,
		tableName,
	)
	err = as.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
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

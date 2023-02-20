package azuresynapse

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var pkgLogger logger.Logger

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
	provider               = warehouseutils.AZURE_SYNAPSE
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

type HandleT struct {
	DB             *sql.DB
	Namespace      string
	ObjectStorage  string
	Warehouse      warehouseutils.Warehouse
	Uploader       warehouseutils.UploaderI
	ConnectTimeout time.Duration
}

type credentialsT struct {
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

func connect(cred credentialsT) (*sql.DB, error) {
	// Create connection string
	// url := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=%s;TrustServerCertificate=true", cred.host, cred.user, cred.password, cred.port, cred.dbName, cred.sslMode)
	// Encryption options : disable, false, true.  https://github.com/denisenkom/go-mssqldb
	// TrustServerCertificate=true ; all options(disable, false, true) work with this
	// if rds.forcessl=1; disable option doesn't work. true, false works alongside TrustServerCertificate=true
	//		https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Concepts.General.SSL.Using.html
	// more combination explanations here: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/connection-string-keywords-and-data-source-names-dsns?view=sql-server-ver15
	query := url.Values{}
	query.Add("database", cred.dbName)
	query.Add("encrypt", cred.sslMode)
	if cred.timeout > 0 {
		query.Add("dial timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}
	query.Add("TrustServerCertificate", "true")
	port, err := strconv.Atoi(cred.port)
	if err != nil {
		pkgLogger.Errorf("Error parsing synapse connection port : %v", err)
		return nil, err
	}
	connUrl := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cred.user, cred.password),
		Host:     net.JoinHostPort(cred.host, strconv.Itoa(port)),
		RawQuery: query.Encode(),
	}
	pkgLogger.Debugf("synapse connection string : %s", connUrl.String())
	var db *sql.DB
	if db, err = sql.Open("sqlserver", connUrl.String()); err != nil {
		return nil, fmt.Errorf("synapse connection error : (%v)", err)
	}
	return db, nil
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("synapse")
}

func (as *HandleT) getConnectionCredentials() credentialsT {
	return credentialsT{
		host:     warehouseutils.GetConfigValue(host, as.Warehouse),
		dbName:   warehouseutils.GetConfigValue(dbName, as.Warehouse),
		user:     warehouseutils.GetConfigValue(user, as.Warehouse),
		password: warehouseutils.GetConfigValue(password, as.Warehouse),
		port:     warehouseutils.GetConfigValue(port, as.Warehouse),
		sslMode:  warehouseutils.GetConfigValue(sslMode, as.Warehouse),
		timeout:  as.ConnectTimeout,
	}
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, rudderDataTypesMapToMssql[dataType]))
	}
	return strings.Join(arr, ",")
}

func (*HandleT) IsEmpty(_ warehouseutils.Warehouse) (empty bool, err error) {
	return
}

func (as *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objects := as.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	storageProvider := warehouseutils.ObjectStorageType(as.Warehouse.Destination.DestinationDefinition.Name, as.Warehouse.Destination.Config, as.Uploader.UseRudderStorage())
	downloader, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           as.Warehouse.Destination.Config,
			UseRudderStorage: as.Uploader.UseRudderStorage(),
			WorkspaceID:      as.Warehouse.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		pkgLogger.Errorf("AZ: Error in setting up a downloader for destinationID : %s Error : %v", as.Warehouse.Destination.ID, err)
		return nil, err
	}
	var fileNames []string
	for _, object := range objects {
		objectName, err := warehouseutils.GetObjectName(object.Location, as.Warehouse.Destination.Config, as.ObjectStorage)
		if err != nil {
			pkgLogger.Errorf("AZ: Error in converting object location to object key for table:%s: %s,%v", tableName, object.Location, err)
			return nil, err
		}
		dirName := fmt.Sprintf(`/%s/`, misc.RudderWarehouseLoadUploadsTmp)
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			pkgLogger.Errorf("AZ: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, as.Warehouse.Destination.DestinationDefinition.Name, as.Warehouse.Destination.ID, time.Now().Unix()) + objectName
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			pkgLogger.Errorf("AZ: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, object.Location, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			pkgLogger.Errorf("AZ: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		err = downloader.Download(context.TODO(), objectFile, objectName)
		if err != nil {
			pkgLogger.Errorf("AZ: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			pkgLogger.Errorf("AZ: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil
}

func (as *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	pkgLogger.Infof("AZ: Starting load for table:%s", tableName)

	previousColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(as.Uploader.GetTableSchemaInWarehouse(tableName))
	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	sortedColumnString := strings.Join(sortedColumnKeys, ", ")

	var extraColumns []string
	for _, column := range previousColumnKeys {
		if !misc.Contains(sortedColumnKeys, column) {
			extraColumns = append(extraColumns, column)
		}
	}
	fileNames, err := as.DownloadLoadFiles(tableName)
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

	pkgLogger.Debugf("AZ: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = as.DB.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("AZ: Error creating temporary table for table:%s: %v\n", tableName, err)
		return
	}

	txn, err := as.DB.Begin()
	if err != nil {
		pkgLogger.Errorf("AZ: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		return
	}

	if !skipTempTableDelete {
		defer as.dropStagingTable(stagingTableName)
	}

	stmt, err := txn.Prepare(mssql.CopyIn(as.Namespace+"."+stagingTableName, mssql.BulkOptions{CheckConstraints: false}, append(sortedColumnKeys, extraColumns...)...))
	if err != nil {
		pkgLogger.Errorf("AZ: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			pkgLogger.Errorf("AZ: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			pkgLogger.Errorf("AZ: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
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
					pkgLogger.Debugf("AZ: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
					break
				}
				pkgLogger.Errorf("AZ: Error while reading csv file %s for loading in staging table:%s: %v", objectFileName, stagingTableName, err)
				txn.Rollback()
				return
			}
			if len(sortedColumnKeys) != len(record) {
				err = fmt.Errorf(`load file CSV columns for a row mismatch number found in upload schema. Columns in CSV row: %d, Columns in upload schema of table-%s: %d. Processed rows in csv file until mismatch: %d`, len(record), tableName, len(sortedColumnKeys), csvRowsProcessedCount)
				pkgLogger.Error(err)
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
					pkgLogger.Debugf("AZ : Found nil value for type : %s, column : %s", valueType, sortedColumnKeys[index])
					finalColumnValues = append(finalColumnValues, nil)
					continue
				}
				strValue := value.(string)
				switch valueType {
				case "int":
					{
						var convertedValue int
						if convertedValue, err = strconv.Atoi(strValue); err != nil {
							pkgLogger.Errorf("AZ : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
							finalColumnValues = append(finalColumnValues, nil)
						} else {
							finalColumnValues = append(finalColumnValues, convertedValue)
						}

					}
				case "float":
					{
						var convertedValue float64
						if convertedValue, err = strconv.ParseFloat(strValue, 64); err != nil {
							pkgLogger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
							finalColumnValues = append(finalColumnValues, nil)
						} else {
							finalColumnValues = append(finalColumnValues, convertedValue)
						}
					}
				case "datetime":
					{
						var convertedValue time.Time
						// TODO : handling milli?
						if convertedValue, err = time.Parse(time.RFC3339, strValue); err != nil {
							pkgLogger.Errorf("AZ : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
							finalColumnValues = append(finalColumnValues, nil)
						} else {
							finalColumnValues = append(finalColumnValues, convertedValue)
						}
						// TODO : handling all cases?
					}
				case "boolean":
					{
						var convertedValue bool
						if convertedValue, err = strconv.ParseBool(strValue); err != nil {
							pkgLogger.Errorf("AZ : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
							finalColumnValues = append(finalColumnValues, nil)
						} else {
							finalColumnValues = append(finalColumnValues, convertedValue)
						}
					}
				case "string":
					{
						// This is needed to enable diacritic support Ex: Ü,ç Ç,©,∆,ß,á,ù,ñ,ê
						// A substitute to this PR; https://github.com/denisenkom/go-mssqldb/pull/576/files
						// An alternate to this approach is to use nvarchar(instead of varchar)
						if len(strValue) > mssqlStringLengthLimit {
							strValue = strValue[:mssqlStringLengthLimit]
						}
						var byteArr []byte
						if hasDiacritics(strValue) {
							pkgLogger.Debug("diacritics " + strValue)
							byteArr = str2ucs2(strValue)
							// This is needed as with above operation every character occupies 2 bytes
							if len(byteArr) > mssqlStringLengthLimit {
								byteArr = byteArr[:mssqlStringLengthLimit]
							}
							finalColumnValues = append(finalColumnValues, byteArr)
						} else {
							pkgLogger.Debug("non-diacritic : " + strValue)
							finalColumnValues = append(finalColumnValues, strValue)
						}
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
			_, err = stmt.Exec(finalColumnValues...)
			if err != nil {
				pkgLogger.Errorf("AZ: Error in exec statement for loading in staging table:%s: %v", stagingTableName, err)
				txn.Rollback()
				return
			}
			csvRowsProcessedCount++
		}
		gzipReader.Close()
		gzipFile.Close()
	}

	_, err = stmt.Exec()
	if err != nil {
		txn.Rollback()
		pkgLogger.Errorf("AZ: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
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
	pkgLogger.Infof("AZ: Deduplicate records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("AZ: Error deleting from original table for dedup: %v\n", err)
		txn.Rollback()
		return
	}
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, as.Namespace, tableName, sortedColumnString, stagingTableName, partitionKey)
	pkgLogger.Infof("AZ: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("AZ: Error inserting into original table: %v\n", err)
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		pkgLogger.Errorf("AZ: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		return
	}

	pkgLogger.Infof("AZ: Complete load for table:%s", tableName)
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

func (as *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("AZ: Starting load for identifies and users tables\n")
	identifyStagingTable, err := as.loadTable(warehouseutils.IdentifiesTable, as.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
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
	defer as.dropStagingTable(stagingTableName)
	defer as.dropStagingTable(unionStagingTableName)
	defer as.dropStagingTable(identifyStagingTable)

	userColMap := as.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		caseSubQuery := fmt.Sprintf(`case
						  when (exists(select 1)) then (
						  	select top 1 %[1]s from %[2]s
						  	where x.id = %[2]s.id
							  and %[1]s is not null
							order by X.received_at desc
							)
						  end as %[1]s`, colName, as.Namespace+"."+unionStagingTableName)
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

	pkgLogger.Debugf("AZ: Creating staging table for union of users table with identify staging table: %s\n", sqlStatement)
	_, err = as.DB.Exec(sqlStatement)
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

	pkgLogger.Debugf("AZ: Creating staging table for users: %s\n", sqlStatement)
	_, err = as.DB.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("AZ: Error Creating staging table for users: %s\n", sqlStatement)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// BEGIN TRANSACTION
	tx, err := as.DB.Begin()
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" FROM %[3]s _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, as.Namespace, warehouseutils.UsersTable, as.Namespace+"."+stagingTableName, primaryKey)
	pkgLogger.Infof("AZ: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("AZ: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  %[3]s`, as.Namespace, warehouseutils.UsersTable, as.Namespace+"."+stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	pkgLogger.Infof("AZ: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("AZ: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("AZ: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (*HandleT) DeleteBy([]string, warehouseutils.DeleteByParams) error {
	return fmt.Errorf(warehouseutils.NotImplementedErrorCode)
}

func (as *HandleT) CreateSchema() (err error) {
	sqlStatement := fmt.Sprintf(`IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'%s' )
    EXEC('CREATE SCHEMA [%s]');
`, as.Namespace, as.Namespace)
	pkgLogger.Infof("SYNAPSE: Creating schema name in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.Exec(sqlStatement)
	if err == io.EOF {
		return nil
	}
	return
}

func (as *HandleT) dropStagingTable(stagingTableName string) {
	pkgLogger.Infof("AZ: dropping table %+v\n", stagingTableName)
	_, err := as.DB.Exec(fmt.Sprintf(`IF OBJECT_ID ('%[1]s','U') IS NOT NULL DROP TABLE %[1]s;`, as.Namespace+"."+stagingTableName))
	if err != nil {
		pkgLogger.Errorf("AZ:  Error dropping staging table %s in synapse: %v", as.Namespace+"."+stagingTableName, err)
	}
}

func (as *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
	CREATE TABLE %[1]s ( %v )`, name, columnsWithDataTypes(columns, ""))

	pkgLogger.Infof("AZ: Creating table in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.Exec(sqlStatement)
	return
}

func (as *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// Search paths doesn't exist unlike Postgres, default is dbo. Hence, use namespace wherever possible
	err = as.createTable(as.Namespace+"."+tableName, columnMap)
	return err
}

func (as *HandleT) DropTable(tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	pkgLogger.Infof("AZ: Dropping table in synapse for AZ:%s : %v", as.Warehouse.Destination.ID, sqlStatement)
	_, err = as.DB.Exec(fmt.Sprintf(sqlStatement, as.Namespace, tableName))
	return
}

func (as *HandleT) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
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
		queryBuilder.WriteString(fmt.Sprintf(` %s %s,`, columnInfo.Name, rudderDataTypesMapToMssql[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	pkgLogger.Infof("AZ: Adding columns for destinationID: %s, tableName: %s with query: %v", as.Warehouse.Destination.ID, tableName, query)
	_, err = as.DB.Exec(query)
	return
}

func (*HandleT) AlterColumn(_, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

func (as *HandleT) TestConnection(warehouse warehouseutils.Warehouse) (err error) {
	as.Warehouse = warehouse
	as.DB, err = connect(as.getConnectionCredentials())
	if err != nil {
		return
	}
	defer as.DB.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), as.ConnectTimeout)
	defer cancel()

	err = as.DB.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", as.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

func (as *HandleT) Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (err error) {
	as.Warehouse = warehouse
	as.Namespace = warehouse.Namespace
	as.Uploader = uploader
	as.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.AZURE_SYNAPSE, warehouse.Destination.Config, as.Uploader.UseRudderStorage())

	as.DB, err = connect(as.getConnectionCredentials())
	return err
}

func (as *HandleT) CrashRecover(warehouse warehouseutils.Warehouse) (err error) {
	as.Warehouse = warehouse
	as.Namespace = warehouse.Namespace
	as.DB, err = connect(as.getConnectionCredentials())
	if err != nil {
		return err
	}
	defer as.DB.Close()
	as.dropDanglingStagingTables()
	return
}

func (as *HandleT) dropDanglingStagingTables() bool {
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
	rows, err := as.DB.Query(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("WH: SYNAPSE: Error dropping dangling staging tables in synapse: %v\nQuery: %s\n", err, sqlStatement)
		return false
	}
	defer rows.Close()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			panic(fmt.Errorf("failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	pkgLogger.Infof("WH: SYNAPSE: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := as.DB.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, as.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: SYNAPSE:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

// FetchSchema queries SYNAPSE and returns the schema associated with provided namespace
func (as *HandleT) FetchSchema(warehouse warehouseutils.Warehouse) (schema, unrecognizedSchema warehouseutils.SchemaT, err error) {
	as.Warehouse = warehouse
	as.Namespace = warehouse.Namespace
	dbHandle, err := connect(as.getConnectionCredentials())
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	unrecognizedSchema = make(warehouseutils.SchemaT)

	sqlStatement := fmt.Sprintf(`
			SELECT
			  table_name,
			  column_name,
			  data_type
			FROM
			  INFORMATION_SCHEMA.COLUMNS
			WHERE
			  table_schema = '%s'
			  and table_name not like '%s'
		`,
		as.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	rows, err := dbHandle.Query(sqlStatement)

	if err != nil && err != io.EOF {
		pkgLogger.Errorf("AZ: Error in fetching schema from synapse destination:%v, query: %v", as.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == io.EOF {
		pkgLogger.Infof("AZ: No rows, while fetching schema from  destination:%v, query: %v", as.Warehouse.Identifier, sqlStatement)
		return schema, unrecognizedSchema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pkgLogger.Errorf("AZ: Error in processing fetched schema from synapse destination:%v", as.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := mssqlDataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tName]; !ok {
				unrecognizedSchema[tName] = make(map[string]string)
			}
			unrecognizedSchema[tName][cName] = warehouseutils.MISSING_DATATYPE

			warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &as.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType}).Count(1)
		}
	}
	return
}

func (as *HandleT) LoadUserTables() map[string]error {
	return as.loadUserTables()
}

func (as *HandleT) LoadTable(tableName string) error {
	_, err := as.loadTable(tableName, as.Uploader.GetTableSchemaInUpload(tableName), false)
	return err
}

func (as *HandleT) Cleanup() {
	if as.DB != nil {
		// extra check aside dropStagingTable(table)
		as.dropDanglingStagingTables()
		as.DB.Close()
	}
}

func (*HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (*HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

func (*HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (as *HandleT) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
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

func (as *HandleT) Connect(warehouse warehouseutils.Warehouse) (client.Client, error) {
	as.Warehouse = warehouse
	as.Namespace = warehouse.Namespace
	dbHandle, err := connect(as.getConnectionCredentials())
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (as *HandleT) LoadTestTable(_, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO "%s"."%s" (%v) VALUES (%s)`,
		as.Namespace,
		tableName,
		fmt.Sprintf(`"%s", "%s"`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = as.DB.Exec(sqlStatement)
	return
}

func (as *HandleT) SetConnectionTimeout(timeout time.Duration) {
	as.ConnectTimeout = timeout
}

func (as *HandleT) ErrorMappings() []model.JobError {
	return errorsMappings
}

package mssql

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

var (
	stagingTablePrefix   string
	pkgLogger            logger.LoggerI
	diacriticLengthLimit = diacriticLimit()
)

const (
	host                   = "host"
	dbName                 = "database"
	user                   = "user"
	password               = "password"
	port                   = "port"
	sslMode                = "sslMode"
	mssqlStringLengthLimit = 512
)

func diacriticLimit() int {
	if mssqlStringLengthLimit%2 != 0 {
		return mssqlStringLengthLimit - 1
	} else {
		return mssqlStringLengthLimit
	}
}

const PROVIDER = "MSSQL"

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

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

type credentialsT struct {
	host     string
	dbName   string
	user     string
	password string
	port     string
	sslMode  string
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
	//url := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;encrypt=%s;TrustServerCertificate=true", cred.host, cred.user, cred.password, cred.port, cred.dbName, cred.sslMode)
	//Encryption options : disable, false, true.  https://github.com/denisenkom/go-mssqldb
	//TrustServerCertificate=true ; all options(disable, false, true) work with this
	//if rds.forcessl=1; disable option doesnt work. true, false works alongside TrustServerCertificate=true
	//		https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/SQLServer.Concepts.General.SSL.Using.html
	//more combination explanations here: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/connection-string-keywords-and-data-source-names-dsns?view=sql-server-ver15
	query := url.Values{}
	query.Add("database", cred.dbName)
	query.Add("encrypt", cred.sslMode)
	query.Add("TrustServerCertificate", "true")
	port, err := strconv.Atoi(cred.port)
	if err != nil {
		pkgLogger.Errorf("Error parsing mssql connection port : %v", err)
		return nil, err
	}
	connUrl := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cred.user, cred.password),
		Host:     fmt.Sprintf("%s:%d", cred.host, port),
		RawQuery: query.Encode(),
	}
	pkgLogger.Debugf("mssql connection string : %s", connUrl.String())
	var db *sql.DB
	if db, err = sql.Open("sqlserver", connUrl.String()); err != nil {
		return nil, fmt.Errorf("mssql connection error : (%v)", err)
	}
	return db, nil
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("mssql")
}

func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
}

func (ms *HandleT) getConnectionCredentials() credentialsT {
	return credentialsT{
		host:     warehouseutils.GetConfigValue(host, ms.Warehouse),
		dbName:   warehouseutils.GetConfigValue(dbName, ms.Warehouse),
		user:     warehouseutils.GetConfigValue(user, ms.Warehouse),
		password: warehouseutils.GetConfigValue(password, ms.Warehouse),
		port:     warehouseutils.GetConfigValue(port, ms.Warehouse),
		sslMode:  warehouseutils.GetConfigValue(sslMode, ms.Warehouse),
	}
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, rudderDataTypesMapToMssql[dataType]))
	}
	return strings.Join(arr, ",")
}

func (bq *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

func (ms *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objects := ms.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	storageProvider := warehouseutils.ObjectStorageType(ms.Warehouse.Destination.DestinationDefinition.Name, ms.Warehouse.Destination.Config, ms.Uploader.UseRudderStorage())
	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           ms.Warehouse.Destination.Config,
			UseRudderStorage: ms.Uploader.UseRudderStorage(),
		}),
	})
	if err != nil {
		pkgLogger.Errorf("MS: Error in setting up a downloader for destionationID : %s Error : %v", ms.Warehouse.Destination.ID, err)
		return nil, err
	}
	var fileNames []string
	for _, object := range objects {
		objectName, err := warehouseutils.GetObjectName(object.Location, ms.Warehouse.Destination.Config, ms.ObjectStorage)
		if err != nil {
			pkgLogger.Errorf("MS: Error in converting object location to object key for table:%s: %s,%v", tableName, object.Location, err)
			return nil, err
		}
		dirName := "/rudder-warehouse-load-uploads-tmp/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			pkgLogger.Errorf("MS: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, ms.Warehouse.Destination.DestinationDefinition.Name, ms.Warehouse.Destination.ID, time.Now().Unix()) + objectName
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			pkgLogger.Errorf("MS: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, object.Location, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			pkgLogger.Errorf("MS: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		err = downloader.Download(objectFile, objectName)
		if err != nil {
			pkgLogger.Errorf("MS: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			pkgLogger.Errorf("MS: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil

}

func (ms *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	pkgLogger.Infof("MS: Starting load for table:%s", tableName)

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	sortedColumnString := strings.Join(sortedColumnKeys, ", ")

	fileNames, err := ms.DownloadLoadFiles(tableName)
	defer misc.RemoveFilePaths(fileNames...)
	if err != nil {
		return
	}

	txn, err := ms.Db.Begin()
	if err != nil {
		pkgLogger.Errorf("MS: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		return
	}
	// create temporary table
	stagingTableName = fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, tableName, strings.ReplaceAll(uuid.NewV4().String(), "-", ""))
	//prepared stmts cannot be used to create temp objects here. Will work in a txn, but will be purged after commit.
	//https://github.com/denisenkom/go-mssqldb/issues/149, https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms175528(v=sql.105)?redirectedfrom=MSDN
	//sqlStatement := fmt.Sprintf(`CREATE  TABLE ##%[2]s like %[1]s.%[3]s`, ms.Namespace, stagingTableName, tableName)
	//Hence falling back to creating normal tables
	sqlStatement := fmt.Sprintf(`select top 0 * into %[1]s.%[2]s from %[1]s.%[3]s`, ms.Namespace, stagingTableName, tableName)

	pkgLogger.Debugf("MS: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("MS: Error creating temporary table for table:%s: %v\n", tableName, err)
		txn.Rollback()
		return
	}
	if !skipTempTableDelete {
		defer ms.dropStagingTable(stagingTableName)
	}

	stmt, err := txn.Prepare(mssql.CopyIn(ms.Namespace+"."+stagingTableName, mssql.BulkOptions{CheckConstraints: false}, sortedColumnKeys...))
	if err != nil {
		pkgLogger.Errorf("MS: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		txn.Rollback()
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			pkgLogger.Errorf("MS: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			txn.Rollback()
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			pkgLogger.Errorf("MS: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			gzipFile.Close()
			txn.Rollback()
			return

		}
		csvReader := csv.NewReader(gzipReader)
		var csvRowsProcessedCount int
		for {
			var record []string
			record, err = csvReader.Read()
			if err != nil {
				if err == io.EOF {
					pkgLogger.Debugf("MS: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
					break
				} else {
					pkgLogger.Errorf("MS: Error while reading csv file %s for loading in staging table:%s: %v", objectFileName, stagingTableName, err)
					txn.Rollback()
					return
				}

			}
			if len(sortedColumnKeys) != len(record) {
				err = fmt.Errorf(`Load file CSV columns for a row mismatch number found in upload schema. Columns in CSV row: %d, Columns in upload schema of table-%s: %d. Processed rows in csv file until mismatch: %d`, len(record), tableName, len(sortedColumnKeys), csvRowsProcessedCount)
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
					pkgLogger.Debugf("MS : Found nil value for type : %s, column : %s", valueType, sortedColumnKeys[index])
					finalColumnValues = append(finalColumnValues, nil)
					continue
				}
				strValue := value.(string)
				switch valueType {
				case "int":
					{
						var convertedValue int
						if convertedValue, err = strconv.Atoi(strValue); err != nil {
							pkgLogger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
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
						//TODO : handling milli?
						if convertedValue, err = time.Parse(time.RFC3339, strValue); err != nil {
							pkgLogger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
							finalColumnValues = append(finalColumnValues, nil)
						} else {
							finalColumnValues = append(finalColumnValues, convertedValue)
						}
						//TODO : handling all cases?
					}
				case "boolean":
					{
						var convertedValue bool
						if convertedValue, err = strconv.ParseBool(strValue); err != nil {
							pkgLogger.Errorf("MS : Mismatch in datatype for type : %s, column : %s, value : %s, err : %v", valueType, sortedColumnKeys[index], strValue, err)
							finalColumnValues = append(finalColumnValues, nil)
						} else {
							finalColumnValues = append(finalColumnValues, convertedValue)
						}
					}
				case "string":
					{
						//This is needed to enable diacritic support Ex: Ü,ç Ç,©,∆,ß,á,ù,ñ,ê
						//A substitute to this PR; https://github.com/denisenkom/go-mssqldb/pull/576/files
						//An alternate to this approach is to use nvarchar(instead of varchar)
						if len(strValue) > mssqlStringLengthLimit {
							strValue = strValue[:mssqlStringLengthLimit]
						}
						byteArr := []byte("")
						if hasDiacritics(strValue) {
							pkgLogger.Debug("diacritics " + strValue)
							byteArr = str2ucs2(strValue)
							// This is needed as with above operation every character occupies 2 bytes
							if len(byteArr) > diacriticLengthLimit {
								byteArr = byteArr[:diacriticLengthLimit]
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

			_, err = stmt.Exec(finalColumnValues...)
			if err != nil {
				pkgLogger.Errorf("MS: Error in exec statement for loading in staging table:%s: %v", stagingTableName, err)
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
		pkgLogger.Errorf("MS: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
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
	pkgLogger.Infof("MS: Deduplicate records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("MS: Error deleting from original table for dedup: %v\n", err)
		txn.Rollback()
		return
	}
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, ms.Namespace, tableName, sortedColumnString, stagingTableName, partitionKey)
	pkgLogger.Infof("MS: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("MS: Error inserting into original table: %v\n", err)
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		pkgLogger.Errorf("MS: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		txn.Rollback()
		return
	}

	pkgLogger.Infof("MS: Complete load for table:%s", tableName)
	return
}

//Taken from https://github.com/denisenkom/go-mssqldb/blob/master/tds.go
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
		if utf8.RuneLen(rune(x)) > 1 {
			return true
		}
	}
	return false
}

func (ms *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("MS: Starting load for identifies and users tables\n")
	identifyStagingTable, err := ms.loadTable(warehouseutils.IdentifiesTable, ms.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(ms.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	unionStagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.NewV4().String(), "-", ""), "users_identifies_union"), 127)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.NewV4().String(), "-", ""), warehouseutils.UsersTable), 127)
	defer ms.dropStagingTable(stagingTableName)
	defer ms.dropStagingTable(unionStagingTableName)
	defer ms.dropStagingTable(identifyStagingTable)

	userColMap := ms.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		caseSubQuery := fmt.Sprintf(`case
						  when (exists(select 1)) then (
						  	select %[1]s from %[2]s
						  	where x.id = %[2]s.id
							  and %[1]s is not null
							  order by received_at desc
						  	OFFSET 0 ROWS
							FETCH NEXT 1 ROWS ONLY)
						  end as %[1]s`, colName, ms.Namespace+"."+unionStagingTableName)

		//IGNORE NULLS only supported in Azure SQL edge, in which case the query can be shortedened to below
		//https://docs.microsoft.com/en-us/sql/t-sql/functions/first-value-transact-sql?view=sql-server-ver15
		//caseSubQuery := fmt.Sprintf(`FIRST_VALUE(%[1]s) IGNORE NULLS OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	//TODO: skipped top level temporary table for now
	sqlStatement := fmt.Sprintf(`SELECT * into %[5]s FROM
												((
													SELECT id, %[4]s FROM %[2]s WHERE id in (SELECT user_id FROM %[3]s WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[4]s FROM %[3]s  WHERE user_id IS NOT NULL
												)) a
											`, ms.Namespace, ms.Namespace+"."+warehouseutils.UsersTable, ms.Namespace+"."+identifyStagingTable, strings.Join(userColNames, ","), ms.Namespace+"."+unionStagingTableName)

	pkgLogger.Debugf("MS: Creating staging table for union of users table with identify staging table: %s\n", sqlStatement)
	_, err = ms.Db.Exec(sqlStatement)
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

	pkgLogger.Debugf("MS: Creating staging table for users: %s\n", sqlStatement)
	_, err = ms.Db.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("MS: Error Creating staging table for users: %s\n", sqlStatement)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// BEGIN TRANSACTION
	tx, err := ms.Db.Begin()
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" FROM %[3]s _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, ms.Namespace, warehouseutils.UsersTable, ms.Namespace+"."+stagingTableName, primaryKey)
	pkgLogger.Infof("MS: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("MS: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  %[3]s`, ms.Namespace, warehouseutils.UsersTable, ms.Namespace+"."+stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	pkgLogger.Infof("MS: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("MS: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("MS: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (ms *HandleT) CreateSchema() (err error) {
	sqlStatement := fmt.Sprintf(`IF NOT EXISTS ( SELECT  * FROM  sys.schemas WHERE   name = N'%s' )
    EXEC('CREATE SCHEMA [%s]');
`, ms.Namespace, ms.Namespace)
	pkgLogger.Infof("MSSQL: Creating schema name in mssql for MS:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.Db.Exec(sqlStatement)
	if err == io.EOF {
		return nil
	}
	return
}

func (ms *HandleT) dropStagingTable(stagingTableName string) {
	pkgLogger.Infof("MS: dropping table %+v\n", stagingTableName)
	_, err := ms.Db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, ms.Namespace+"."+stagingTableName))
	if err != nil {
		pkgLogger.Errorf("MS:  Error dropping staging table %s in mssql: %v", ms.Namespace+"."+stagingTableName, err)
	}
}

func (ms *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`IF  NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
	CREATE TABLE %[1]s ( %v )`, name, columnsWithDataTypes(columns, ""))

	pkgLogger.Infof("MS: Creating table in mssql for MS:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.Db.Exec(sqlStatement)
	return
}

func (ms *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`IF NOT EXISTS (SELECT 1  FROM SYS.COLUMNS WHERE OBJECT_ID = OBJECT_ID(N'%[1]s') AND name = '%[2]s')
			ALTER TABLE %[1]s ADD %[2]s %[3]s`, tableName, columnName, rudderDataTypesMapToMssql[columnType])
	pkgLogger.Infof("MS: Adding column in mssql for MS:%s : %v", ms.Warehouse.Destination.ID, sqlStatement)
	_, err = ms.Db.Exec(sqlStatement)
	return
}

func (ms *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// Search paths doesnt exist unlike Postgres, default is dbo. Hence use namespace whereever possible
	err = ms.createTable(ms.Namespace+"."+tableName, columnMap)
	return err
}

func (ms *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	err = ms.addColumn(ms.Namespace+"."+tableName, columnName, columnType)
	return err
}

func (ms *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

func (ms *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	ms.Warehouse = warehouse
	ms.Db, err = connect(ms.getConnectionCredentials())
	if err != nil {
		return
	}
	defer ms.Db.Close()

	timeOut := 5 * time.Second

	ctx, cancel := context.WithTimeout(context.TODO(), timeOut)
	defer cancel()

	err = ms.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", timeOut/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

func (ms *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	ms.Warehouse = warehouse
	ms.Namespace = warehouse.Namespace
	ms.Uploader = uploader
	ms.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.MSSQL, warehouse.Destination.Config, ms.Uploader.UseRudderStorage())

	ms.Db, err = connect(ms.getConnectionCredentials())
	return err
}

func (ms *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	ms.Warehouse = warehouse
	ms.Namespace = warehouse.Namespace
	ms.Db, err = connect(ms.getConnectionCredentials())
	if err != nil {
		return err
	}
	defer ms.Db.Close()
	ms.dropDanglingStagingTables()
	return
}

func (ms *HandleT) dropDanglingStagingTables() bool {

	sqlStatement := fmt.Sprintf(`select table_name
								 from information_schema.tables
								 where table_schema = '%s' AND table_name like '%s';`, ms.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "%"))
	rows, err := ms.Db.Query(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("WH: MSSQL: Error dropping dangling staging tables in MSSQL: %v\nQuery: %s\n", err, sqlStatement)
		return false
	}
	defer rows.Close()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	pkgLogger.Infof("WH: MSSQL: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := ms.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, ms.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: MSSQL:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

// FetchSchema queries mssql and returns the schema associated with provided namespace
func (ms *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	ms.Warehouse = warehouse
	ms.Namespace = warehouse.Namespace
	dbHandle, err := connect(ms.getConnectionCredentials())
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	sqlStatement := fmt.Sprintf(`SELECT table_name, column_name, data_type
									FROM INFORMATION_SCHEMA.COLUMNS
									WHERE table_schema = '%s' and table_name not like '%s%s'`, ms.Namespace, stagingTablePrefix, "%")

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != io.EOF {
		pkgLogger.Errorf("MS: Error in fetching schema from mssql destination:%v, query: %v", ms.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == io.EOF {
		pkgLogger.Infof("MS: No rows, while fetching schema from  destination:%v, query: %v", ms.Warehouse.Identifier, sqlStatement)
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pkgLogger.Errorf("MS: Error in processing fetched schema from mssql destination:%v", ms.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := mssqlDataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
		}
	}
	return
}

func (ms *HandleT) LoadUserTables() map[string]error {
	return ms.loadUserTables()
}

func (ms *HandleT) LoadTable(tableName string) error {
	_, err := ms.loadTable(tableName, ms.Uploader.GetTableSchemaInUpload(tableName), false)
	return err
}

func (ms *HandleT) Cleanup() {
	if ms.Db != nil {
		//extra check aside dropStagingTable(table)
		ms.dropDanglingStagingTables()
		ms.Db.Close()
	}
}

func (ms *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (ms *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

func (ms *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (ms *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, ms.Namespace, tableName)
	err = ms.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`MS: Error getting total count in table %s:%s`, ms.Namespace, tableName)
	}
	return
}

func (ms *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	ms.Warehouse = warehouse
	ms.Namespace = warehouse.Namespace
	dbHandle, err := connect(ms.getConnectionCredentials())
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

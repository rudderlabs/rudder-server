package postgres

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	stagingTablePrefix            string
	pkgLogger                     logger.LoggerI
	skipComputingUserLatestTraits bool
)

const (
	host          = "host"
	dbName        = "database"
	user          = "user"
	password      = "password"
	port          = "port"
	sslMode       = "sslMode"
	serverCAPem   = "serverCA"
	clientSSLCert = "clientCert"
	clientSSLKey  = "clientKey"
	verifyCA      = "verify-ca"
)

const PROVIDER = "POSTGRES"

var rudderDataTypesMapToPostgres = map[string]string{
	"int":      "bigint",
	"float":    "numeric",
	"string":   "text",
	"datetime": "timestamptz",
	"boolean":  "boolean",
	"json":     "jsonb",
}

var postgresDataTypesMapToRudder = map[string]string{
	"integer":                  "int",
	"smallint":                 "int",
	"bigint":                   "int",
	"double precision":         "float",
	"numeric":                  "float",
	"real":                     "float",
	"text":                     "string",
	"varchar":                  "string",
	"char":                     "string",
	"timestamptz":              "datetime",
	"timestamp with time zone": "datetime",
	"timestamp":                "datetime",
	"boolean":                  "boolean",
	"jsonb":                    "json",
}

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	ObjectStorage string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

type credentialsT struct {
	host      string
	dbName    string
	user      string
	password  string
	port      string
	sslMode   string
	sslParams sslParamsT
}

type sslParamsT struct {
	serverCa   string
	clientCert string
	clientKey  string
	id         string
}

func (ssl *sslParamsT) saveToFileSystem() {
	///sslrootcert=server-ca.pem sslcert=client-cert.pem sslkey=client-key.pem
	if ssl.id != "" {
		return
	}
	ssl.id = uuid.Must(uuid.NewV4()).String()
	var err error
	sslBasePath := fmt.Sprintf("/tmp/ssl-files-%s", ssl.id)
	if err = os.MkdirAll(sslBasePath, 700); err != nil {
		panic(fmt.Sprintf("Error creating ssl-files root directory %s", err))
	}
	if err = ioutil.WriteFile(fmt.Sprintf("%s/server-ca.pem", sslBasePath), []byte(ssl.serverCa), 600); err != nil {
		panic(fmt.Sprintf("Error persisting server-ca.pem file to file system %s", err))
	}
	if err = ioutil.WriteFile(fmt.Sprintf("%s/client-cert.pem", sslBasePath), []byte(ssl.clientCert), 600); err != nil {
		panic(fmt.Sprintf("Error persisting client-cert.pem file to file system %s", err))
	}
	if err = ioutil.WriteFile(fmt.Sprintf("%s/client-key.pem", sslBasePath), []byte(ssl.clientKey), 600); err != nil {
		panic(fmt.Sprintf("Error persisting client-key.pem file to file system %s", err))
	}

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
	url := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v sslmode=%v",
		cred.user,
		cred.password,
		cred.host,
		cred.port,
		cred.dbName,
		cred.sslMode)
	if cred.sslMode == verifyCA {
		url = fmt.Sprintf("%s sslrootcert=%[2]s/server-ca.pem sslcert=%[2]s/client-cert.pem sslkey=%[2]s/client-key.pem", url, cred.sslParams.id)
	}

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("postgres connection error : (%v)", err)
	}
	return db, nil
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("postgres")
}

func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
	config.RegisterBoolConfigVariable(false, &skipComputingUserLatestTraits, true, "Warehouse.postgres.skipComputingUserLatestTraits")
}

func (pg *HandleT) getConnectionCredentials() credentialsT {
	sslMode := warehouseutils.GetConfigValue(sslMode, pg.Warehouse)
	var sslParams sslParamsT
	if sslMode == verifyCA {
		sslParams = sslParamsT{
			warehouseutils.GetConfigValue(serverCAPem, pg.Warehouse),
			warehouseutils.GetConfigValue(clientSSLCert, pg.Warehouse),
			warehouseutils.GetConfigValue(clientSSLKey, pg.Warehouse),
			"",
		}
		sslParams.saveToFileSystem()
	}

	return credentialsT{
		host:      warehouseutils.GetConfigValue(host, pg.Warehouse),
		dbName:    warehouseutils.GetConfigValue(dbName, pg.Warehouse),
		user:      warehouseutils.GetConfigValue(user, pg.Warehouse),
		password:  warehouseutils.GetConfigValue(password, pg.Warehouse),
		port:      warehouseutils.GetConfigValue(port, pg.Warehouse),
		sslMode:   warehouseutils.GetConfigValue(sslMode, pg.Warehouse),
		sslParams: sslParams,
	}
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, rudderDataTypesMapToPostgres[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (bq *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	return
}

func (pg *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objects := pg.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	storageProvider := warehouseutils.ObjectStorageType(pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.Config, pg.Uploader.UseRudderStorage())
	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           pg.Warehouse.Destination.Config,
			UseRudderStorage: pg.Uploader.UseRudderStorage(),
		}),
	})
	if err != nil {
		pkgLogger.Errorf("PG: Error in setting up a downloader for destionationID : %s Error : %v", pg.Warehouse.Destination.ID, err)
		return nil, err
	}
	var fileNames []string
	for _, object := range objects {
		objectName, err := warehouseutils.GetObjectName(object.Location, pg.Warehouse.Destination.Config, pg.ObjectStorage)
		if err != nil {
			pkgLogger.Errorf("PG: Error in converting object location to object key for table:%s: %s,%v", tableName, object.Location, err)
			return nil, err
		}
		dirName := fmt.Sprintf(`/%s/`, misc.RudderWarehouseLoadUploadsTmp)
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			pkgLogger.Errorf("PG: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.ID, time.Now().Unix()) + objectName
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			pkgLogger.Errorf("PG: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, object.Location, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			pkgLogger.Errorf("PG: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		err = downloader.Download(objectFile, objectName)
		if err != nil {
			pkgLogger.Errorf("PG: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			pkgLogger.Errorf("PG: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, object.Location, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil

}

func (pg *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	sqlStatement := fmt.Sprintf(`SET search_path to "%s"`, pg.Namespace)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		return
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	pkgLogger.Infof("PG: Starting load for table:%s", tableName)

	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)
	sortedColumnString := strings.Join(sortedColumnKeys, ", ")

	fileNames, err := pg.DownloadLoadFiles(tableName)
	defer misc.RemoveFilePaths(fileNames...)
	if err != nil {
		return
	}

	txn, err := pg.Db.Begin()
	if err != nil {
		pkgLogger.Errorf("PG: Error while beginning a transaction in db for loading in table:%s: %v", tableName, err)
		return
	}
	// create temporary table
	stagingTableName = misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, tableName, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")), 63)
	sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s".%[2]s (LIKE "%[1]s"."%[3]s")`, pg.Namespace, stagingTableName, tableName)
	pkgLogger.Debugf("PG: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error creating temporary table for table:%s: %v\n", tableName, err)
		txn.Rollback()
		return
	}
	if !skipTempTableDelete {
		defer pg.dropStagingTable(stagingTableName)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(pg.Namespace, stagingTableName, sortedColumnKeys...))
	if err != nil {
		pkgLogger.Errorf("PG: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		txn.Rollback()
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			pkgLogger.Errorf("PG: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			txn.Rollback()
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			pkgLogger.Errorf("PG: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
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
					pkgLogger.Debugf("PG: File reading completed while reading csv file for loading in staging table:%s: %s", stagingTableName, objectFileName)
					break
				} else {
					pkgLogger.Errorf("PG: Error while reading csv file %s for loading in staging table:%s: %v", objectFileName, stagingTableName, err)
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
			_, err = stmt.Exec(recordInterface...)
			if err != nil {
				pkgLogger.Errorf("PG: Error in exec statement for loading in staging table:%s: %v", stagingTableName, err)
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
		pkgLogger.Errorf("PG: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
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
		additionalJoinClause = fmt.Sprintf(`AND _source.%[3]s = "%[1]s"."%[2]s"."%[3]s" AND _source.%[4]s = "%[1]s"."%[2]s"."%[4]s"`, pg.Namespace, tableName, "table_name", "column_name")
	}
	sqlStatement = fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" USING "%[1]s"."%[3]s" as  _source where (_source.%[4]s = "%[1]s"."%[2]s"."%[4]s" %[5]s)`, pg.Namespace, tableName, stagingTableName, primaryKey, additionalJoinClause)
	pkgLogger.Infof("PG: Deduplicate records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error deleting from original table for dedup: %v\n", err)
		txn.Rollback()
		return
	}
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, pg.Namespace, tableName, sortedColumnString, stagingTableName, partitionKey)
	pkgLogger.Infof("PG: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("PG: Error inserting into original table: %v\n", err)
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		pkgLogger.Errorf("PG: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		txn.Rollback()
		return
	}

	pkgLogger.Infof("PG: Complete load for table:%s", tableName)
	return
}

func (pg *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	sqlStatement := fmt.Sprintf(`SET search_path to "%s"`, pg.Namespace)
	_, err := pg.Db.Exec(sqlStatement)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	pkgLogger.Infof("PG: Starting load for identifies and users tables\n")
	identifyStagingTable, err := pg.loadTable(warehouseutils.IdentifiesTable, pg.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
	defer pg.dropStagingTable(identifyStagingTable)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(pg.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	if skipComputingUserLatestTraits {
		_, err := pg.loadTable(warehouseutils.UsersTable, pg.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable), false)
		if err != nil {
			errorMap[warehouseutils.UsersTable] = err
		}
		return
	}

	unionStagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), "users_identifies_union"), 63)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), warehouseutils.UsersTable), 63)
	defer pg.dropStagingTable(stagingTableName)
	defer pg.dropStagingTable(unionStagingTableName)

	userColMap := pg.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		caseSubQuery := fmt.Sprintf(`case
						  when (select true) then (
						  	select %[1]s from "%[3]s"."%[2]s" as staging_table
						  	where x.id = staging_table.id
							  and %[1]s is not null
							  order by received_at desc
						  	limit 1)
						  end as %[1]s`, colName, unionStagingTableName, pg.Namespace)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s".%[5]s as (
												(
													SELECT id, %[4]s FROM "%[1]s"."%[2]s" WHERE id in (SELECT user_id FROM "%[1]s"."%[3]s" WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[4]s FROM "%[1]s"."%[3]s"  WHERE user_id IS NOT NULL
												)
											)`, pg.Namespace, warehouseutils.UsersTable, identifyStagingTable, strings.Join(userColNames, ","), unionStagingTableName)

	pkgLogger.Infof("PG: Creating staging table for union of users table with identify staging table: %s\n", sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE %[4]s.%[1]s AS (SELECT DISTINCT * FROM
										(
											SELECT
											x.id, %[2]s
											FROM %[4]s.%[3]s as x
										) as xyz
									)`,
		stagingTableName,
		strings.Join(firstValProps, ","),
		unionStagingTableName,
		pg.Namespace,
	)

	pkgLogger.Debugf("PG: Creating staging table for users: %s\n", sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	// BEGIN TRANSACTION
	tx, err := pg.Db.Begin()
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" using "%[1]s"."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, pg.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey)
	pkgLogger.Infof("PG: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  "%[1]s"."%[3]s"`, pg.Namespace, warehouseutils.UsersTable, stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	pkgLogger.Infof("PG: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("PG: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("PG: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (pg *HandleT) schemaExists(schemaname string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, pg.Namespace)
	err = pg.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (pg *HandleT) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = pg.schemaExists(pg.Namespace)
	if err != nil {
		pkgLogger.Errorf("PG: Error checking if schema: %s exists: %v", pg.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("PG: Skipping creating schema: %s since it already exists", pg.Namespace)
		return
	}
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, pg.Namespace)
	pkgLogger.Infof("PG: Creating schema name in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) dropStagingTable(stagingTableName string) {
	pkgLogger.Infof("PG: dropping table %+v\n", stagingTableName)
	_, err := pg.Db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%[1]s"."%[2]s"`, pg.Namespace, stagingTableName))
	if err != nil {
		pkgLogger.Errorf("PG:  Error dropping staging table %s in postgres: %v", stagingTableName, err)
	}
}

func (pg *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%[1]s"."%[2]s" ( %v )`, pg.Namespace, name, columnsWithDataTypes(columns, ""))
	pkgLogger.Infof("PG: Creating table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s`, pg.Namespace, tableName, columnName, rudderDataTypesMapToPostgres[columnType])
	pkgLogger.Infof("PG: Adding column in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to "%s"`, pg.Namespace)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		return err
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	err = pg.createTable(tableName, columnMap)
	return err
}

func (pg *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to "%s"`, pg.Namespace)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		return err
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	err = pg.addColumn(tableName, columnName, columnType)
	return err
}

func (pg *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

func (pg *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	pg.Warehouse = warehouse
	pg.Db, err = connect(pg.getConnectionCredentials())
	if err != nil {
		return
	}
	defer pg.Db.Close()

	timeOut := 5 * time.Second

	ctx, cancel := context.WithTimeout(context.TODO(), timeOut)
	defer cancel()

	err = pg.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", timeOut/time.Second)
	}
	if err != nil {
		return err
	}

	return nil

}

func (pg *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.Uploader = uploader
	pg.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.POSTGRES, warehouse.Destination.Config, pg.Uploader.UseRudderStorage())

	pg.Db, err = connect(pg.getConnectionCredentials())
	return err
}

func (pg *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.Db, err = connect(pg.getConnectionCredentials())
	if err != nil {
		return err
	}
	defer pg.Db.Close()
	pg.dropDanglingStagingTables()
	return
}

func (pg *HandleT) dropDanglingStagingTables() bool {

	sqlStatement := fmt.Sprintf(`select table_name
								 from information_schema.tables
								 where table_schema = '%s' AND table_name like '%s';`, pg.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "%"))
	rows, err := pg.Db.Query(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("WH: PG: Error dropping dangling staging tables in PG: %v\nQuery: %s\n", err, sqlStatement)
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
	pkgLogger.Infof("WH: PG: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := pg.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, pg.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: PG:  Error dropping dangling staging table: %s in PG: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

// FetchSchema queries postgres and returns the schema associated with provided namespace
func (pg *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	dbHandle, err := connect(pg.getConnectionCredentials())
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	sqlStatement := fmt.Sprintf(`select t.table_name, c.column_name, c.data_type from INFORMATION_SCHEMA.TABLES t LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON (t.table_name = c.table_name and t.table_schema = c.table_schema) WHERE t.table_schema = '%s' and t.table_name not like '%s%s'`, pg.Namespace, stagingTablePrefix, "%")

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("PG: Error in fetching schema from postgres destination:%v, query: %v", pg.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("PG: No rows, while fetching schema from  destination:%v, query: %v", pg.Warehouse.Identifier, sqlStatement)
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType sql.NullString
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pkgLogger.Errorf("PG: Error in processing fetched schema from redshift destination:%v", pg.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName.String]; !ok {
			schema[tName.String] = make(map[string]string)
		}
		if cName.Valid && cType.Valid {
			if datatype, ok := postgresDataTypesMapToRudder[cType.String]; ok {
				schema[tName.String][cName.String] = datatype
			}
		}
	}
	return
}

func (pg *HandleT) LoadUserTables() map[string]error {
	return pg.loadUserTables()
}

func (pg *HandleT) LoadTable(tableName string) error {
	_, err := pg.loadTable(tableName, pg.Uploader.GetTableSchemaInUpload(tableName), false)
	return err
}

func (pg *HandleT) Cleanup() {
	if pg.Db != nil {
		pg.dropDanglingStagingTables()
		pg.Db.Close()
	}
}

func (pg *HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (pg *HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

func (pg *HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (pg *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, pg.Namespace, tableName)
	err = pg.Db.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`PG: Error getting total count in table %s:%s`, pg.Namespace, tableName)
	}
	return
}

func (pg *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	dbHandle, err := connect(pg.getConnectionCredentials())
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

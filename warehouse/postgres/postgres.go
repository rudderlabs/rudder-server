package postgres

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

var (
	stagingTablePrefix string
	pkgLogger          logger.LoggerI
)

const (
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
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
	url := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v sslmode=%v",
		cred.user,
		cred.password,
		cred.host,
		cred.port,
		cred.dbName,
		cred.sslMode)

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("postgres connection error : (%v)", err)
	}
	return db, nil
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("postgres")
}

func loadConfig() {
	stagingTablePrefix = "rudder_staging_"
}

func (pg *HandleT) getConnectionCredentials() credentialsT {
	return credentialsT{
		host:     warehouseutils.GetConfigValue(host, pg.Warehouse),
		dbName:   warehouseutils.GetConfigValue(dbName, pg.Warehouse),
		user:     warehouseutils.GetConfigValue(user, pg.Warehouse),
		password: warehouseutils.GetConfigValue(password, pg.Warehouse),
		port:     warehouseutils.GetConfigValue(port, pg.Warehouse),
		sslMode:  warehouseutils.GetConfigValue(sslMode, pg.Warehouse),
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
	objectLocations := pg.Uploader.GetLoadFileLocations(warehouseutils.GetLoadFileLocationsOptionsT{Table: tableName})
	var fileNames []string
	for _, objectLocation := range objectLocations {
		object, err := warehouseutils.GetObjectName(objectLocation, pg.Warehouse.Destination.Config, pg.ObjectStorage)
		if err != nil {
			pkgLogger.Errorf("PG: Error in converting object location to object key for table:%s: %s,%v", tableName, objectLocation, err)
			return nil, err
		}
		dirName := "/rudder-warehouse-load-uploads-tmp/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			pkgLogger.Errorf("PG: Error in creating tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		ObjectPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s_%d/`, pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.ID, time.Now().Unix()) + object
		err = os.MkdirAll(filepath.Dir(ObjectPath), os.ModePerm)
		if err != nil {
			pkgLogger.Errorf("PG: Error in making tmp directory for downloading load file for table:%s: %s, %s %v", tableName, objectLocation, err)
			return nil, err
		}
		objectFile, err := os.Create(ObjectPath)
		if err != nil {
			pkgLogger.Errorf("PG: Error in creating file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		downloader, err := filemanager.New(&filemanager.SettingsT{
			Provider: warehouseutils.ObjectStorageType(pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.Config),
			Config:   pg.Warehouse.Destination.Config,
		})
		if err != nil {
			pkgLogger.Errorf("PG: Error in setting up a downloader for destionationID : %s Error : %v", pg.Warehouse.Destination.ID, err)
			return nil, err
		}
		err = downloader.Download(objectFile, object)
		if err != nil {
			pkgLogger.Errorf("PG: Error in downloading file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileName := objectFile.Name()
		if err = objectFile.Close(); err != nil {
			pkgLogger.Errorf("PG: Error in closing downloaded file in tmp directory for downloading load file for table:%s: %s, %v", tableName, objectLocation, err)
			return nil, err
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames, nil

}

func (pg *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
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
	stagingTableName = fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, tableName, strings.Replace(uuid.NewV4().String(), "-", "", -1))
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE "%[2]s" (LIKE "%[1]s"."%[3]s")`, pg.Namespace, stagingTableName, tableName)
	pkgLogger.Debugf("PG: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error creating temporary table for table:%s: %v\n", tableName, err)
		return
	}
	if !skipTempTableDelete {
		defer pg.dropStagingTable(stagingTableName)
	}

	stmt, err := txn.Prepare(pq.CopyIn(stagingTableName, sortedColumnKeys...))
	if err != nil {
		pkgLogger.Errorf("PG: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			pkgLogger.Errorf("PG: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			pkgLogger.Errorf("PG: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			gzipFile.Close()
			return

		}
		csvReader := csv.NewReader(gzipReader)
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
	sqlStatement = fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" USING "%[3]s" as  _source where (_source.%[4]s = "%[1]s"."%[2]s"."%[4]s" %[5]s)`, pg.Namespace, tableName, stagingTableName, primaryKey, additionalJoinClause)
	pkgLogger.Infof("PG: Deduplicate records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error deleting from original table for dedup: %v\n", err)
		txn.Rollback()
		return
	}
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[4]s" ) AS _ where _rudder_staging_row_number = 1`, pg.Namespace, tableName, sortedColumnString, stagingTableName, partitionKey)
	pkgLogger.Infof("PG: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("PG: Error inserting into original table: %v\n", err)
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		pkgLogger.Errorf("PG: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		return
	}

	pkgLogger.Infof("PG: Complete load for table:%s", tableName)
	return
}

func (pg *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("PG: Starting load for identifies and users tables\n")
	identifyStagingTable, err := pg.loadTable(warehouseutils.IdentifiesTable, pg.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(pg.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	unionStagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), "users_identifies_union"), 127)
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), warehouseutils.UsersTable), 127)

	userColMap := pg.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		caseSubQuery := fmt.Sprintf(`case
						  when (select true) then (
						  	select %[1]s from %[2]s
						  	where x.id = %[2]s.id
							  and %[1]s is not null
							  order by received_at desc
						  	limit 1)
						  end as %[1]s`, colName, unionStagingTableName)
		firstValProps = append(firstValProps, caseSubQuery)
	}

	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[5]s as (
												(
													SELECT id, %[4]s FROM "%[1]s"."%[2]s" WHERE id in (SELECT user_id FROM %[3]s WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[4]s FROM %[3]s  WHERE user_id IS NOT NULL
												)
											)`, pg.Namespace, warehouseutils.UsersTable, identifyStagingTable, strings.Join(userColNames, ","), unionStagingTableName)

	pkgLogger.Infof("PG: Creating staging table for union of users table with identify staging table: %s\n", sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`CREATE TEMPORARY TABLE %[1]s AS (SELECT DISTINCT * FROM
										(
											SELECT
											x.id, %[2]s
											FROM %[3]s as x
										) as xyz
									)`,
		stagingTableName,
		strings.Join(firstValProps, ","),
		unionStagingTableName,
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
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[3]s _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, pg.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey)
	pkgLogger.Infof("PG: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  %[3]s`, pg.Namespace, warehouseutils.UsersTable, stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
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
	_, err := pg.Db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, stagingTableName))
	if err != nil {
		pkgLogger.Errorf("PG:  Error dropping staging table %s in postgres: %v", stagingTableName, err)
	}
}

func (pg *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ( %v )`, name, columnsWithDataTypes(columns, ""))
	pkgLogger.Infof("PG: Creating table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s`, tableName, columnName, rudderDataTypesMapToPostgres[columnType])
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
	pingResultChannel := make(chan error, 1)
	defer pg.Db.Close()
	rruntime.Go(func() {
		pingResultChannel <- pg.Db.Ping()
	})
	var timeOut time.Duration = 5
	select {
	case err = <-pingResultChannel:
	case <-time.After(timeOut * time.Second):
		err = fmt.Errorf("connection testing timed out after %d sec", timeOut)
	}
	return
}

func (pg *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.POSTGRES, warehouse.Destination.Config)
	pg.Uploader = uploader

	pg.Db, err = connect(pg.getConnectionCredentials())
	return err
}

func (pg *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return
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
	sqlStatement := fmt.Sprintf(`select t.table_name, c.column_name, c.data_type from INFORMATION_SCHEMA.TABLES t LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on t.table_name = c.table_name WHERE t.table_schema = '%s' and t.table_name not like '%s%s'`, pg.Namespace, stagingTablePrefix, "%")

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

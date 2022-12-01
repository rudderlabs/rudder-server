package postgres

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	pkgLogger                       logger.Logger
	skipComputingUserLatestTraits   bool
	enableSQLStatementExecutionPlan bool
	txnRollbackTimeout              time.Duration
	enableDeleteByJobs              bool
)

const (
	host     = "host"
	dbName   = "database"
	user     = "user"
	password = "password"
	port     = "port"
	sslMode  = "sslMode"
	verifyCA = "verify-ca"
)

const (
	provider       = warehouseutils.POSTGRES
	tableNameLimit = 127
)

// load table transaction stages
const (
	createStagingTable       = "staging_table_creation"
	copyInSchemaStagingTable = "staging_table_copy_in_schema"
	openLoadFiles            = "load_files_opening"
	readGzipLoadFiles        = "load_files_gzip_reading"
	readCsvLoadFiles         = "load_files_csv_reading"
	csvColumnCountMismatch   = "csv_column_count_mismatch"
	loadStagingTable         = "staging_table_loading"
	stagingTableloadStage    = "staging_table_load_stage"
	deleteDedup              = "dedup_deletion"
	insertDedup              = "dedup_insertion"
	dedupStage               = "dedup_stage"
)

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
	Db             *sql.DB
	Namespace      string
	ObjectStorage  string
	Warehouse      warehouseutils.Warehouse
	Uploader       warehouseutils.UploaderI
	ConnectTimeout time.Duration
}

type CredentialsT struct {
	Host     string
	DBName   string
	User     string
	Password string
	Port     string
	SSLMode  string
	SSLDir   string
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

func Connect(cred CredentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v sslmode=%v",
		cred.User,
		cred.Password,
		cred.Host,
		cred.Port,
		cred.DBName,
		cred.SSLMode,
	)
	if cred.timeout > 0 {
		url += fmt.Sprintf(" connect_timeout=%d", cred.timeout/time.Second)
	}
	if cred.SSLMode == verifyCA {
		url = fmt.Sprintf("%s sslrootcert=%[2]s/server-ca.pem sslcert=%[2]s/client-cert.pem sslkey=%[2]s/client-key.pem", url, cred.SSLDir)
	}
	var (
		err error
		db  *sql.DB
	)

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
	config.RegisterBoolConfigVariable(false, &skipComputingUserLatestTraits, true, "Warehouse.postgres.skipComputingUserLatestTraits")
	config.RegisterDurationConfigVariable(30, &txnRollbackTimeout, true, time.Second, "Warehouse.postgres.txnRollbackTimeout")
	config.RegisterBoolConfigVariable(false, &enableSQLStatementExecutionPlan, true, "Warehouse.postgres.enableSQLStatementExecutionPlan")
	config.RegisterBoolConfigVariable(false, &enableDeleteByJobs, true, "Warehouse.postgres.enableDeleteByJobs")
}

func (pg *HandleT) getConnectionCredentials() CredentialsT {
	sslMode := warehouseutils.GetConfigValue[string](sslMode, &pg.Warehouse)
	return CredentialsT{
		Host:     warehouseutils.GetConfigValue[string](host, &pg.Warehouse),
		DBName:   warehouseutils.GetConfigValue[string](dbName, &pg.Warehouse),
		User:     warehouseutils.GetConfigValue[string](user, &pg.Warehouse),
		Password: warehouseutils.GetConfigValue[string](password, &pg.Warehouse),
		Port:     warehouseutils.GetConfigValue[string](port, &pg.Warehouse),
		SSLMode:  sslMode,
		SSLDir:   warehouseutils.GetSSLKeyDirPath(pg.Warehouse.Destination.ID),
		timeout:  pg.ConnectTimeout,
	}
}

func ColumnsWithDataTypes(columns map[string]string, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToPostgres[dataType]))
	}
	return strings.Join(arr, ",")
}

func (*HandleT) IsEmpty(_ warehouseutils.Warehouse) (empty bool, err error) {
	return
}

func (pg *HandleT) DownloadLoadFiles(tableName string) ([]string, error) {
	objects := pg.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	storageProvider := warehouseutils.ObjectStorageType(pg.Warehouse.Destination.DestinationDefinition.Name, pg.Warehouse.Destination.Config, pg.Uploader.UseRudderStorage())
	downloader, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           pg.Warehouse.Destination.Config,
			UseRudderStorage: pg.Uploader.UseRudderStorage(),
			WorkspaceID:      pg.Warehouse.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		pkgLogger.Errorf("PG: Error in setting up a downloader for destinationID : %s Error : %v", pg.Warehouse.Destination.ID, err)
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
		err = downloader.Download(context.TODO(), objectFile, objectName)
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

func handleRollbackTimeout(tags stats.Tags) {
	stats.Default.NewTaggedStat("pg_rollback_timeout", stats.CountType, tags).Count(1)
}

func runRollbackWithTimeout(f func() error, onTimeout func(tags stats.Tags), d time.Duration, tags stats.Tags) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		err := f()
		if err != nil {
			pkgLogger.Errorf("PG: Error in rolling back transaction : %v", err)
		}
	}()

	select {
	case <-c:
	case <-time.After(d):
		pkgLogger.Errorf("PG: Timed out rolling back transaction after %v", d)
		onTimeout(tags)
	}
}

func (pg *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	sqlStatement := fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		return
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	pkgLogger.Infof("PG: Starting load for table:%s", tableName)

	// tags
	tags := stats.Tags{
		"workspaceId":   pg.Warehouse.WorkspaceID,
		"namepsace":     pg.Namespace,
		"destinationID": pg.Warehouse.Destination.ID,
		"tableName":     tableName,
	}
	// sort column names
	sortedColumnKeys := warehouseutils.SortColumnKeysFromColumnMap(tableSchemaInUpload)

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
	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s".%[2]s (LIKE "%[1]s"."%[3]s")`, pg.Namespace, stagingTableName, tableName)
	pkgLogger.Debugf("PG: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("PG: Error creating temporary table for table:%s: %v\n", tableName, err)
		tags["stage"] = createStagingTable
		runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		return
	}
	if !skipTempTableDelete {
		defer pg.dropStagingTable(stagingTableName)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(pg.Namespace, stagingTableName, sortedColumnKeys...))
	if err != nil {
		pkgLogger.Errorf("PG: Error while preparing statement for  transaction in db for loading in staging table:%s: %v\nstmt: %v", stagingTableName, err, stmt)
		tags["stage"] = copyInSchemaStagingTable
		runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		return
	}
	for _, objectFileName := range fileNames {
		var gzipFile *os.File
		gzipFile, err = os.Open(objectFileName)
		if err != nil {
			pkgLogger.Errorf("PG: Error opening file using os.Open for file:%s while loading to table %s", objectFileName, tableName)
			tags["stage"] = openLoadFiles
			runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
			return
		}

		var gzipReader *gzip.Reader
		gzipReader, err = gzip.NewReader(gzipFile)
		if err != nil {
			pkgLogger.Errorf("PG: Error reading file using gzip.NewReader for file:%s while loading to table %s", gzipFile, tableName)
			gzipFile.Close()
			tags["stage"] = readGzipLoadFiles
			runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
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
				}
				pkgLogger.Errorf("PG: Error while reading csv file %s for loading in staging table:%s: %v", objectFileName, stagingTableName, err)
				tags["stage"] = readCsvLoadFiles
				runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
				return
			}
			if len(sortedColumnKeys) != len(record) {
				err = fmt.Errorf(`load file CSV columns for a row mismatch number found in upload schema. Columns in CSV row: %d, Columns in upload schema of table-%s: %d. Processed rows in csv file until mismatch: %d`, len(record), tableName, len(sortedColumnKeys), csvRowsProcessedCount)
				pkgLogger.Error(err)
				tags["stage"] = csvColumnCountMismatch
				runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
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
				tags["stage"] = loadStagingTable
				runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
				return
			}
			csvRowsProcessedCount++
		}
		gzipReader.Close()
		gzipFile.Close()
	}

	_, err = stmt.Exec()
	if err != nil {
		pkgLogger.Errorf("PG: Rollback transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		tags["stage"] = stagingTableloadStage
		runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
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
	err = handleExec(&QueryParams{txn: txn, query: sqlStatement, enableWithQueryPlan: enableSQLStatementExecutionPlan})
	if err != nil {
		pkgLogger.Errorf("PG: Error deleting from original table for dedup: %v\n", err)
		tags["stage"] = deleteDedup
		runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		return
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(sortedColumnKeys)
	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s)
									SELECT %[3]s FROM (
										SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at DESC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s"
									) AS _ where _rudder_staging_row_number = 1
									`, pg.Namespace, tableName, quotedColumnNames, stagingTableName, partitionKey)
	pkgLogger.Infof("PG: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	err = handleExec(&QueryParams{txn: txn, query: sqlStatement, enableWithQueryPlan: enableSQLStatementExecutionPlan})

	if err != nil {
		pkgLogger.Errorf("PG: Error inserting into original table: %v\n", err)
		tags["stage"] = insertDedup
		runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		return
	}

	if err = txn.Commit(); err != nil {
		pkgLogger.Errorf("PG: Error while committing transaction as there was error while loading staging table:%s: %v", stagingTableName, err)
		tags["stage"] = dedupStage
		runRollbackWithTimeout(txn.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		return
	}

	pkgLogger.Infof("PG: Complete load for table:%s", tableName)
	return
}

// DeleteBy Need to create a structure with delete parameters instead of simply adding a long list of params
func (pg *HandleT) DeleteBy(tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	pkgLogger.Infof("PG: Cleaning up the following tables in postgres for PG:%s : %+v", tableNames, params)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
		context_sources_job_run_id <> $1 AND
		context_sources_task_run_id <> $2 AND
		context_source_id = $3 AND
		received_at < $4`,
			pg.Namespace,
			tb,
		)
		pkgLogger.Infof("PG: Deleting rows in table in postgres for PG:%s", pg.Warehouse.Destination.ID)
		pkgLogger.Debugf("PG: Executing the statement  %v", sqlStatement)
		if enableDeleteByJobs {
			_, err = pg.Db.Exec(sqlStatement,
				params.JobRunId,
				params.TaskRunId,
				params.SourceId,
				params.StartTime)
			if err != nil {
				pkgLogger.Errorf("Error %s", err)
				return err
			}
		}

	}
	return nil
}

func (pg *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	sqlStatement := fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
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

	unionStagingTableName := warehouseutils.StagingTableName(provider, "users_identifies_union", tableNameLimit)
	stagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	defer pg.dropStagingTable(stagingTableName)
	defer pg.dropStagingTable(unionStagingTableName)

	userColMap := pg.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf(`%q`, colName))
		caseSubQuery := fmt.Sprintf(`case
						  when (select true) then (
						  	select "%[1]s" from "%[3]s"."%[2]s" as staging_table
						  	where x.id = staging_table.id
							  and "%[1]s" is not null
							  order by received_at desc
						  	limit 1)
						  end as "%[1]s"`, colName, unionStagingTableName, pg.Namespace)
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
	// tags
	tags := stats.Tags{
		"workspaceId": pg.Warehouse.WorkspaceID,
		"namespace":   pg.Namespace,
		"destId":      pg.Warehouse.Destination.ID,
		"tableName":   warehouseutils.UsersTable,
	}
	err = handleExec(&QueryParams{txn: tx, query: sqlStatement, enableWithQueryPlan: enableSQLStatementExecutionPlan})
	if err != nil {
		pkgLogger.Errorf("PG: Error deleting from original table for dedup: %v\n", err)
		tags["stage"] = deleteDedup
		runRollbackWithTimeout(tx.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  "%[1]s"."%[3]s"`, pg.Namespace, warehouseutils.UsersTable, stagingTableName, strings.Join(append([]string{"id"}, userColNames...), ","))
	pkgLogger.Infof("PG: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
	err = handleExec(&QueryParams{txn: tx, query: sqlStatement, enableWithQueryPlan: enableSQLStatementExecutionPlan})

	if err != nil {
		pkgLogger.Errorf("PG: Error inserting into users table from staging table: %v\n", err)
		tags["stage"] = insertDedup
		runRollbackWithTimeout(tx.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("PG: Error in transaction commit for users table: %v\n", err)
		tags["stage"] = dedupStage
		runRollbackWithTimeout(tx.Rollback, handleRollbackTimeout, txnRollbackTimeout, tags)
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

func (pg *HandleT) schemaExists(_ string) (exists bool, err error) {
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
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, pg.Namespace)
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
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%[1]s"."%[2]s" ( %v )`, pg.Namespace, name, ColumnsWithDataTypes(columns, ""))
	pkgLogger.Infof("PG: Creating table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	_, err = pg.Db.Exec(sqlStatement)
	if err != nil {
		return err
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	err = pg.createTable(tableName, columnMap)
	return err
}

func (pg *HandleT) DropTable(tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	pkgLogger.Infof("PG: Dropping table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.Db.Exec(fmt.Sprintf(sqlStatement, pg.Namespace, tableName))
	return
}

func (pg *HandleT) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var query string

	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	query = fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	if _, err = pg.Db.Exec(query); err != nil {
		return
	}
	pkgLogger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, query)

	query = fmt.Sprintf(`
		ALTER TABLE
		  %s.%s`,
		pg.Namespace,
		tableName,
	)

	for _, columnInfo := range columnsInfo {
		query += fmt.Sprintf(` ADD COLUMN IF NOT EXISTS %q %s,`, columnInfo.Name, rudderDataTypesMapToPostgres[columnInfo.Type])
	}

	query = strings.TrimSuffix(query, ",")
	query += ";"

	pkgLogger.Infof("PG: Adding columns for destinationID: %s, tableName: %s with query: %v", pg.Warehouse.Destination.ID, tableName, query)
	_, err = pg.Db.Exec(query)
	return
}

func (*HandleT) AlterColumn(_, _, _ string) (err error) {
	return
}

func (pg *HandleT) TestConnection(warehouse warehouseutils.Warehouse) (err error) {
	if warehouse.Destination.Config["sslMode"] == "verify-ca" {
		if sslKeyError := warehouseutils.WriteSSLKeys(warehouse.Destination); sslKeyError.IsError() {
			pkgLogger.Error(sslKeyError.Error())
			err = fmt.Errorf(sslKeyError.Error())
			return
		}
	}
	pg.Warehouse = warehouse
	pg.Db, err = Connect(pg.getConnectionCredentials())
	if err != nil {
		return
	}
	defer pg.Db.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), pg.ConnectTimeout)
	defer cancel()

	err = pg.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", pg.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

func (pg *HandleT) Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.Uploader = uploader
	pg.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.POSTGRES, warehouse.Destination.Config, pg.Uploader.UseRudderStorage())

	pg.Db, err = Connect(pg.getConnectionCredentials())
	return err
}

func (pg *HandleT) CrashRecover(warehouse warehouseutils.Warehouse) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.Db, err = Connect(pg.getConnectionCredentials())
	if err != nil {
		return err
	}
	defer pg.Db.Close()
	pg.dropDanglingStagingTables()
	return
}

func (pg *HandleT) dropDanglingStagingTables() bool {
	sqlStatement := `
			select
			  table_name
			from
			  information_schema.tables
			where
			  table_schema = $1
			  AND table_name like $2;
		`
	rows, err := pg.Db.Query(
		sqlStatement,
		pg.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
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
func (pg *HandleT) FetchSchema(warehouse warehouseutils.Warehouse) (schema, unrecognizedSchema warehouseutils.SchemaT, err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	dbHandle, err := Connect(pg.getConnectionCredentials())
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	unrecognizedSchema = make(warehouseutils.SchemaT)

	sqlStatement := `
		SELECT
		  table_name,
		  column_name,
		  data_type
		FROM
		  INFORMATION_SCHEMA.COLUMNS
		WHERE
		  table_schema = $1
		  AND table_name NOT LIKE $2;
		`
	rows, err := dbHandle.Query(
		sqlStatement,
		pg.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("PG: Error in fetching schema from postgres destination:%v, query: %v", pg.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("PG: No rows, while fetching schema from  destination:%v, query: %v", pg.Warehouse.Identifier, sqlStatement)
		return schema, unrecognizedSchema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType sql.NullString
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pkgLogger.Errorf("PG: Error in processing fetched schema from clickhouse destination:%v", pg.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName.String]; !ok {
			schema[tName.String] = make(map[string]string)
		}
		if cName.Valid && cType.Valid {
			if datatype, ok := postgresDataTypesMapToRudder[cType.String]; ok {
				schema[tName.String][cName.String] = datatype
			} else {
				if _, ok := unrecognizedSchema[tName.String]; !ok {
					unrecognizedSchema[tName.String] = make(map[string]string)
				}
				unrecognizedSchema[tName.String][cType.String] = warehouseutils.MISSING_DATATYPE

				warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &pg.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType.String}).Count(1)
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

func (*HandleT) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (*HandleT) LoadIdentityMappingsTable() (err error) {
	return
}

func (*HandleT) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (pg *HandleT) GetTotalCountInTable(ctx context.Context, tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, pg.Namespace, tableName)
	err = pg.Db.QueryRowContext(ctx, sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`PG: Error getting total count in table %s:%s`, pg.Namespace, tableName)
	}
	return
}

func (pg *HandleT) Connect(warehouse warehouseutils.Warehouse) (client.Client, error) {
	if warehouse.Destination.Config["sslMode"] == "verify-ca" {
		if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
			pkgLogger.Error(err.Error())
			return client.Client{}, fmt.Errorf(err.Error())
		}
	}
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.POSTGRES,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(pg.Warehouse.Destination.Config),
	)
	dbHandle, err := Connect(pg.getConnectionCredentials())
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (pg *HandleT) LoadTestTable(_, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		pg.Namespace,
		tableName,
		fmt.Sprintf(`%q, %q`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = pg.Db.Exec(sqlStatement)
	return
}

func (pg *HandleT) SetConnectionTimeout(timeout time.Duration) {
	pg.ConnectTimeout = timeout
}

type QueryParams struct {
	txn                 *sql.Tx
	db                  *sql.DB
	query               string
	enableWithQueryPlan bool
}

func (q *QueryParams) validate() (err error) {
	if q.txn == nil && q.db == nil {
		return fmt.Errorf("both txn and db are nil")
	}
	return
}

// handleExec
// Print execution plan if enableWithQueryPlan is set to true else return result set.
// Currently, these statements are supported by EXPLAIN
// Any INSERT, UPDATE, DELETE whose execution plan you wish to see.
func handleExec(e *QueryParams) (err error) {
	sqlStatement := e.query

	if err = e.validate(); err != nil {
		err = fmt.Errorf("[WH][POSTGRES] Not able to handle query execution for statement: %s as both txn and db are nil", sqlStatement)
		return
	}

	if e.enableWithQueryPlan {
		sqlStatement := "EXPLAIN " + e.query

		var rows *sql.Rows
		if e.txn != nil {
			rows, err = e.txn.Query(sqlStatement)
		} else if e.db != nil {
			rows, err = e.db.Query(sqlStatement)
		}
		if err != nil {
			err = fmt.Errorf("[WH][POSTGRES] error occurred while handling transaction for query: %s with err: %w", sqlStatement, err)
			return
		}
		defer func() { _ = rows.Close() }()

		var response []string
		for rows.Next() {
			var s string
			if err = rows.Scan(&s); err != nil {
				err = fmt.Errorf("[WH][POSTGRES] Error occurred while processing destination revisionID query %+v with err: %w", e, err)
				return
			}
			response = append(response, s)
		}
		pkgLogger.Infof(fmt.Sprintf(`[WH][POSTGRES] Execution Query plan for statement: %s is %s`, sqlStatement, strings.Join(response, `
`)))
	}
	if e.txn != nil {
		_, err = e.txn.Exec(sqlStatement)
	} else if e.db != nil {
		_, err = e.db.Exec(sqlStatement)
	}
	return
}

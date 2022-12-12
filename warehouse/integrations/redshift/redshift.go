package redshift

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	setVarCharMax                 bool
	dedupWindow                   bool
	dedupWindowInHours            time.Duration
	pkgLogger                     logger.Logger
	skipComputingUserLatestTraits bool
	enableDeleteByJobs            bool
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("redshift")
}

func loadConfig() {
	setVarCharMax = config.GetBool("Warehouse.redshift.setVarCharMax", false)

	config.RegisterBoolConfigVariable(false, &dedupWindow, true, "Warehouse.redshift.dedupWindow")
	config.RegisterDurationConfigVariable(720, &dedupWindowInHours, true, time.Hour, "Warehouse.redshift.dedupWindowInHours")
	config.RegisterBoolConfigVariable(false, &skipComputingUserLatestTraits, true, "Warehouse.redshift.skipComputingUserLatestTraits")
	config.RegisterBoolConfigVariable(false, &enableDeleteByJobs, true, "Warehouse.redshift.enableDeleteByJobs")
}

type HandleT struct {
	Db             *sql.DB
	Namespace      string
	Warehouse      warehouseutils.Warehouse
	Uploader       warehouseutils.UploaderI
	ConnectTimeout time.Duration
}

// String constants for redshift destination config
const (
	RSHost     = "host"
	RSPort     = "port"
	RSDbName   = "database"
	RSUserName = "user"
	RSPassword = "password"
)

const (
	rudderStringLength = 512
	provider           = warehouseutils.RS
	tableNameLimit     = 127
)

var dataTypesMap = map[string]string{
	"boolean":  "boolean encode runlength",
	"int":      "bigint",
	"bigint":   "bigint",
	"float":    "double precision",
	"string":   "varchar(512)",
	"text":     "varchar(max)",
	"datetime": "timestamp",
	"json":     "super",
}

var dataTypesMapToRudder = map[string]string{
	"int":                         "int",
	"int2":                        "int",
	"int4":                        "int",
	"int8":                        "int",
	"bigint":                      "int",
	"float":                       "float",
	"float4":                      "float",
	"float8":                      "float",
	"numeric":                     "float",
	"double precision":            "float",
	"boolean":                     "boolean",
	"bool":                        "boolean",
	"text":                        "string",
	"character varying":           "string",
	"nchar":                       "string",
	"bpchar":                      "string",
	"character":                   "string",
	"nvarchar":                    "string",
	"string":                      "string",
	"date":                        "datetime",
	"timestamp without time zone": "datetime",
	"timestamp with time zone":    "datetime",
	"super":                       "json",
}

var primaryKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id",
}

var partitionKeyMap = map[string]string{
	"users":                      "id",
	"identifies":                 "id",
	warehouseutils.DiscardsTable: "row_id, column_name, table_name",
}

// getRSDataType gets datatype for rs which is mapped with RudderStack datatype
func getRSDataType(columnType string) string {
	return dataTypesMap[columnType]
}

func ColumnsWithDataTypes(columns map[string]string, prefix string) string {
	// TODO: do we need sorted order here?
	var keys []string
	for colName := range columns {
		keys = append(keys, colName)
	}
	sort.Strings(keys)

	var arr []string
	for _, name := range keys {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, getRSDataType(columns[name])))
	}
	return strings.Join(arr, ",")
}

func (rs *HandleT) CreateTable(tableName string, columns map[string]string) (err error) {
	name := fmt.Sprintf(`%q.%q`, rs.Namespace, tableName)
	sortKeyField := "received_at"
	if _, ok := columns["received_at"]; !ok {
		sortKeyField = "uuid_ts"
		if _, ok = columns["uuid_ts"]; !ok {
			sortKeyField = "id"
		}
	}
	var distKeySql string
	if _, ok := columns["id"]; ok {
		distKeySql = `DISTSTYLE KEY DISTKEY("id")`
	}
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v ) %s SORTKEY(%q) `, name, ColumnsWithDataTypes(columns, ""), distKeySql, sortKeyField)
	pkgLogger.Infof("Creating table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) DropTable(tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	pkgLogger.Infof("RS: Dropping table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(fmt.Sprintf(sqlStatement, rs.Namespace, tableName))
	return
}

func (rs *HandleT) schemaExists(_ string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, rs.Namespace)
	err = rs.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (rs *HandleT) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	for _, columnInfo := range columnsInfo {
		query := fmt.Sprintf(`
		ALTER TABLE
		  %q.%q
		ADD
		  COLUMN %q %s;
	`,
			rs.Namespace,
			tableName,
			columnInfo.Name,
			getRSDataType(columnInfo.Type),
		)
		pkgLogger.Infof("AZ: Adding column for destinationID: %s, tableName: %s with query: %v", rs.Warehouse.Destination.ID, tableName, query)

		if _, err := rs.Db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (rs *HandleT) DeleteBy(tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	pkgLogger.Infof("RS: Cleaning up the following tables in redshift for RS:%s : %+v", tableNames, params)
	pkgLogger.Infof("RS: Flag for enableDeleteByJobs is %t", enableDeleteByJobs)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
		context_sources_job_run_id <> $1 AND
		context_sources_task_run_id <> $2 AND
		context_source_id = $3 AND
		received_at < $4`,
			rs.Namespace,
			tb,
		)

		pkgLogger.Infof("RS: Deleting rows in table in redshift for RS:%s", rs.Warehouse.Destination.ID)
		pkgLogger.Debugf("RS: Executing the query %v", sqlStatement)

		if enableDeleteByJobs {
			_, err = rs.Db.Exec(sqlStatement,
				params.JobRunId,
				params.TaskRunId,
				params.SourceId,
				params.StartTime,
			)
			if err != nil {
				pkgLogger.Errorf("Error in executing the query %s", err.Error)
				return err
			}
		}

	}
	return nil
}

// alterStringToText alters column data type string(varchar(512)) to text which is varchar(max) in redshift
func (rs *HandleT) alterStringToText(tableName, columnName string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %v ALTER COLUMN %q TYPE %s`, tableName, columnName, getRSDataType("text"))
	pkgLogger.Infof("Altering column type in redshift from string to text(varchar(max)) RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, rs.Namespace)
	pkgLogger.Infof("Creating schema name in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.Db.Exec(sqlStatement)
	return
}

type S3ManifestEntryMetadataT struct {
	ContentLength int64 `json:"content_length"`
}

type S3ManifestEntryT struct {
	Url       string                   `json:"url"`
	Mandatory bool                     `json:"mandatory"`
	Metadata  S3ManifestEntryMetadataT `json:"meta"`
}

type S3ManifestT struct {
	Entries []S3ManifestEntryT `json:"entries"`
}

func (rs *HandleT) generateManifest(tableName string, _ map[string]string) (string, error) {
	loadFiles := rs.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	loadFiles = warehouseutils.GetS3Locations(loadFiles)
	var manifest S3ManifestT
	for idx, loadFile := range loadFiles {
		manifestEntry := S3ManifestEntryT{Url: loadFile.Location, Mandatory: true}
		// add contentLength to manifest entry if it exists
		contentLength := gjson.Get(string(loadFiles[idx].Metadata), "content_length")
		if contentLength.Exists() {
			manifestEntry.Metadata.ContentLength = contentLength.Int()
		}
		manifest.Entries = append(manifest.Entries, manifestEntry)
	}
	pkgLogger.Infof("RS: Generated manifest for table:%s", tableName)
	manifestJSON, _ := json.Marshal(&manifest)

	manifestFolder := misc.RudderRedshiftManifests
	dirName := "/" + manifestFolder + "/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	localManifestPath := fmt.Sprintf("%v%v", tmpDirPath+dirName, misc.FastUUID().String())
	err = os.MkdirAll(filepath.Dir(localManifestPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer misc.RemoveFilePaths(localManifestPath)
	_ = os.WriteFile(localManifestPath, manifestJSON, 0o644)

	file, err := os.Open(localManifestPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	uploader, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: warehouseutils.S3,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         warehouseutils.S3,
			Config:           rs.Warehouse.Destination.Config,
			UseRudderStorage: rs.Uploader.UseRudderStorage(),
			WorkspaceID:      rs.Warehouse.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		return "", err
	}

	uploadOutput, err := uploader.Upload(context.TODO(), file, manifestFolder, rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID, time.Now().Format("01-02-2006"), tableName, misc.FastUUID().String())
	if err != nil {
		return "", err
	}

	return uploadOutput.Location, nil
}

func (rs *HandleT) dropStagingTables(stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		pkgLogger.Infof("WH: dropping table %+v\n", stagingTableName)
		_, err := rs.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: RS:  Error dropping staging tables in redshift: %v", err)
		}
	}
}

func (rs *HandleT) loadTable(tableName string, tableSchemaInUpload, tableSchemaAfterUpload warehouseutils.TableSchemaT, skipTempTableDelete bool) (stagingTableName string, err error) {
	manifestLocation, err := rs.generateManifest(tableName, tableSchemaInUpload)
	if err != nil {
		return
	}
	pkgLogger.Infof("RS: Generated and stored manifest for table:%s at %s\n", tableName, manifestLocation)

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`%q`, name)
	}, ",")

	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	err = rs.CreateTable(stagingTableName, tableSchemaAfterUpload)
	if err != nil {
		return
	}
	if !skipTempTableDelete {
		defer rs.dropStagingTables([]string{stagingTableName})
	}

	manifestS3Location, region := warehouseutils.GetS3Location(manifestLocation)
	if region == "" {
		region = "us-east-1"
	}

	// BEGIN TRANSACTION
	tx, err := rs.Db.Begin()
	if err != nil {
		return
	}
	// create session token and temporary credentials
	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&rs.Warehouse.Destination)
	if err != nil {
		pkgLogger.Errorf("RS: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
		tx.Rollback()
		return
	}

	var sqlStatement string
	if rs.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		// copy statement for parquet load files
		sqlStatement = fmt.Sprintf(`COPY %v FROM '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' MANIFEST FORMAT PARQUET`, fmt.Sprintf(`%q.%q`, rs.Namespace, stagingTableName), manifestS3Location, tempAccessKeyId, tempSecretAccessKey, token)
	} else {
		// copy statement for csv load files
		sqlStatement = fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' REGION '%s'  DATEFORMAT 'auto' TIMEFORMAT 'auto' MANIFEST TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, stagingTableName), sortedColumnNames, manifestS3Location, tempAccessKeyId, tempSecretAccessKey, token, region)
	}

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
		"SESSION_TOKEN '[^']*'":     "SESSION_TOKEN '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("RS: Running COPY command for table:%s at %s\n", tableName, sanitisedSQLStmt)
	}

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("RS: Error running COPY command: %v\n", err)
		tx.Rollback()
		return
	}

	var (
		primaryKey   = "id"
		partitionKey = "id"
	)

	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	sqlStatement = fmt.Sprintf(`
		DELETE FROM
			%[1]s.%[2]q
		USING
			%[1]s.%[3]q _source
		WHERE
			_source.%[4]s = %[1]s.%[2]q.%[4]s
`,
		rs.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
	)

	if dedupWindow {
		if _, ok := tableSchemaAfterUpload["received_at"]; ok {
			sqlStatement += fmt.Sprintf(`
				AND %[1]s.%[2]q.received_at > GETDATE() - INTERVAL '%[3]d DAY'
`,
				rs.Namespace,
				tableName,
				dedupWindowInHours/time.Hour,
			)
		}
	}

	if tableName == warehouseutils.DiscardsTable {
		sqlStatement += fmt.Sprintf(`
			AND _source.%[3]s = %[1]s.%[2]q.%[3]s
			AND _source.%[4]s = %[1]s.%[2]q.%[4]s
`,
			rs.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	pkgLogger.Infof("RS: Dedup records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("RS: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		return
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(strKeys)

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[3]s) SELECT %[3]s FROM ( SELECT *, row_number() OVER (PARTITION BY %[5]s ORDER BY received_at ASC) AS _rudder_staging_row_number FROM "%[1]s"."%[4]s" ) AS _ where _rudder_staging_row_number = 1`, rs.Namespace, tableName, quotedColumnNames, stagingTableName, partitionKey)
	pkgLogger.Infof("RS: Inserting records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("RS: Error inserting into original table: %v\n", err)
		tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("RS: Error in transaction commit: %v\n", err)
		tx.Rollback()
		return
	}
	pkgLogger.Infof("RS: Complete load for table:%s\n", tableName)
	return
}

func (rs *HandleT) loadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("RS: Starting load for identifies and users tables\n")

	identifyStagingTable, err := rs.loadTable(warehouseutils.IdentifiesTable, rs.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable), rs.Uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable), true)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}
	defer rs.dropStagingTables([]string{identifyStagingTable})

	if len(rs.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	if skipComputingUserLatestTraits {
		_, err := rs.loadTable(warehouseutils.UsersTable, rs.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable), rs.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable), false)
		if err != nil {
			errorMap[warehouseutils.UsersTable] = err
		}
		return
	}

	userColMap := rs.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		// do not reference uuid in queries as it can be an autoincrement field set by segment compatible tables
		if colName == "id" || colName == "user_id" || colName == "uuid" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE("%[1]s" IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "%[1]s"`, colName))
	}
	quotedUserColNames := warehouseutils.DoubleQuoteAndJoinByComma(userColNames)

	stagingTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)

	sqlStatement := fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" AS (SELECT DISTINCT * FROM
										(
											SELECT
											id, %[3]s
											FROM (
												(
													SELECT id, %[6]s FROM "%[1]s"."%[4]s" WHERE id in (SELECT DISTINCT(user_id) FROM "%[1]s"."%[5]s" WHERE user_id IS NOT NULL)
												) UNION
												(
													SELECT user_id, %[6]s FROM "%[1]s"."%[5]s" WHERE user_id IS NOT NULL
												)
											)
										)
									)`,
		rs.Namespace,                     // 1
		stagingTableName,                 // 2
		strings.Join(firstValProps, ","), // 3
		warehouseutils.UsersTable,        // 4
		identifyStagingTable,             // 5
		quotedUserColNames,               // 6
	)

	// BEGIN TRANSACTION
	tx, err := rs.Db.Begin()
	if err != nil {
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("RS: Creating staging table for users failed: %s\n", sqlStatement)
		pkgLogger.Errorf("RS: Error creating users staging table from original table and identifies staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	defer rs.dropStagingTables([]string{stagingTableName})

	primaryKey := "id"
	sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s."%[2]s" using %[1]s."%[3]s" _source where (_source.%[4]s = %[1]s.%[2]s.%[4]s)`, rs.Namespace, warehouseutils.UsersTable, stagingTableName, primaryKey)

	_, err = tx.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("RS: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)
		pkgLogger.Errorf("RS: Error deleting from original table for dedup: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	sqlStatement = fmt.Sprintf(`INSERT INTO "%[1]s"."%[2]s" (%[4]s) SELECT %[4]s FROM  "%[1]s"."%[3]s"`, rs.Namespace, warehouseutils.UsersTable, stagingTableName, warehouseutils.DoubleQuoteAndJoinByComma(append([]string{"id"}, userColNames...)))
	pkgLogger.Infof("RS: Inserting records for table:%s using staging table: %s\n", warehouseutils.UsersTable,
		sqlStatement)
	_, err = tx.Exec(sqlStatement)

	if err != nil {
		pkgLogger.Errorf("RS: Error inserting into users table from staging table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("RS: Error in transaction commit for users table: %v\n", err)
		tx.Rollback()
		errorMap[warehouseutils.UsersTable] = err
		return
	}
	return
}

type RedshiftCredentialsT struct {
	Host     string
	Port     string
	DbName   string
	Username string
	Password string
	timeout  time.Duration
}

func Connect(cred RedshiftCredentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("sslmode=require user=%v password=%v host=%v port=%v dbname=%v",
		cred.Username,
		cred.Password,
		cred.Host,
		cred.Port,
		cred.DbName,
	)
	if cred.timeout > 0 {
		url += fmt.Sprintf(" connect_timeout=%d", cred.timeout/time.Second)
	}

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("redshift connect error : (%v)", err)
	}
	stmt := `SET query_group to 'RudderStack'`
	_, err = db.Exec(stmt)
	if err != nil {
		return nil, fmt.Errorf("redshift set query_group error : %v", err)
	}
	return db, nil
}

func (rs *HandleT) dropDanglingStagingTables() bool {
	sqlStatement := `
		select
		  table_name
		from
		  information_schema.tables
		where
		  table_schema = $1
		  AND table_name like $2;
	`
	rows, err := rs.Db.Query(
		sqlStatement,
		rs.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if err != nil {
		pkgLogger.Errorf("WH: RS: Error dropping dangling staging tables in redshift: %v\nQuery: %s\n", err, sqlStatement)
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
	pkgLogger.Infof("WH: RS: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := rs.Db.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			pkgLogger.Errorf("WH: RS:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (rs *HandleT) connectToWarehouse() (*sql.DB, error) {
	return Connect(rs.getConnectionCredentials())
}

func (rs *HandleT) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = rs.schemaExists(rs.Namespace)
	if err != nil {
		pkgLogger.Errorf("RS: Error checking if schema: %s exists: %v", rs.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("RS: Skipping creating schema: %s since it already exists", rs.Namespace)
		return
	}
	return rs.createSchema()
}

func (rs *HandleT) AlterColumn(tableName, columnName, columnType string) (err error) {
	if setVarCharMax && columnType == "text" {
		err = rs.alterStringToText(fmt.Sprintf(`%q.%q`, rs.Namespace, tableName), columnName)
	}
	return
}

func (rs *HandleT) getConnectionCredentials() RedshiftCredentialsT {
	return RedshiftCredentialsT{
		Host:     warehouseutils.GetConfigValue(RSHost, rs.Warehouse),
		Port:     warehouseutils.GetConfigValue(RSPort, rs.Warehouse),
		DbName:   warehouseutils.GetConfigValue(RSDbName, rs.Warehouse),
		Username: warehouseutils.GetConfigValue(RSUserName, rs.Warehouse),
		Password: warehouseutils.GetConfigValue(RSPassword, rs.Warehouse),
		timeout:  rs.ConnectTimeout,
	}
}

// FetchSchema queries redshift and returns the schema associated with provided namespace
func (rs *HandleT) FetchSchema(warehouse warehouseutils.Warehouse) (schema, unrecognizedSchema warehouseutils.SchemaT, err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	dbHandle, err := Connect(rs.getConnectionCredentials())
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
			  data_type,
			  character_maximum_length
			FROM
			  INFORMATION_SCHEMA.COLUMNS
			WHERE
			  table_schema = $1
			  and table_name not like $2;
		`

	rows, err := dbHandle.Query(
		sqlStatement,
		rs.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("RS: Error in fetching schema from redshift destination:%v, query: %v", rs.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pkgLogger.Infof("RS: No rows, while fetching schema from  destination:%v, query: %v", rs.Warehouse.Identifier,
			sqlStatement)
		return schema, unrecognizedSchema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		var charLength sql.NullInt64
		err = rows.Scan(&tName, &cName, &cType, &charLength)
		if err != nil {
			pkgLogger.Errorf("RS: Error in processing fetched schema from redshift destination:%v", rs.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := dataTypesMapToRudder[cType]; ok {
			if datatype == "string" && charLength.Int64 > rudderStringLength {
				datatype = "text"
			}
			schema[tName][cName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tName]; !ok {
				unrecognizedSchema[tName] = make(map[string]string)
			}
			unrecognizedSchema[tName][cName] = warehouseutils.MISSING_DATATYPE

			warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &rs.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType}).Count(1)
		}
	}
	return
}

func (rs *HandleT) Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	rs.Uploader = uploader

	rs.Db, err = rs.connectToWarehouse()
	return err
}

func (rs *HandleT) TestConnection(warehouse warehouseutils.Warehouse) (err error) {
	rs.Warehouse = warehouse
	rs.Db, err = Connect(rs.getConnectionCredentials())
	if err != nil {
		return
	}
	defer rs.Db.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), rs.ConnectTimeout)
	defer cancel()

	err = rs.Db.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", rs.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return
}

func (rs *HandleT) Cleanup() {
	if rs.Db != nil {
		rs.dropDanglingStagingTables()
		rs.Db.Close()
	}
}

func (rs *HandleT) CrashRecover(warehouse warehouseutils.Warehouse) (err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	rs.Db, err = Connect(rs.getConnectionCredentials())
	if err != nil {
		return err
	}
	defer rs.Db.Close()
	rs.dropDanglingStagingTables()
	return
}

func (*HandleT) IsEmpty(_ warehouseutils.Warehouse) (empty bool, err error) {
	return
}

func (rs *HandleT) LoadUserTables() map[string]error {
	return rs.loadUserTables()
}

func (rs *HandleT) LoadTable(tableName string) error {
	_, err := rs.loadTable(tableName, rs.Uploader.GetTableSchemaInUpload(tableName), rs.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
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

func (rs *HandleT) GetTotalCountInTable(ctx context.Context, tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM "%[1]s"."%[2]s"`, rs.Namespace, tableName)
	err = rs.Db.QueryRowContext(ctx, sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`RS: Error getting total count in table %s:%s`, rs.Namespace, tableName)
	}
	return
}

func (rs *HandleT) Connect(warehouse warehouseutils.Warehouse) (client.Client, error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	dbHandle, err := Connect(rs.getConnectionCredentials())
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (rs *HandleT) LoadTestTable(location, tableName string, _ map[string]interface{}, format string) (err error) {
	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&rs.Warehouse.Destination)
	if err != nil {
		pkgLogger.Errorf("RS: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
		return
	}

	manifestS3Location, region := warehouseutils.GetS3Location(location)
	if region == "" {
		region = "us-east-1"
	}

	var sqlStatement string
	if format == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		// copy statement for parquet load files
		sqlStatement = fmt.Sprintf(`COPY %v FROM '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' FORMAT PARQUET`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, tableName),
			manifestS3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
		)
	} else {
		// copy statement for csv load files
		sqlStatement = fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' REGION '%s'  DATEFORMAT 'auto' TIMEFORMAT 'auto' TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, tableName),
			fmt.Sprintf(`%q, %q`, "id", "val"),
			manifestS3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
			region,
		)
	}
	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
		"SESSION_TOKEN '[^']*'":     "SESSION_TOKEN '***'",
	})
	if regexErr == nil {
		pkgLogger.Infof("RS: Running COPY command for load test table: %s with sqlStatement: %s", tableName, sanitisedSQLStmt)
	}

	_, err = rs.Db.Exec(sqlStatement)
	return
}

func (rs *HandleT) SetConnectionTimeout(timeout time.Duration) {
	rs.ConnectTimeout = timeout
}

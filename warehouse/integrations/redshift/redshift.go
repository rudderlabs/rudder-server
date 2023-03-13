package redshift

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/lib/pq"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var pkgLogger logger.Logger

var errorsMappings = []model.JobError{
	{
		Type:   model.AlterColumnError,
		Format: regexp.MustCompile(`pq: cannot alter type of a column used by a view or rule`),
	},
	{
		Type:   model.InsufficientResourceError,
		Format: regexp.MustCompile(`pq: Disk Full`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`redshift set query_group error : EOF`),
	},
	{
		Type:   model.ConcurrentQueriesError,
		Format: regexp.MustCompile(`pq: 1023`),
	},
	{
		Type:   model.ColumnSizeError,
		Format: regexp.MustCompile(`pq: Value too long for character type`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: permission denied for database`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: must be owner of relation`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: Cannot execute write query because system is in resize mode`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: SSL is not enabled on the server`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`Bucket .* not found`),
	},
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
	"string":   "varchar(65535)",
	"text":     "varchar(65535)",
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

type Redshift struct {
	DB             *sql.DB
	Namespace      string
	Warehouse      model.Warehouse
	Uploader       warehouseutils.Uploader
	ConnectTimeout time.Duration
	Logger         logger.Logger
	stats          stats.Stats

	DedupWindow                   bool
	DedupWindowInHours            time.Duration
	SkipDedupDestinationIDs       []string
	SkipComputingUserLatestTraits bool
	EnableDeleteByJobs            bool
}

type S3ManifestEntryMetadata struct {
	ContentLength int64 `json:"content_length"`
}

type S3ManifestEntry struct {
	Url       string                  `json:"url"`
	Mandatory bool                    `json:"mandatory"`
	Metadata  S3ManifestEntryMetadata `json:"meta"`
}

type S3Manifest struct {
	Entries []S3ManifestEntry `json:"entries"`
}

type RedshiftCredentials struct {
	Host       string
	Port       string
	DbName     string
	Username   string
	Password   string
	timeout    time.Duration
	TunnelInfo *tunnelling.TunnelInfo
}

func NewRedshift() *Redshift {
	return &Redshift{
		Logger: pkgLogger,
		stats:  stats.Default,
	}
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("redshift")
}

func WithConfig(h *Redshift, config *config.Config) {
	h.DedupWindow = config.GetBool("Warehouse.redshift.dedupWindow", false)
	h.DedupWindowInHours = config.GetDuration("Warehouse.redshift.dedupWindowInHours", 720, time.Hour)
	h.SkipDedupDestinationIDs = config.GetStringSlice("Warehouse.redshift.skipDedupDestinationIDs", nil)
	h.SkipComputingUserLatestTraits = config.GetBool("Warehouse.redshift.skipComputingUserLatestTraits", false)
	h.EnableDeleteByJobs = config.GetBool("Warehouse.redshift.enableDeleteByJobs", false)
}

// getRSDataType gets datatype for rs which is mapped with RudderStack datatype
func getRSDataType(columnType string) string {
	return dataTypesMap[columnType]
}

func ColumnsWithDataTypes(columns model.TableSchema, prefix string) string {
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

func (rs *Redshift) CreateTable(tableName string, columns model.TableSchema) (err error) {
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
	rs.Logger.Infof("Creating table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.DB.Exec(sqlStatement)
	return
}

func (rs *Redshift) DropTable(tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	rs.Logger.Infof("RS: Dropping table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.DB.Exec(fmt.Sprintf(sqlStatement, rs.Namespace, tableName))
	return
}

func (rs *Redshift) schemaExists(_ string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, rs.Namespace)
	err = rs.DB.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (rs *Redshift) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	for _, columnInfo := range columnsInfo {
		columnType := getRSDataType(columnInfo.Type)
		query := fmt.Sprintf(`
			ALTER TABLE
			  %q.%q
			ADD
			  COLUMN %q %s;
	`,
			rs.Namespace,
			tableName,
			columnInfo.Name,
			columnType,
		)
		rs.Logger.Infof("RS: Adding column for destinationID: %s, tableName: %s with query: %v", rs.Warehouse.Destination.ID, tableName, query)

		if _, err := rs.DB.Exec(query); err != nil {
			if CheckAndIgnoreColumnAlreadyExistError(err) {
				rs.Logger.Infow("column already exists",
					logfield.SourceID, rs.Warehouse.Source.ID,
					logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
					logfield.DestinationID, rs.Warehouse.Destination.ID,
					logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
					logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
					logfield.Schema, rs.Namespace,
					logfield.TableName, tableName,
					logfield.ColumnName, columnInfo.Name,
					logfield.ColumnType, columnType,
					logfield.Error, err.Error(),
					logfield.Query, query,
				)
				continue
			}

			return err
		}
	}
	return nil
}

func CheckAndIgnoreColumnAlreadyExistError(err error) bool {
	if err != nil {
		if e, ok := err.(*pq.Error); ok {
			if e.Code == "42701" {
				return true
			}
		}
		return false
	}
	return true
}

func (rs *Redshift) DeleteBy(tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	rs.Logger.Infof("RS: Cleaning up the following tables in redshift for RS:%s : %+v", tableNames, params)
	rs.Logger.Infof("RS: Flag for enableDeleteByJobs is %t", rs.EnableDeleteByJobs)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
		context_sources_job_run_id <> $1 AND
		context_sources_task_run_id <> $2 AND
		context_source_id = $3 AND
		received_at < $4`,
			rs.Namespace,
			tb,
		)

		rs.Logger.Infof("RS: Deleting rows in table in redshift for RS:%s", rs.Warehouse.Destination.ID)
		rs.Logger.Debugf("RS: Executing the query %v", sqlStatement)

		if rs.EnableDeleteByJobs {
			_, err = rs.DB.Exec(sqlStatement,
				params.JobRunId,
				params.TaskRunId,
				params.SourceId,
				params.StartTime,
			)
			if err != nil {
				rs.Logger.Errorf("Error in executing the query %s", err.Error)
				return err
			}
		}

	}
	return nil
}

func (rs *Redshift) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, rs.Namespace)
	rs.Logger.Infof("Creating schema name in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.DB.Exec(sqlStatement)
	return
}

func (rs *Redshift) generateManifest(tableName string) (string, error) {
	loadFiles := rs.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions{Table: tableName})
	loadFiles = warehouseutils.GetS3Locations(loadFiles)
	var manifest S3Manifest
	for idx, loadFile := range loadFiles {
		manifestEntry := S3ManifestEntry{Url: loadFile.Location, Mandatory: true}
		// add contentLength to manifest entry if it exists
		contentLength := gjson.Get(string(loadFiles[idx].Metadata), "content_length")
		if contentLength.Exists() {
			manifestEntry.Metadata.ContentLength = contentLength.Int()
		}
		manifest.Entries = append(manifest.Entries, manifestEntry)
	}
	rs.Logger.Infof("RS: Generated manifest for table:%s", tableName)
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

func (rs *Redshift) dropStagingTables(stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		rs.Logger.Infof("WH: dropping table %+v\n", stagingTableName)
		_, err := rs.DB.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			rs.Logger.Errorf("WH: RS:  Error dropping staging tables in redshift: %v", err)
		}
	}
}

func (rs *Redshift) loadTable(tableName string, tableSchemaInUpload, tableSchemaAfterUpload model.TableSchema, skipTempTableDelete bool) (string, error) {
	var (
		err              error
		query            string
		stagingTableName string
		rowsAffected     int64
		txn              *sql.Tx
		result           sql.Result
	)

	rs.Logger.Infow("started loading",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, tableName,
	)

	manifestLocation, err := rs.generateManifest(tableName)
	if err != nil {
		return "", fmt.Errorf("generating manifest: %w", err)
	}

	rs.Logger.Infow("Generated manifest",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, tableName,
		"manifestLocation", manifestLocation,
	)

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)
	sortedColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`%q`, name)
	}, ",")

	stagingTableName = warehouseutils.StagingTableName(provider, tableName, tableNameLimit)
	if err = rs.CreateTable(stagingTableName, tableSchemaAfterUpload); err != nil {
		return "", fmt.Errorf("creating staging table: %w", err)
	}

	if !skipTempTableDelete {
		defer rs.dropStagingTables([]string{stagingTableName})
	}

	manifestS3Location, region := warehouseutils.GetS3Location(manifestLocation)
	if region == "" {
		region = "us-east-1"
	}

	// create session token and temporary credentials
	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&rs.Warehouse.Destination)
	if err != nil {
		rs.Logger.Warnw("getting temporary s3 credentials",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, tableName,
			logfield.Error, err.Error(),
		)
		return "", fmt.Errorf("getting temporary s3 credentials: %w", err)
	}

	if txn, err = rs.DB.Begin(); err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	if rs.Uploader.GetLoadFileType() == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		query = fmt.Sprintf(`
			COPY %v
			FROM '%s'
			ACCESS_KEY_ID '%s'
			SECRET_ACCESS_KEY '%s'
			SESSION_TOKEN '%s'
			MANIFEST
			FORMAT PARQUET;
		`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, stagingTableName),
			manifestS3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
		)
	} else {
		query = fmt.Sprintf(`
			COPY %v(%v)
			FROM '%v'
			CSV
			GZIP
			ACCESS_KEY_ID '%s'
			SECRET_ACCESS_KEY '%s'
			SESSION_TOKEN '%s'
			REGION '%s'
			DATEFORMAT 'auto'
			TIMEFORMAT 'auto'
			MANIFEST
			TRUNCATECOLUMNS
			EMPTYASNULL
			BLANKSASNULL
			FILLRECORD
			ACCEPTANYDATE
			TRIMBLANKS
			ACCEPTINVCHARS
			COMPUPDATE OFF
			STATUPDATE OFF;
		`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, stagingTableName),
			sortedColumnNames,
			manifestS3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
			region,
		)
	}

	sanitisedQuery, regexErr := misc.ReplaceMultiRegex(query, map[string]string{
		"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
		"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
		"SESSION_TOKEN '[^']*'":     "SESSION_TOKEN '***'",
	})
	if regexErr != nil {
		sanitisedQuery = ""
	}

	rs.Logger.Infow("copy command",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, tableName,
		logfield.Query, sanitisedQuery,
	)

	if _, err := txn.Exec(query); err != nil {
		rs.Logger.Warnw("failure running copy command",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, tableName,
			logfield.Query, sanitisedQuery,
			logfield.Error, err.Error(),
		)

		return "", fmt.Errorf("running copy command: %w", err)
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

	// Deduplication
	// Delete rows from the table which are already present in the staging table
	query = fmt.Sprintf(`
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

	if rs.DedupWindow {
		if _, ok := tableSchemaAfterUpload["received_at"]; ok {
			query += fmt.Sprintf(`
				AND %[1]s.%[2]q.received_at > GETDATE() - INTERVAL '%[3]d HOUR'
`,
				rs.Namespace,
				tableName,
				rs.DedupWindowInHours/time.Hour,
			)
		}
	}

	if tableName == warehouseutils.DiscardsTable {
		query += fmt.Sprintf(`
			AND _source.%[3]s = %[1]s.%[2]q.%[3]s
			AND _source.%[4]s = %[1]s.%[2]q.%[4]s
`,
			rs.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	if !slices.Contains(rs.SkipDedupDestinationIDs, rs.Warehouse.Destination.ID) {
		rs.Logger.Infow("deduplication",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, tableName,
			logfield.Query, query,
		)

		if result, err = txn.Exec(query); err != nil {
			rs.Logger.Warnw("deleting from original table for dedup",
				logfield.SourceID, rs.Warehouse.Source.ID,
				logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, rs.Warehouse.Destination.ID,
				logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
				logfield.Namespace, rs.Namespace,
				logfield.TableName, tableName,
				logfield.Query, query,
				logfield.Error, err.Error(),
			)
			return "", fmt.Errorf("deleting from original table for dedup: %w", err)
		}

		if rowsAffected, err = result.RowsAffected(); err != nil {
			rs.Logger.Warnw("getting rows affected for dedup",
				logfield.SourceID, rs.Warehouse.Source.ID,
				logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, rs.Warehouse.Destination.ID,
				logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
				logfield.Namespace, rs.Namespace,
				logfield.TableName, tableName,
				logfield.Query, query,
				logfield.Error, err.Error(),
			)

			return "", fmt.Errorf("getting rows affected for dedup: %w", err)
		}

		rs.stats.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
			"sourceID":    rs.Warehouse.Source.ID,
			"sourceType":  rs.Warehouse.Source.SourceDefinition.Name,
			"destID":      rs.Warehouse.Destination.ID,
			"destType":    rs.Warehouse.Destination.DestinationDefinition.Name,
			"workspaceId": rs.Warehouse.WorkspaceID,
			"namespace":   rs.Namespace,
			"tableName":   tableName,
		}).Count(int(rowsAffected))
	}

	// Deduplication
	// Insert rows from staging table to the original table
	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(strKeys)
	query = fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (%[3]s)
		SELECT
		  %[3]s
		FROM
		  (
			SELECT
			  *,
			  row_number() OVER (
				PARTITION BY %[5]s
				ORDER BY
				  received_at ASC
			  ) AS _rudder_staging_row_number
			FROM
			  %[1]q.%[4]q
		  ) AS _
		WHERE
		  _rudder_staging_row_number = 1;
`,
		rs.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	rs.Logger.Infow("inserting into original table",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, tableName,
		logfield.Query, query,
	)

	if _, err = txn.Exec(query); err != nil {
		rs.Logger.Warnw("failed inserting into original table",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, tableName,
			logfield.Error, err.Error(),
		)

		return "", fmt.Errorf("inserting into original table: %w", err)
	}

	if err = txn.Commit(); err != nil {
		rs.Logger.Warnw("committing transaction",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, tableName,
			logfield.Error, err.Error(),
		)

		return "", fmt.Errorf("committing transaction: %w", err)
	}

	rs.Logger.Infow("completed loading",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, tableName,
	)

	return stagingTableName, nil
}

func (rs *Redshift) loadUserTables() map[string]error {
	var (
		err                  error
		query                string
		identifyStagingTable string
		txn                  *sql.Tx
		userColNames         []string
		firstValProps        []string
	)

	rs.Logger.Infow("started loading for identifies and users tables",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
	)

	identifyStagingTable, err = rs.loadTable(
		warehouseutils.IdentifiesTable,
		rs.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable),
		rs.Uploader.GetTableSchemaInWarehouse(warehouseutils.IdentifiesTable),
		true,
	)
	if err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("loading identifies table: %w", err),
		}
	}

	defer rs.dropStagingTables([]string{identifyStagingTable})

	if len(rs.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
		}
	}

	if rs.SkipComputingUserLatestTraits {
		_, err := rs.loadTable(
			warehouseutils.UsersTable,
			rs.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable),
			rs.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable),
			false,
		)
		if err != nil {
			return map[string]error{
				warehouseutils.IdentifiesTable: nil,
				warehouseutils.UsersTable:      fmt.Errorf("loading users table: %w", err),
			}
		}
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      nil,
		}
	}

	userColMap := rs.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
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

	query = fmt.Sprintf(`
		CREATE TABLE %[1]q.%[2]q AS (
		  SELECT
			DISTINCT *
		  FROM
			(
			  SELECT
				id,
				%[3]s
			  FROM
				(
				  (
					SELECT
					  id,
					  %[6]s
					FROM
					  %[1]q.%[4]q
					WHERE
					  id in (
						SELECT
						  DISTINCT(user_id)
						FROM
						  %[1]q.%[5]q
						WHERE
						  user_id IS NOT NULL
					  )
				  )
				  UNION
					(
					  SELECT
						user_id,
						%[6]s
					  FROM
						%[1]q.%[5]q
					  WHERE
						user_id IS NOT NULL
					)
				)
			)
		);
`,
		rs.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		warehouseutils.UsersTable,
		identifyStagingTable,
		quotedUserColNames,
	)

	if txn, err = rs.DB.Begin(); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("beginning transaction: %w", err),
		}
	}

	if _, err = txn.Exec(query); err != nil {
		_ = txn.Rollback()

		rs.Logger.Warnw("creating staging table for users",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, warehouseutils.UsersTable,
			logfield.Error, err.Error(),
		)
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating staging table for users: %w", err),
		}
	}
	defer rs.dropStagingTables([]string{stagingTableName})

	primaryKey := "id"
	query = fmt.Sprintf(`
		DELETE FROM
		  %[1]s.%[2]q USING %[1]s.%[3]q _source
		WHERE
		  (
			_source.%[4]s = %[1]s.%[2]s.%[4]s
		  );
`,
		rs.Namespace,
		warehouseutils.UsersTable,
		stagingTableName,
		primaryKey,
	)

	if _, err = txn.Exec(query); err != nil {
		_ = txn.Rollback()

		rs.Logger.Warnw("deleting from users table for dedup",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.Query, query,
			logfield.TableName, warehouseutils.UsersTable,
			logfield.Error, err.Error(),
		)
		return map[string]error{
			warehouseutils.UsersTable: fmt.Errorf("deleting from original table for dedup: %w", err),
		}
	}

	query = fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (%[4]s)
		SELECT
		  %[4]s
		FROM
		  %[1]q.%[3]q;
`,
		rs.Namespace,
		warehouseutils.UsersTable,
		stagingTableName,
		warehouseutils.DoubleQuoteAndJoinByComma(append([]string{"id"}, userColNames...)),
	)

	rs.Logger.Infow("inserting into users table",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, warehouseutils.UsersTable,
		logfield.Query, query,
	)

	if _, err = txn.Exec(query); err != nil {
		_ = txn.Rollback()

		rs.Logger.Warnw("failed inserting into users table",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, warehouseutils.UsersTable,
			logfield.Error, err.Error(),
		)

		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("inserting into users table from staging table: %w", err),
		}
	}

	if err = txn.Commit(); err != nil {
		_ = txn.Rollback()

		rs.Logger.Warnw("committing transaction for user table",
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Namespace, rs.Namespace,
			logfield.TableName, warehouseutils.UsersTable,
			logfield.Error, err.Error(),
		)

		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("committing transaction: %w", err),
		}
	}

	rs.Logger.Infow("completed loading for users and identifies tables",
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
	)

	return map[string]error{
		warehouseutils.IdentifiesTable: nil,
		warehouseutils.UsersTable:      nil,
	}
}

func Connect(cred RedshiftCredentials) (*sql.DB, error) {
	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cred.Username, cred.Password),
		Host:   fmt.Sprintf("%s:%s", cred.Host, cred.Port),
		Path:   cred.DbName,
	}

	params := url.Values{}
	params.Add("sslmode", "require")

	if cred.timeout > 0 {
		params.Add("connect_timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}

	dsn.RawQuery = params.Encode()

	var (
		err error
		db  *sql.DB
	)

	if cred.TunnelInfo != nil {
		if db, err = tunnelling.SQLConnectThroughTunnel(dsn.String(), cred.TunnelInfo.Config); err != nil {
			return nil, fmt.Errorf("connecting to redshift through tunnel: %w", err)
		}
	} else {
		if db, err = sql.Open("postgres", dsn.String()); err != nil {
			return nil, fmt.Errorf("connecting to redshift: %w", err)
		}
	}

	stmt := `SET query_group to 'RudderStack'`
	_, err = db.Exec(stmt)
	if err != nil {
		return nil, fmt.Errorf("redshift set query_group error : %v", err)
	}
	return db, nil
}

func (rs *Redshift) dropDanglingStagingTables() bool {
	sqlStatement := `
		SELECT
		  table_name
		FROM
		  information_schema.tables
		WHERE
		  table_schema = $1 AND
		  table_name like $2;
	`
	rows, err := rs.DB.Query(
		sqlStatement,
		rs.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if err != nil {
		rs.Logger.Errorf("WH: RS: Error dropping dangling staging tables in redshift: %v\nQuery: %s\n", err, sqlStatement)
		return false
	}
	defer func() { _ = rows.Close() }()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	rs.Logger.Infof("WH: RS: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		_, err := rs.DB.Exec(fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			rs.Logger.Errorf("WH: RS:  Error dropping dangling staging table: %s in redshift: %v\n", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (rs *Redshift) connectToWarehouse() (*sql.DB, error) {
	return Connect(rs.getConnectionCredentials())
}

func (rs *Redshift) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = rs.schemaExists(rs.Namespace)
	if err != nil {
		rs.Logger.Errorf("RS: Error checking if schema: %s exists: %v", rs.Namespace, err)
		return err
	}
	if schemaExists {
		rs.Logger.Infof("RS: Skipping creating schema: %s since it already exists", rs.Namespace)
		return
	}
	return rs.createSchema()
}

func (rs *Redshift) AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	var (
		query                string
		stagingColumnName    string
		stagingColumnType    string
		deprecatedColumnName string
		isDependent          bool
		tx                   *sql.Tx
		err                  error
		ctx                  = context.TODO()
	)

	// Begin a transaction
	if tx, err = rs.DB.BeginTx(ctx, &sql.TxOptions{}); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
	}()

	// creating staging column
	stagingColumnType = getRSDataType(columnType)
	stagingColumnName = fmt.Sprintf(`%s-staging-%s`, columnName, misc.FastUUID().String())
	query = fmt.Sprintf(`
		ALTER TABLE
		  %q.%q
		ADD
		  COLUMN %q %s;
	`,
		rs.Namespace,
		tableName,
		stagingColumnName,
		stagingColumnType,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("add staging column: %w", err)
	}

	// populating staging column
	query = fmt.Sprintf(`
		UPDATE
		  %[1]q.%[2]q
		SET
		  %[3]q = CAST (%[4]q AS %[5]s)
		WHERE
		  %[4]q IS NOT NULL;
	`,
		rs.Namespace,
		tableName,
		stagingColumnName,
		columnName,
		stagingColumnType,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("populate staging column: %w", err)
	}

	// renaming original column to deprecated column
	deprecatedColumnName = fmt.Sprintf(`%s-deprecated-%s`, columnName, misc.FastUUID().String())
	query = fmt.Sprintf(`
		ALTER TABLE
		  %[1]q.%[2]q
		RENAME COLUMN
		  %[3]q TO %[4]q;
	`,
		rs.Namespace,
		tableName,
		columnName,
		deprecatedColumnName,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("rename original column: %w", err)
	}

	// renaming staging column to original column
	query = fmt.Sprintf(`
		ALTER TABLE
		  %[1]q.%[2]q
		RENAME COLUMN
		  %[3]q TO %[4]q;
	`,
		rs.Namespace,
		tableName,
		stagingColumnName,
		columnName,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("rename staging column: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("commit transaction: %w", err)
	}

	// dropping deprecated column
	// Since dropping the column can fail, we need to do it outside the transaction
	// Because if will fail during the commit of the transaction
	// https://github.com/lib/pq/blob/d5affd5073b06f745459768de35356df2e5fd91d/conn.go#L600
	query = fmt.Sprintf(`
		ALTER TABLE
		  %[1]q.%[2]q
		DROP COLUMN
		  %[3]q;
	`,
		rs.Namespace,
		tableName,
		deprecatedColumnName,
	)
	if _, err = rs.DB.ExecContext(ctx, query); err != nil {
		if pqError, ok := err.(*pq.Error); !ok || pqError.Code != "2BP01" {
			return model.AlterTableResponse{}, fmt.Errorf("drop deprecated column: %w", err)
		}

		isDependent = true
		err = nil
	}

	res := model.AlterTableResponse{
		IsDependent: isDependent,
		Query: fmt.Sprintf(`ALTER TABLE %[1]q.%[2]q DROP COLUMN %[3]q CASCADE;`,
			rs.Namespace,
			tableName,
			deprecatedColumnName,
		),
	}

	return res, nil
}

func (rs *Redshift) getConnectionCredentials() RedshiftCredentials {
	creds := RedshiftCredentials{
		Host:       warehouseutils.GetConfigValue(RSHost, rs.Warehouse),
		Port:       warehouseutils.GetConfigValue(RSPort, rs.Warehouse),
		DbName:     warehouseutils.GetConfigValue(RSDbName, rs.Warehouse),
		Username:   warehouseutils.GetConfigValue(RSUserName, rs.Warehouse),
		Password:   warehouseutils.GetConfigValue(RSPassword, rs.Warehouse),
		timeout:    rs.ConnectTimeout,
		TunnelInfo: warehouseutils.ExtractTunnelInfoFromDestinationConfig(rs.Warehouse.Destination.Config),
	}

	return creds
}

// FetchSchema queries redshift and returns the schema associated with provided namespace
func (rs *Redshift) FetchSchema(warehouse model.Warehouse) (schema, unrecognizedSchema model.Schema, err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	dbHandle, err := Connect(rs.getConnectionCredentials())
	if err != nil {
		return
	}
	defer func() { _ = dbHandle.Close() }()

	schema = make(model.Schema)
	unrecognizedSchema = make(model.Schema)

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
		rs.Logger.Errorf("RS: Error in fetching schema from redshift destination:%v, query: %v", rs.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		rs.Logger.Infof("RS: No rows, while fetching schema from  destination:%v, query: %v", rs.Warehouse.Identifier,
			sqlStatement)
		return schema, unrecognizedSchema, nil
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var tName, cName, cType string
		var charLength sql.NullInt64
		err = rows.Scan(&tName, &cName, &cType, &charLength)
		if err != nil {
			rs.Logger.Errorf("RS: Error in processing fetched schema from redshift destination:%v", rs.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(model.TableSchema)
		}
		if datatype, ok := dataTypesMapToRudder[cType]; ok {
			if datatype == "string" && charLength.Int64 > rudderStringLength {
				datatype = "text"
			}
			schema[tName][cName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tName]; !ok {
				unrecognizedSchema[tName] = make(model.TableSchema)
			}
			unrecognizedSchema[tName][cName] = warehouseutils.MISSING_DATATYPE

			warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &rs.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType}).Count(1)
		}
	}
	return
}

func (rs *Redshift) Setup(warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	rs.Uploader = uploader

	rs.DB, err = rs.connectToWarehouse()
	return err
}

func (rs *Redshift) TestConnection(warehouse model.Warehouse) (err error) {
	rs.Warehouse = warehouse
	rs.DB, err = Connect(rs.getConnectionCredentials())
	if err != nil {
		return
	}
	defer func() { _ = rs.DB.Close() }()

	ctx, cancel := context.WithTimeout(context.TODO(), rs.ConnectTimeout)
	defer cancel()

	err = rs.DB.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", rs.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return
}

func (rs *Redshift) Cleanup() {
	if rs.DB != nil {
		rs.dropDanglingStagingTables()
		_ = rs.DB.Close()
	}
}

func (rs *Redshift) CrashRecover(warehouse model.Warehouse) (err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	rs.DB, err = Connect(rs.getConnectionCredentials())
	if err != nil {
		return err
	}
	defer func() { _ = rs.DB.Close() }()
	rs.dropDanglingStagingTables()
	return
}

func (*Redshift) IsEmpty(_ model.Warehouse) (empty bool, err error) {
	return
}

func (rs *Redshift) LoadUserTables() map[string]error {
	return rs.loadUserTables()
}

func (rs *Redshift) LoadTable(tableName string) error {
	_, err := rs.loadTable(tableName, rs.Uploader.GetTableSchemaInUpload(tableName), rs.Uploader.GetTableSchemaInWarehouse(tableName), false)
	return err
}

func (*Redshift) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (*Redshift) LoadIdentityMappingsTable() (err error) {
	return
}

func (*Redshift) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (rs *Redshift) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		rs.Namespace,
		tableName,
	)
	err = rs.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (rs *Redshift) Connect(warehouse model.Warehouse) (client.Client, error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	dbHandle, err := Connect(rs.getConnectionCredentials())
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: dbHandle}, err
}

func (rs *Redshift) LoadTestTable(location, tableName string, _ map[string]interface{}, format string) (err error) {
	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&rs.Warehouse.Destination)
	if err != nil {
		rs.Logger.Errorf("RS: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
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
		rs.Logger.Infof("RS: Running COPY command for load test table: %s with sqlStatement: %s", tableName, sanitisedSQLStmt)
	}

	_, err = rs.DB.Exec(sqlStatement)
	return
}

func (rs *Redshift) SetConnectionTimeout(timeout time.Duration) {
	rs.ConnectTimeout = timeout
}

func (rs *Redshift) ErrorMappings() []model.JobError {
	return errorsMappings
}

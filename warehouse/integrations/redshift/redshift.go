package redshift

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	sqlconnectconfig "github.com/rudderlabs/sqlconnect-go/sqlconnect/config"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/tunnelling"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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
	{
		Type:   model.ColumnCountError,
		Format: regexp.MustCompile(`pq: tables can have at most 1600 columns`),
	},
}

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
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id",
}

var partitionKeyMap = map[string]string{
	warehouseutils.UsersTable:      "id",
	warehouseutils.IdentifiesTable: "id",
	warehouseutils.DiscardsTable:   "row_id, column_name, table_name",
}

type Redshift struct {
	DB             *sqlmiddleware.DB
	Namespace      string
	Warehouse      model.Warehouse
	Uploader       warehouseutils.Uploader
	connectTimeout time.Duration
	conf           *config.Config
	logger         logger.Logger
	stats          stats.Stats

	config struct {
		allowMerge                    bool
		slowQueryThreshold            time.Duration
		dedupWindow                   bool
		dedupWindowInHours            time.Duration
		skipDedupDestinationIDs       []string
		skipComputingUserLatestTraits bool
		enableDeleteByJobs            bool
		loadByFolderPath              bool
	}
}

type s3ManifestEntryMetadata struct {
	ContentLength int64 `json:"content_length"`
}

type s3ManifestEntry struct {
	Url       string                  `json:"url"`
	Mandatory bool                    `json:"mandatory"`
	Metadata  s3ManifestEntryMetadata `json:"meta"`
}

type s3Manifest struct {
	Entries []s3ManifestEntry `json:"entries"`
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats) *Redshift {
	rs := &Redshift{}

	rs.conf = conf
	rs.logger = log.Child("integrations").Child("redshift")
	rs.stats = stat

	rs.config.allowMerge = conf.GetBool("Warehouse.redshift.allowMerge", true)
	rs.config.dedupWindow = conf.GetBool("Warehouse.redshift.dedupWindow", false)
	rs.config.dedupWindowInHours = conf.GetDuration("Warehouse.redshift.dedupWindowInHours", 720, time.Hour)
	rs.config.skipDedupDestinationIDs = conf.GetStringSlice("Warehouse.redshift.skipDedupDestinationIDs", nil)
	rs.config.skipComputingUserLatestTraits = conf.GetBool("Warehouse.redshift.skipComputingUserLatestTraits", false)
	rs.config.enableDeleteByJobs = conf.GetBool("Warehouse.redshift.enableDeleteByJobs", false)
	rs.config.slowQueryThreshold = conf.GetDuration("Warehouse.redshift.slowQueryThreshold", 5, time.Minute)
	rs.config.loadByFolderPath = conf.GetBool("Warehouse.redshift.loadByFolderPath", false)

	return rs
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

func (rs *Redshift) CreateTable(ctx context.Context, tableName string, columns model.TableSchema) (err error) {
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
	rs.logger.Infof("Creating table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.DB.ExecContext(ctx, sqlStatement)
	return
}

func (rs *Redshift) DropTable(ctx context.Context, tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	rs.logger.Infof("RS: Dropping table in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.DB.ExecContext(ctx, fmt.Sprintf(sqlStatement, rs.Namespace, tableName))
	return
}

func (rs *Redshift) schemaExists(ctx context.Context) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, rs.Namespace)
	err = rs.DB.QueryRowContext(ctx, sqlStatement).Scan(&exists)
	return
}

func (rs *Redshift) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
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
		rs.logger.Infof("RS: Adding column for destinationID: %s, tableName: %s with query: %v", rs.Warehouse.Destination.ID, tableName, query)

		if _, err := rs.DB.ExecContext(ctx, query); err != nil {
			if CheckAndIgnoreColumnAlreadyExistError(err) {
				rs.logger.Infow("column already exists",
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
		var e *pq.Error
		if errors.As(err, &e) {
			if e.Code == "42701" {
				return true
			}
		}
		return false
	}
	return true
}

func (rs *Redshift) DeleteBy(ctx context.Context, tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	rs.logger.Infof("RS: Cleaning up the following tables in redshift for RS:%s : %+v", tableNames, params)
	rs.logger.Infof("RS: Flag for enableDeleteByJobs is %t", rs.config.enableDeleteByJobs)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
			context_sources_job_run_id <> $1 AND
			context_sources_task_run_id <> $2 AND
			context_source_id = $3 AND
			received_at < $4`,
			rs.Namespace,
			tb,
		)

		rs.logger.Infof("RS: Deleting rows in table in redshift for RS:%s", rs.Warehouse.Destination.ID)
		rs.logger.Infof("RS: Executing the query %v", sqlStatement)

		if rs.config.enableDeleteByJobs {
			_, err = rs.DB.ExecContext(ctx, sqlStatement,
				params.JobRunId,
				params.TaskRunId,
				params.SourceId,
				params.StartTime,
			)
			if err != nil {
				rs.logger.Errorf("Error in executing the query %s", err.Error())
				return err
			}
		}

	}
	return nil
}

func (rs *Redshift) createSchema(ctx context.Context) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, rs.Namespace)
	rs.logger.Infof("Creating schema name in redshift for RS:%s : %v", rs.Warehouse.Destination.ID, sqlStatement)
	_, err = rs.DB.ExecContext(ctx, sqlStatement)
	return
}

func (rs *Redshift) generateManifest(ctx context.Context, tableName string) (string, error) {
	metadata, err := rs.Uploader.GetLoadFilesMetadata(
		ctx,
		warehouseutils.GetLoadFilesOptions{
			Table: tableName,
		},
	)
	if err != nil {
		return "", err
	}
	loadFiles := warehouseutils.GetS3Locations(metadata)

	entries := lo.Map(loadFiles, func(loadFile warehouseutils.LoadFile, index int) s3ManifestEntry {
		manifestEntry := s3ManifestEntry{
			Url:       loadFile.Location,
			Mandatory: true,
		}

		// add contentLength to manifest entry if it exists
		contentLength := gjson.Get(string(loadFile.Metadata), "content_length")
		if contentLength.Exists() {
			manifestEntry.Metadata.ContentLength = contentLength.Int()
		}

		return manifestEntry
	})

	manifestJSON, err := json.Marshal(&s3Manifest{
		Entries: entries,
	})
	if err != nil {
		return "", fmt.Errorf("marshalling manifest: %v", err)
	}

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}

	localManifestPath := tmpDirPath + "/" + misc.RudderRedshiftManifests + "/" + misc.FastUUID().String()
	err = os.MkdirAll(filepath.Dir(localManifestPath), os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("creating manifest directory: %v", err)
	}

	defer func() {
		misc.RemoveFilePaths(localManifestPath)
	}()

	err = os.WriteFile(localManifestPath, manifestJSON, 0o644)
	if err != nil {
		return "", fmt.Errorf("writing manifest to file: %v", err)
	}

	file, err := os.Open(localManifestPath)
	if err != nil {
		return "", fmt.Errorf("opening manifest file: %v", err)
	}
	defer func() { _ = file.Close() }()

	uploader, err := filemanager.New(&filemanager.Settings{
		Provider: warehouseutils.S3,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         warehouseutils.S3,
			Config:           rs.Warehouse.Destination.Config,
			UseRudderStorage: rs.Uploader.UseRudderStorage(),
			WorkspaceID:      rs.Warehouse.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		return "", fmt.Errorf("creating uploader: %w", err)
	}

	uploadOutput, err := uploader.Upload(
		ctx, file, misc.RudderRedshiftManifests,
		rs.Warehouse.Source.ID, rs.Warehouse.Destination.ID,
		time.Now().Format("01-02-2006"), tableName,
		misc.FastUUID().String(),
	)
	if err != nil {
		return "", fmt.Errorf("uploading manifest file: %w", err)
	}

	return uploadOutput.Location, nil
}

func (rs *Redshift) dropStagingTables(ctx context.Context, stagingTableNames []string) {
	for _, stagingTableName := range stagingTableNames {
		rs.logger.Infof("WH: dropping table %+v\n", stagingTableName)
		_, err := rs.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			rs.logger.Errorf("WH: RS:  Error dropping staging tables in redshift: %v", err)
		}
	}
}

func (rs *Redshift) loadTable(
	ctx context.Context,
	tableName string,
	tableSchemaInUpload,
	tableSchemaAfterUpload model.TableSchema,
	skipTempTableDelete bool,
) (*types.LoadTableStats, string, error) {
	log := rs.logger.With(
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.TableName, tableName,
		logfield.ShouldMerge, rs.ShouldMerge(tableName),
	)
	log.Infow("started loading")

	stagingTableName := warehouseutils.StagingTableName(
		provider,
		tableName,
		tableNameLimit,
	)

	log.Debugw("creating staging table")
	createStagingTableStmt := fmt.Sprintf(`CREATE TABLE %[1]q.%[2]q (LIKE %[1]q.%[3]q INCLUDING DEFAULTS);`,
		rs.Namespace,
		stagingTableName,
		tableName,
	)
	if _, err := rs.DB.ExecContext(ctx, createStagingTableStmt); err != nil {
		return nil, "", fmt.Errorf("creating staging table: %w", err)
	}

	if !skipTempTableDelete {
		defer func() {
			rs.dropStagingTables(ctx, []string{stagingTableName})
		}()
	}

	txn, err := rs.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	strKeys := warehouseutils.GetColumnsFromTableSchema(tableSchemaInUpload)
	sort.Strings(strKeys)

	log.Infow("loading data into staging table")
	err = rs.copyIntoLoadTable(
		ctx, txn, tableName, stagingTableName,
		strKeys,
	)
	if err != nil {
		return nil, "", fmt.Errorf("loading data into staging table: %w", err)
	}

	var (
		rowsDeletedResult, rowsInsertedResult sql.Result
		rowsDeleted, rowsInserted             int64
	)
	if rs.ShouldMerge(tableName) {
		log.Infow("deleting from load table")
		rowsDeletedResult, err = rs.deleteFromLoadTable(
			ctx, txn, tableName,
			stagingTableName, tableSchemaAfterUpload,
		)
		if err != nil {
			return nil, "", fmt.Errorf("delete from load table: %w", err)
		}
	}

	log.Infow("inserting into load table")
	rowsInsertedResult, err = rs.insertIntoLoadTable(
		ctx, txn, tableName,
		stagingTableName, strKeys,
	)
	if err != nil {
		return nil, "", fmt.Errorf("insert into: %w", err)
	}

	log.Debugw("committing transaction")
	if err = txn.Commit(); err != nil {
		return nil, "", fmt.Errorf("commit transaction: %w", err)
	}

	if rowsDeletedResult != nil {
		rowsDeleted, err = rowsDeletedResult.RowsAffected()
		if err != nil {
			return nil, "", fmt.Errorf("getting rows affected: %w", err)
		}
	}
	if rowsInsertedResult != nil {
		rowsInserted, err = rowsInsertedResult.RowsAffected()
		if err != nil {
			return nil, "", fmt.Errorf("getting rows affected: %w", err)
		}
	}

	log.Infow("completed loading")

	return &types.LoadTableStats{
		RowsInserted: rowsInserted - rowsDeleted,
		RowsUpdated:  rowsDeleted,
	}, stagingTableName, nil
}

func (rs *Redshift) copyIntoLoadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	stagingTableName string,
	strKeys []string,
) error {
	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&rs.Warehouse.Destination)
	if err != nil {
		return fmt.Errorf("getting temporary s3 credentials: %w", err)
	}

	var manifestSQL, s3Location, region string
	if rs.config.loadByFolderPath {
		objectLocation, err := rs.Uploader.GetSampleLoadFileLocation(ctx, tableName)
		if err != nil {
			return fmt.Errorf("getting sample load file location: %w", err)
		}

		s3Location, region = warehouseutils.GetS3Location(objectLocation)
		if region == "" {
			region = "us-east-1"
		}
		s3Location = warehouseutils.GetLocationFolder(s3Location)
	} else {
		manifestSQL = "MANIFEST"

		manifestLocation, err := rs.generateManifest(ctx, tableName)
		if err != nil {
			return fmt.Errorf("generating manifest: %w", err)
		}
		s3Location, region = warehouseutils.GetS3Location(manifestLocation)
		if region == "" {
			region = "us-east-1"
		}
	}

	sortedColumnNames := warehouseutils.JoinWithFormatting(strKeys, func(_ int, name string) string {
		return fmt.Sprintf(`%q`, name)
	}, ",")

	var copyStmt string
	if rs.Uploader.GetLoadFileType() == warehouseutils.LoadFileTypeParquet {
		copyStmt = fmt.Sprintf(
			`COPY %s
			FROM '%s'
			ACCESS_KEY_ID '%s'
			SECRET_ACCESS_KEY '%s'
			SESSION_TOKEN '%s'
			%s FORMAT PARQUET;`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, stagingTableName),
			s3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
			manifestSQL,
		)
	} else {
		copyStmt = fmt.Sprintf(
			`COPY %s(%s)
			FROM '%s'
			CSV GZIP
			ACCESS_KEY_ID '%s'
			SECRET_ACCESS_KEY '%s'
			SESSION_TOKEN '%s'
			REGION '%s'
			DATEFORMAT 'auto'
			TIMEFORMAT 'auto'
			%s TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS
			COMPUPDATE OFF
			STATUPDATE OFF;`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, stagingTableName),
			sortedColumnNames,
			s3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
			region,
			manifestSQL,
		)
	}

	if _, err := txn.ExecContext(ctx, copyStmt); err != nil {
		return fmt.Errorf("running copy command: %w", normalizeError(err))
	}
	return nil
}

func (rs *Redshift) deleteFromLoadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	stagingTableName string,
	tableSchemaAfterUpload model.TableSchema,
) (sql.Result, error) {
	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	deleteStmt := fmt.Sprintf(
		`DELETE FROM %[1]s.%[2]q
		USING %[1]s.%[3]q _source
		WHERE _source.%[4]s = %[1]s.%[2]q.%[4]s`,
		rs.Namespace,
		tableName,
		stagingTableName,
		primaryKey,
	)
	if rs.config.dedupWindow {
		if _, ok := tableSchemaAfterUpload["received_at"]; ok {
			deleteStmt += fmt.Sprintf(
				` AND %[1]s.%[2]q.received_at > GETDATE() - INTERVAL '%[3]d HOUR'`,
				rs.Namespace,
				tableName,
				rs.config.dedupWindowInHours/time.Hour,
			)
		}
	}
	if tableName == warehouseutils.DiscardsTable {
		deleteStmt += fmt.Sprintf(
			` AND _source.%[3]s = %[1]s.%[2]q.%[3]s AND _source.%[4]s = %[1]s.%[2]q.%[4]s`,
			rs.Namespace,
			tableName,
			"table_name",
			"column_name",
		)
	}

	result, err := txn.ExecContext(ctx, deleteStmt)
	if err != nil {
		return nil, fmt.Errorf("deleting from main table for dedup: %w", normalizeError(err))
	}
	return result, nil
}

func (rs *Redshift) insertIntoLoadTable(
	ctx context.Context,
	txn *sqlmiddleware.Tx,
	tableName string,
	stagingTableName string,
	sortedColumnKeys []string,
) (sql.Result, error) {
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	quotedColumnNames := warehouseutils.DoubleQuoteAndJoinByComma(
		sortedColumnKeys,
	)

	insertStmt := fmt.Sprintf(
		`INSERT INTO %[1]q.%[2]q (%[3]s)
		SELECT %[3]s
		FROM
		  (
			SELECT
			  *,
			  row_number() OVER (
				PARTITION BY %[5]s
				ORDER BY
				  received_at DESC
			  ) AS _rudder_staging_row_number
			FROM %[1]q.%[4]q
		  ) AS _
		WHERE _rudder_staging_row_number = 1;`,
		rs.Namespace,
		tableName,
		quotedColumnNames,
		stagingTableName,
		partitionKey,
	)

	result, err := txn.ExecContext(ctx, insertStmt)
	if err != nil {
		return nil, fmt.Errorf("inserting into main table: %w", err)
	}
	return result, nil
}

func (rs *Redshift) loadUserTables(ctx context.Context) map[string]error {
	var (
		err                  error
		query                string
		identifyStagingTable string
		txn                  *sqlmiddleware.Tx
		userColNames         []string
		firstValProps        []string
	)

	logFields := []any{
		logfield.SourceID, rs.Warehouse.Source.ID,
		logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, rs.Warehouse.Destination.ID,
		logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
		logfield.Namespace, rs.Namespace,
		logfield.ShouldMerge, !rs.config.skipComputingUserLatestTraits || rs.ShouldMerge(warehouseutils.UsersTable),
		logfield.TableName, warehouseutils.UsersTable,
	}
	rs.logger.Infow("started loading for identifies and users tables", logFields...)

	_, identifyStagingTable, err = rs.loadTable(ctx,
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

	defer rs.dropStagingTables(ctx, []string{identifyStagingTable})

	if len(rs.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
		}
	}

	if rs.config.skipComputingUserLatestTraits {
		_, _, err := rs.loadTable(ctx,
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

	query = fmt.Sprintf(
		`CREATE TABLE %[1]q.%[2]q AS (
		  SELECT DISTINCT *
		  FROM
			(
			  SELECT id, %[3]s
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
					  SELECT user_id, %[6]s
					  FROM %[1]q.%[5]q
					  WHERE user_id IS NOT NULL
					)
				)
			)
		);`,
		rs.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		warehouseutils.UsersTable,
		identifyStagingTable,
		quotedUserColNames,
	)

	if txn, err = rs.DB.BeginTx(ctx, &sql.TxOptions{}); err != nil {
		return map[string]error{
			warehouseutils.IdentifiesTable: fmt.Errorf("beginning transaction: %w", err),
		}
	}

	if _, err = txn.ExecContext(ctx, query); err != nil {
		_ = txn.Rollback()

		rs.logger.Warnw("creating staging table for users", append(logFields, logfield.Error, err.Error())...)
		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("creating staging table for users: %w", err),
		}
	}
	defer rs.dropStagingTables(ctx, []string{stagingTableName})

	primaryKey := "id"
	query = fmt.Sprintf(`DELETE FROM %[1]s.%[2]q USING %[1]s.%[3]q _source
			WHERE _source.%[4]s = %[1]s.%[2]s.%[4]s;`,
		rs.Namespace,
		warehouseutils.UsersTable,
		stagingTableName,
		primaryKey,
	)

	if _, err = txn.ExecContext(ctx, query); err != nil {
		_ = txn.Rollback()

		rs.logger.Warnw("deleting from users table for dedup", append(logFields,
			logfield.Query, query,
			logfield.Error, err.Error(),
		)...)
		return map[string]error{
			warehouseutils.UsersTable: fmt.Errorf("deleting from main table for dedup: %w", normalizeError(err)),
		}
	}

	query = fmt.Sprintf(
		`INSERT INTO %[1]q.%[2]q (%[4]s)
		SELECT %[4]s
		FROM %[1]q.%[3]q;`,
		rs.Namespace,
		warehouseutils.UsersTable,
		stagingTableName,
		warehouseutils.DoubleQuoteAndJoinByComma(append([]string{"id"}, userColNames...)),
	)

	rs.logger.Infow("inserting into users table", append(logFields, logfield.Query, query)...)

	if _, err = txn.ExecContext(ctx, query); err != nil {
		_ = txn.Rollback()

		rs.logger.Warnw("failed inserting into users table", append(logFields, logfield.Error, err.Error())...)

		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("inserting into users table from staging table: %w", normalizeError(err)),
		}
	}

	if err = txn.Commit(); err != nil {
		_ = txn.Rollback()

		rs.logger.Warnw("committing transaction for user table", append(logFields, logfield.Error, err.Error())...)

		return map[string]error{
			warehouseutils.IdentifiesTable: nil,
			warehouseutils.UsersTable:      fmt.Errorf("committing transaction: %w", err),
		}
	}

	rs.logger.Infow("completed loading for users and identifies tables", logFields...)

	return map[string]error{
		warehouseutils.IdentifiesTable: nil,
		warehouseutils.UsersTable:      nil,
	}
}

func (rs *Redshift) connect(ctx context.Context) (*sqlmiddleware.DB, error) {
	var (
		err error
		db  *sql.DB
	)

	if rs.useIAMForAuth() {
		db, err = rs.connectUsingIAMRole()
	} else {
		db, err = rs.connectUsingPassword()
	}
	if err != nil {
		return nil, fmt.Errorf("connecting to redshift: %w", err)
	}

	_, err = db.ExecContext(ctx, `SET query_group to 'RudderStack'`)
	if err != nil {
		return nil, fmt.Errorf("redshift set query_group error : %v", err)
	}
	middleware := sqlmiddleware.New(
		db,
		sqlmiddleware.WithStats(rs.stats),
		sqlmiddleware.WithLogger(rs.logger),
		sqlmiddleware.WithKeyAndValues(
			logfield.SourceID, rs.Warehouse.Source.ID,
			logfield.SourceType, rs.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, rs.Warehouse.Destination.ID,
			logfield.DestinationType, rs.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, rs.Warehouse.WorkspaceID,
			logfield.Schema, rs.Namespace,
		),
		sqlmiddleware.WithSlowQueryThreshold(rs.config.slowQueryThreshold),
		sqlmiddleware.WithQueryTimeout(rs.connectTimeout),
		sqlmiddleware.WithSecretsRegex(map[string]string{
			"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
			"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
			"SESSION_TOKEN '[^']*'":     "SESSION_TOKEN '***'",
		}),
	)
	return middleware, nil
}

func (rs *Redshift) useIAMForAuth() bool {
	return rs.Warehouse.GetBoolDestinationConfig(model.UseIAMForAuthSetting)
}

func (rs *Redshift) connectUsingIAMRole() (*sql.DB, error) {
	var (
		database          = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.DatabaseSetting)
		user              = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.UserSetting)
		iamRoleARNForAuth = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.IAMRoleARNForAuthSetting)
		clusterID         = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.ClusterIDSetting)
		useServerless     = rs.Warehouse.GetBoolDestinationConfig(model.UseServerlessSetting)
		workgroupName     = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.WorkgroupNameSetting)
		clusterRegion     = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.ClusterRegionSetting)
		timeout           = rs.connectTimeout
	)

	data := sqlconnectconfig.RedshiftData{
		Database:      database,
		Region:        clusterRegion,
		RoleARN:       iamRoleARNForAuth,
		ExternalID:    rs.Warehouse.WorkspaceID,
		RoleARNExpiry: time.Hour,
		Timeout:       timeout,
	}
	if useServerless {
		data.WorkgroupName = workgroupName
	} else {
		data.User = user
		data.ClusterIdentifier = clusterID
	}

	credentialsJSON, err := data.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("connectUsingIAMRole: marshalling redshift credentials: %w", err)
	}

	sqlConnectDB, err := sqlconnect.NewDB("redshift", credentialsJSON)
	if err != nil {
		return nil, fmt.Errorf("connectUsingIAMRole: creating redshift connection: %w", err)
	}
	return sqlConnectDB.SqlDB(), nil
}

func (rs *Redshift) connectUsingPassword() (*sql.DB, error) {
	var (
		host       = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.HostSetting)
		port       = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.PortSetting)
		database   = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.DatabaseSetting)
		user       = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.UserSetting)
		password   = rs.Warehouse.GetStringDestinationConfig(rs.conf, model.PasswordSetting)
		timeout    = rs.connectTimeout
		tunnelInfo = tunnelling.ExtractTunnelInfoFromDestinationConfig(rs.Warehouse.Destination.Config)
	)

	var (
		err error
		db  *sql.DB
	)

	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   database,
	}

	params := url.Values{}
	params.Add("sslmode", "require")

	if timeout > 0 && timeout < time.Second {
		return nil, fmt.Errorf("connectUsingPassword: invalid timeout value: %d", timeout)
	} else if timeout >= time.Second {
		params.Add("connect_timeout", fmt.Sprintf("%d", timeout/time.Second))
	}

	dsn.RawQuery = params.Encode()

	if tunnelInfo != nil {
		if db, err = tunnelling.Connect(dsn.String(), tunnelInfo.Config); err != nil {
			return nil, fmt.Errorf("connectUsingPassword: connecting to redshift through tunnel: %w", err)
		}
	} else {
		if db, err = sql.Open("postgres", dsn.String()); err != nil {
			return nil, fmt.Errorf("connectUsingPassword: connecting to redshift: %w", err)
		}
	}
	return db, nil
}

func (rs *Redshift) dropDanglingStagingTables(ctx context.Context) error {
	sqlStatement := `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_name like $2;`
	rows, err := rs.DB.QueryContext(ctx,
		sqlStatement,
		rs.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if err != nil {
		return fmt.Errorf("querying for dangling staging tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var stagingTableNames []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return fmt.Errorf("querying for dangling staging tables: %w", err)
		}
		stagingTableNames = append(stagingTableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating for dangling staging tables: %w", err)
	}
	rs.logger.Infon("Dropping dangling staging tables",
		logger.NewStringField("stagingTableNames", strings.Join(stagingTableNames, ",")),
	)
	for _, stagingTableName := range stagingTableNames {
		_, err := rs.DB.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%[1]s"."%[2]s"`, rs.Namespace, stagingTableName))
		if err != nil {
			return fmt.Errorf("dropping dangling staging table %q.%q: %w", rs.Namespace, stagingTableName, err)
		}
	}
	return nil
}

func (rs *Redshift) CreateSchema(ctx context.Context) (err error) {
	var schemaExists bool
	schemaExists, err = rs.schemaExists(ctx)
	if err != nil {
		rs.logger.Errorf("RS: Error checking if schema: %s exists: %v", rs.Namespace, err)
		return err
	}
	if schemaExists {
		rs.logger.Infof("RS: Skipping creating schema: %s since it already exists", rs.Namespace)
		return
	}
	return rs.createSchema(ctx)
}

func (rs *Redshift) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	var (
		query                string
		stagingColumnName    string
		stagingColumnType    string
		deprecatedColumnName string
		isDependent          bool
		tx                   *sqlmiddleware.Tx
		err                  error
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
	query = fmt.Sprintf(`ALTER TABLE %q.%q ADD COLUMN %q %s;`,
		rs.Namespace,
		tableName,
		stagingColumnName,
		stagingColumnType,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("add staging column: %w", err)
	}

	// populating staging column
	query = fmt.Sprintf(
		`UPDATE %[1]q.%[2]q
		SET %[3]q = CAST (%[4]q AS %[5]s)
		WHERE %[4]q IS NOT NULL;`,
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
	query = fmt.Sprintf(
		`ALTER TABLE %[1]q.%[2]q RENAME COLUMN %[3]q TO %[4]q;`,
		rs.Namespace,
		tableName,
		columnName,
		deprecatedColumnName,
	)
	if _, err = tx.ExecContext(ctx, query); err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("rename original column: %w", err)
	}

	// renaming staging column to original column
	query = fmt.Sprintf(
		`ALTER TABLE %[1]q.%[2]q RENAME COLUMN %[3]q TO %[4]q;`,
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
	// Because if it will fail during the commit of the transaction
	// https://github.com/lib/pq/blob/d5affd5073b06f745459768de35356df2e5fd91d/conn.go#L600
	query = fmt.Sprintf(
		`ALTER TABLE %[1]q.%[2]q DROP COLUMN %[3]q;`,
		rs.Namespace,
		tableName,
		deprecatedColumnName,
	)
	if _, err = rs.DB.ExecContext(ctx, query); err != nil {
		var pqError *pq.Error
		if !errors.As(err, &pqError) || pqError.Code != "2BP01" {
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

// FetchSchema queries redshift and returns the schema associated with provided namespace
func (rs *Redshift) FetchSchema(ctx context.Context) (model.Schema, error) {
	schema := make(model.Schema)

	sqlStatement := `SELECT
		  table_name,
		  column_name,
		  data_type,
		  character_maximum_length
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE table_schema = $1 and table_name not like $2;`

	rows, err := rs.DB.QueryContext(
		ctx,
		sqlStatement,
		rs.Namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	if errors.Is(err, sql.ErrNoRows) {
		return schema, nil
	}
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, columnName, columnType string
		var charLength sql.NullInt64

		if err := rows.Scan(&tableName, &columnName, &columnType, &charLength); err != nil {
			return nil, fmt.Errorf("scanning schema: %w", err)
		}

		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}
		if datatype, ok := calculateDataType(columnType, charLength); ok {
			schema[tableName][columnName] = datatype
		} else {
			warehouseutils.WHCounterStat(rs.stats, warehouseutils.RudderMissingDatatype, &rs.Warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	return schema, nil
}

func calculateDataType(columnType string, charLength sql.NullInt64) (string, bool) {
	if datatype, ok := dataTypesMapToRudder[columnType]; ok {
		if datatype == "string" && charLength.Int64 > rudderStringLength {
			datatype = "text"
		}
		return datatype, true
	}
	return "", false
}

func (rs *Redshift) Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	rs.Uploader = uploader

	rs.DB, err = rs.connect(ctx)
	return err
}

func (rs *Redshift) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := rs.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (rs *Redshift) Cleanup(ctx context.Context) {
	if rs.DB != nil {
		err := rs.dropDanglingStagingTables(ctx)
		if err != nil {
			rs.logger.Errorw("Error dropping dangling staging tables",
				logfield.Error, err.Error(),
			)
		}
		_ = rs.DB.Close()
	}
}

func (*Redshift) IsEmpty(context.Context, model.Warehouse) (empty bool, err error) {
	return
}

func (rs *Redshift) LoadUserTables(ctx context.Context) map[string]error {
	return rs.loadUserTables(ctx)
}

func (rs *Redshift) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStat, _, err := rs.loadTable(
		ctx,
		tableName,
		rs.Uploader.GetTableSchemaInUpload(tableName),
		rs.Uploader.GetTableSchemaInWarehouse(tableName),
		false,
	)
	return loadTableStat, err
}

func (*Redshift) LoadIdentityMergeRulesTable(context.Context) (err error) {
	return
}

func (*Redshift) LoadIdentityMappingsTable(context.Context) (err error) {
	return
}

func (*Redshift) DownloadIdentityRules(context.Context, *misc.GZipWriter) (err error) {
	return
}

func (rs *Redshift) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	rs.Warehouse = warehouse
	rs.Namespace = warehouse.Namespace
	db, err := rs.connect(ctx)
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, err
}

func (rs *Redshift) LoadTestTable(ctx context.Context, location, tableName string, _ map[string]interface{}, format string) (err error) {
	tempAccessKeyId, tempSecretAccessKey, token, err := warehouseutils.GetTemporaryS3Cred(&rs.Warehouse.Destination)
	if err != nil {
		rs.logger.Errorf("RS: Failed to create temp credentials before copying, while create load for table %v, err%v", tableName, err)
		return
	}

	var s3Location, region string
	if rs.config.loadByFolderPath {
		s3Location, region = warehouseutils.GetS3Location(location)
		if region == "" {
			region = "us-east-1"
		}
		s3Location = warehouseutils.GetLocationFolder(s3Location)
	} else {
		s3Location, region = warehouseutils.GetS3Location(location)
		if region == "" {
			region = "us-east-1"
		}
	}

	var sqlStatement string
	if format == warehouseutils.LoadFileTypeParquet {
		// copy statement for parquet load files
		sqlStatement = fmt.Sprintf(`COPY %v FROM '%s' ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' FORMAT PARQUET`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, tableName),
			s3Location,
			tempAccessKeyId,
			tempSecretAccessKey,
			token,
		)
	} else {
		// copy statement for csv load files
		sqlStatement = fmt.Sprintf(`COPY %v(%v) FROM '%v' CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' REGION '%s'  DATEFORMAT 'auto' TIMEFORMAT 'auto' TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL FILLRECORD ACCEPTANYDATE TRIMBLANKS ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF`,
			fmt.Sprintf(`%q.%q`, rs.Namespace, tableName),
			fmt.Sprintf(`%q, %q`, "id", "val"),
			s3Location,
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
		rs.logger.Infof("RS: Running COPY command for load test table: %s with sqlStatement: %s", tableName, sanitisedSQLStmt)
	}

	_, err = rs.DB.ExecContext(ctx, sqlStatement)

	return normalizeError(err)
}

func (rs *Redshift) SetConnectionTimeout(timeout time.Duration) {
	rs.connectTimeout = timeout
}

func (rs *Redshift) ShouldMerge(tableName string) bool {
	if !rs.config.allowMerge {
		return false
	}
	if tableName == warehouseutils.UsersTable {
		// If we are here it's because skipComputingUserLatestTraits is true.
		// preferAppend doesn't apply to the users table, so we are just checking skipDedupDestinationIDs for
		// backwards compatibility.
		return !slices.Contains(rs.config.skipDedupDestinationIDs, rs.Warehouse.Destination.ID)
	}

	configKey := "Warehouse.redshift.appendOnlyTables." + rs.Warehouse.Destination.ID
	appendOnlyTables := rs.conf.GetStringSlice(configKey, nil)
	if slices.Contains(appendOnlyTables, tableName) {
		return false
	}

	// It's important to check the ability to append after skipDedup to make sure that if both
	// skipDedupDestinationIDs and skipComputingUserLatestTraits are set, we still merge.
	// see hyperverge user table use case for more details.
	if !rs.Uploader.CanAppend() {
		return true
	}
	return !rs.Warehouse.GetPreferAppendSetting() &&
		!slices.Contains(rs.config.skipDedupDestinationIDs, rs.Warehouse.Destination.ID)
}

func (*Redshift) ErrorMappings() []model.JobError {
	return errorsMappings
}

func normalizeError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return fmt.Errorf("pq: message: %s, detail: %s",
			pqErr.Message,
			pqErr.Detail,
		)
	}
	return err
}

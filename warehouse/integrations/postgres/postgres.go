package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"
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
	verifyCA = "verify-ca"
)

const (
	provider       = warehouseutils.POSTGRES
	tableNameLimit = 127
)

var errorsMappings = []model.JobError{
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`dial tcp: lookup .*: no such host`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`dial tcp .* connect: connection refused`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: database .* does not exist`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: the database system is starting up`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: the database system is shutting down`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: relation .* does not exist`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`pq: cannot set transaction read-write mode during recovery`),
	},
	{
		Type:   model.ColumnCountError,
		Format: regexp.MustCompile(`pq: tables can have at most 1600 columns`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: password authentication failed for user`),
	},
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`pq: permission denied`),
	},
}

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

type Postgres struct {
	DB                          *sql.DB
	Namespace                   string
	ObjectStorage               string
	Warehouse                   model.Warehouse
	Uploader                    warehouseutils.Uploader
	ConnectTimeout              time.Duration
	Logger                      logger.Logger
	EnableDeleteByJobs          bool
	NumWorkersDownloadLoadFiles int
	LoadFileDownloader          downloader.Downloader
}

type Credentials struct {
	Host       string
	DBName     string
	User       string
	Password   string
	Port       string
	SSLMode    string
	SSLDir     string
	TunnelInfo *tunnelling.TunnelInfo
	timeout    time.Duration
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

func NewPostgres() *Postgres {
	return &Postgres{
		Logger: pkgLogger,
	}
}

func WithConfig(h *Postgres, config *config.Config) {
	h.EnableDeleteByJobs = config.GetBool("Warehouse.postgres.enableDeleteByJobs", false)
	h.NumWorkersDownloadLoadFiles = config.GetInt("Warehouse.postgres.numWorkersDownloadLoadFiles", 1)
}

func Connect(cred Credentials) (*sql.DB, error) {
	dsn := url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", cred.Host, cred.Port),
		User:   url.UserPassword(cred.User, cred.Password),
		Path:   cred.DBName,
	}

	values := url.Values{}
	values.Add("sslmode", cred.SSLMode)

	if cred.timeout > 0 {
		values.Add("connect_timeout", fmt.Sprintf("%d", cred.timeout/time.Second))
	}

	if cred.SSLMode == verifyCA {
		values.Add("sslrootcert", fmt.Sprintf("%s/server-ca.pem", cred.SSLDir))
		values.Add("sslcert", fmt.Sprintf("%s/client-cert.pem", cred.SSLDir))
		values.Add("sslkey", fmt.Sprintf("%s/client-key.pem", cred.SSLDir))
	}

	dsn.RawQuery = values.Encode()

	var (
		err error
		db  *sql.DB
	)

	if cred.TunnelInfo != nil {

		db, err = tunnelling.SQLConnectThroughTunnel(dsn.String(), cred.TunnelInfo.Config)
		if err != nil {
			return nil, fmt.Errorf("opening connection to postgres through tunnelling: %w", err)
		}
		return db, nil
	}

	if db, err = sql.Open("postgres", dsn.String()); err != nil {
		return nil, fmt.Errorf("opening connection to postgres: %w", err)
	}

	return db, nil
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("postgres")
}

func (pg *Postgres) getConnectionCredentials() Credentials {
	sslMode := warehouseutils.GetConfigValue(sslMode, pg.Warehouse)
	creds := Credentials{
		Host:     warehouseutils.GetConfigValue(host, pg.Warehouse),
		DBName:   warehouseutils.GetConfigValue(dbName, pg.Warehouse),
		User:     warehouseutils.GetConfigValue(user, pg.Warehouse),
		Password: warehouseutils.GetConfigValue(password, pg.Warehouse),
		Port:     warehouseutils.GetConfigValue(port, pg.Warehouse),
		SSLMode:  sslMode,
		SSLDir:   warehouseutils.GetSSLKeyDirPath(pg.Warehouse.Destination.ID),
		timeout:  pg.ConnectTimeout,
		TunnelInfo: warehouseutils.ExtractTunnelInfoFromDestinationConfig(
			pg.Warehouse.Destination.Config,
		),
	}

	return creds
}

func ColumnsWithDataTypes(columns model.TableSchema, prefix string) string {
	var arr []string
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`"%s%s" %s`, prefix, name, rudderDataTypesMapToPostgres[dataType]))
	}
	return strings.Join(arr, ",")
}

func (*Postgres) IsEmpty(_ model.Warehouse) (empty bool, err error) {
	return
}

// DeleteBy Need to create a structure with delete parameters instead of simply adding a long list of params
func (pg *Postgres) DeleteBy(tableNames []string, params warehouseutils.DeleteByParams) (err error) {
	pg.Logger.Infof("PG: Cleaning up the following tables in postgres for PG:%s : %+v", tableNames, params)
	for _, tb := range tableNames {
		sqlStatement := fmt.Sprintf(`DELETE FROM "%[1]s"."%[2]s" WHERE
		context_sources_job_run_id <> $1 AND
		context_sources_task_run_id <> $2 AND
		context_source_id = $3 AND
		received_at < $4`,
			pg.Namespace,
			tb,
		)
		pg.Logger.Infof("PG: Deleting rows in table in postgres for PG:%s", pg.Warehouse.Destination.ID)
		pg.Logger.Debugf("PG: Executing the statement  %v", sqlStatement)
		if pg.EnableDeleteByJobs {
			_, err = pg.DB.Exec(sqlStatement,
				params.JobRunId,
				params.TaskRunId,
				params.SourceId,
				params.StartTime)
			if err != nil {
				pg.Logger.Errorf("Error %s", err)
				return err
			}
		}

	}
	return nil
}

func (pg *Postgres) schemaExists(_ string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%s');`, pg.Namespace)
	err = pg.DB.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (pg *Postgres) CreateSchema() (err error) {
	var schemaExists bool
	schemaExists, err = pg.schemaExists(pg.Namespace)
	if err != nil {
		pg.Logger.Errorf("PG: Error checking if schema: %s exists: %v", pg.Namespace, err)
		return err
	}
	if schemaExists {
		pg.Logger.Infof("PG: Skipping creating schema: %s since it already exists", pg.Namespace)
		return
	}
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, pg.Namespace)
	pg.Logger.Infof("PG: Creating schema name in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.DB.Exec(sqlStatement)
	return
}

func (pg *Postgres) createTable(name string, columns model.TableSchema) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%[1]s"."%[2]s" ( %v )`, pg.Namespace, name, ColumnsWithDataTypes(columns, ""))
	pg.Logger.Infof("PG: Creating table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.DB.Exec(sqlStatement)
	return
}

func (pg *Postgres) CreateTable(tableName string, columnMap model.TableSchema) (err error) {
	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	sqlStatement := fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	_, err = pg.DB.Exec(sqlStatement)
	if err != nil {
		return err
	}
	pg.Logger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, sqlStatement)
	err = pg.createTable(tableName, columnMap)
	return err
}

func (pg *Postgres) DropTable(tableName string) (err error) {
	sqlStatement := `DROP TABLE "%[1]s"."%[2]s"`
	pg.Logger.Infof("PG: Dropping table in postgres for PG:%s : %v", pg.Warehouse.Destination.ID, sqlStatement)
	_, err = pg.DB.Exec(fmt.Sprintf(sqlStatement, pg.Namespace, tableName))
	return
}

func (pg *Postgres) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	var (
		query        string
		queryBuilder strings.Builder
	)

	// set the schema in search path. so that we can query table with unqualified name which is just the table name rather than using schema.table in queries
	query = fmt.Sprintf(`SET search_path to %q`, pg.Namespace)
	if _, err = pg.DB.Exec(query); err != nil {
		return
	}
	pg.Logger.Infof("PG: Updated search_path to %s in postgres for PG:%s : %v", pg.Namespace, pg.Warehouse.Destination.ID, query)

	queryBuilder.WriteString(fmt.Sprintf(`
		ALTER TABLE
		  %s.%s`,
		pg.Namespace,
		tableName,
	))

	for _, columnInfo := range columnsInfo {
		queryBuilder.WriteString(fmt.Sprintf(` ADD COLUMN IF NOT EXISTS %q %s,`, columnInfo.Name, rudderDataTypesMapToPostgres[columnInfo.Type]))
	}

	query = strings.TrimSuffix(queryBuilder.String(), ",")
	query += ";"

	pg.Logger.Infof("PG: Adding columns for destinationID: %s, tableName: %s with query: %v", pg.Warehouse.Destination.ID, tableName, query)
	_, err = pg.DB.Exec(query)
	return
}

func (*Postgres) AlterColumn(_, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

func (pg *Postgres) TestConnection(warehouse model.Warehouse) (err error) {
	if warehouse.Destination.Config["sslMode"] == "verify-ca" {
		if sslKeyError := warehouseutils.WriteSSLKeys(warehouse.Destination); sslKeyError.IsError() {
			pg.Logger.Error(sslKeyError.Error())
			err = fmt.Errorf(sslKeyError.Error())
			return
		}
	}
	pg.Warehouse = warehouse
	pg.DB, err = Connect(pg.getConnectionCredentials())
	if err != nil {
		return
	}
	defer pg.DB.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), pg.ConnectTimeout)
	defer cancel()

	err = pg.DB.PingContext(ctx)
	if err == context.DeadlineExceeded {
		return fmt.Errorf("connection testing timed out after %d sec", pg.ConnectTimeout/time.Second)
	}
	if err != nil {
		return err
	}

	return nil
}

func (pg *Postgres) Setup(
	warehouse model.Warehouse,
	uploader warehouseutils.Uploader,
) (err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	pg.Uploader = uploader
	pg.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.POSTGRES, warehouse.Destination.Config, pg.Uploader.UseRudderStorage())
	pg.LoadFileDownloader = downloader.NewDownloader(&warehouse, uploader, pg.NumWorkersDownloadLoadFiles)

	pg.DB, err = Connect(pg.getConnectionCredentials())
	return err
}

func (pg *Postgres) CrashRecover(model.Warehouse) error {
	return nil
}

// FetchSchema queries postgres and returns the schema associated with provided namespace
func (pg *Postgres) FetchSchema(warehouse model.Warehouse) (schema, unrecognizedSchema model.Schema, err error) {
	pg.Warehouse = warehouse
	pg.Namespace = warehouse.Namespace
	dbHandle, err := Connect(pg.getConnectionCredentials())
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(model.Schema)
	unrecognizedSchema = make(model.Schema)

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
		pg.Logger.Errorf("PG: Error in fetching schema from postgres destination:%v, query: %v", pg.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		pg.Logger.Infof("PG: No rows, while fetching schema from  destination:%v, query: %v", pg.Warehouse.Identifier, sqlStatement)
		return schema, unrecognizedSchema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType sql.NullString
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			pg.Logger.Errorf("PG: Error in processing fetched schema from clickhouse destination:%v", pg.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName.String]; !ok {
			schema[tName.String] = make(model.TableSchema)
		}
		if cName.Valid && cType.Valid {
			if datatype, ok := postgresDataTypesMapToRudder[cType.String]; ok {
				schema[tName.String][cName.String] = datatype
			} else {
				if _, ok := unrecognizedSchema[tName.String]; !ok {
					unrecognizedSchema[tName.String] = make(model.TableSchema)
				}
				unrecognizedSchema[tName.String][cType.String] = warehouseutils.MISSING_DATATYPE

				warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &pg.Warehouse, warehouseutils.Tag{Name: "datatype", Value: cType.String}).Count(1)
			}
		}
	}
	return
}

func (pg *Postgres) LoadUserTables() map[string]error {
	var (
		identifiesSchemaInUpload = pg.Uploader.GetTableSchemaInUpload(warehouseutils.IdentifiesTable)
		usersSchemaInUpload      = pg.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)
		usersSchemaInwarehouse   = pg.Uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)

		lut = LoadUsersTable{
			Logger:             pg.Logger,
			DB:                 pg.DB,
			Namespace:          pg.Namespace,
			Warehouse:          &pg.Warehouse,
			Stats:              stats.Default,
			Config:             config.Default,
			LoadFileDownloader: pg.LoadFileDownloader,
		}
	)

	errorMap := lut.Load(context.TODO(), identifiesSchemaInUpload, usersSchemaInUpload, usersSchemaInwarehouse)
	for tableName, err := range errorMap {
		if err != nil {
			lut.Logger.Warnw("loading users table",
				logfield.SourceID, lut.Warehouse.Source.ID,
				logfield.SourceType, lut.Warehouse.Source.SourceDefinition.Name,
				logfield.DestinationID, lut.Warehouse.Destination.ID,
				logfield.DestinationType, lut.Warehouse.Destination.DestinationDefinition.Name,
				logfield.WorkspaceID, lut.Warehouse.WorkspaceID,
				logfield.TableName, tableName,
				logfield.Namespace, pg.Namespace,
				logfield.Error, err.Error(),
			)
		}
	}
	return errorMap
}

func (pg *Postgres) LoadTable(tableName string) error {
	lt := LoadTable{
		Logger:             pg.Logger,
		DB:                 pg.DB,
		Namespace:          pg.Namespace,
		Warehouse:          &pg.Warehouse,
		Stats:              stats.Default,
		Config:             config.Default,
		LoadFileDownloader: pg.LoadFileDownloader,
	}
	tableSchemaInUpload := pg.Uploader.GetTableSchemaInUpload(tableName)
	_, err := lt.Load(context.TODO(), tableName, tableSchemaInUpload)
	if err != nil {
		lt.Logger.Warnw("loading table",
			logfield.SourceID, lt.Warehouse.Source.ID,
			logfield.SourceType, lt.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, lt.Warehouse.Destination.ID,
			logfield.DestinationType, lt.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, lt.Warehouse.WorkspaceID,
			logfield.TableName, tableName,
			logfield.Namespace, pg.Namespace,
			logfield.Error, err.Error(),
		)

		return fmt.Errorf("loading table: %w", err)
	}

	return nil
}

func (pg *Postgres) Cleanup() {
	if pg.DB != nil {
		pg.DB.Close()
	}
}

func (*Postgres) LoadIdentityMergeRulesTable() (err error) {
	return
}

func (*Postgres) LoadIdentityMappingsTable() (err error) {
	return
}

func (*Postgres) DownloadIdentityRules(*misc.GZipWriter) (err error) {
	return
}

func (pg *Postgres) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM "%[1]s"."%[2]s";
	`,
		pg.Namespace,
		tableName,
	)
	err = pg.DB.QueryRowContext(ctx, sqlStatement).Scan(&total)
	return total, err
}

func (pg *Postgres) Connect(warehouse model.Warehouse) (client.Client, error) {
	if warehouse.Destination.Config["sslMode"] == "verify-ca" {
		if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
			pg.Logger.Error(err.Error())
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

func (pg *Postgres) LoadTestTable(_, tableName string, payloadMap map[string]interface{}, _ string) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q.%q (%v) VALUES (%s)`,
		pg.Namespace,
		tableName,
		fmt.Sprintf(`%q, %q`, "id", "val"),
		fmt.Sprintf(`'%d', '%s'`, payloadMap["id"], payloadMap["val"]),
	)
	_, err = pg.DB.Exec(sqlStatement)
	return
}

func (pg *Postgres) SetConnectionTimeout(timeout time.Duration) {
	pg.ConnectTimeout = timeout
}

func (pg *Postgres) ErrorMappings() []model.JobError {
	return errorsMappings
}

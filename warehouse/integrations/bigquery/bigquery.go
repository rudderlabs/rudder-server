package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/samber/lo"
	bqService "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/middleware"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type BigQuery struct {
	db         *bigquery.Client
	middleware *middleware.Client
	namespace  string
	warehouse  model.Warehouse
	projectID  string
	uploader   warehouseutils.Uploader
	conf       *config.Config
	logger     logger.Logger

	config struct {
		setUsersLoadPartitionFirstEventFilter bool
		customPartitionsEnabled               bool
		enableDeleteByJobs                    bool
		customPartitionsEnabledWorkspaceIDs   []string
		slowQueryThreshold                    time.Duration
	}
}

type loadTableResponse struct {
	partitionDate string
}

const (
	provider       = warehouseutils.BQ
	tableNameLimit = 127
)

// maps datatype stored in rudder to datatype in bigquery
var dataTypesMap = map[string]bigquery.FieldType{
	"boolean":  bigquery.BooleanFieldType,
	"int":      bigquery.IntegerFieldType,
	"float":    bigquery.FloatFieldType,
	"string":   bigquery.StringFieldType,
	"datetime": bigquery.TimestampFieldType,
}

// maps datatype in bigquery to datatype stored in rudder
var dataTypesMapToRudder = map[bigquery.FieldType]string{
	"BOOLEAN":   "boolean",
	"BOOL":      "boolean",
	"INTEGER":   "int",
	"INT64":     "int",
	"NUMERIC":   "float",
	"FLOAT":     "float",
	"FLOAT64":   "float",
	"STRING":    "string",
	"BYTES":     "string",
	"DATE":      "datetime",
	"DATETIME":  "datetime",
	"TIME":      "datetime",
	"TIMESTAMP": "datetime",
}

var partitionKeyMap = map[string]string{
	"users":                                "id",
	"identifies":                           "id",
	warehouseutils.DiscardsTable:           "row_id, column_name, table_name",
	warehouseutils.IdentityMappingsTable:   "merge_property_type, merge_property_value",
	warehouseutils.IdentityMergeRulesTable: "merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value",
}

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`googleapi: Error 403: Access Denied`),
	},
	{
		Type:   model.ResourceNotFoundError,
		Format: regexp.MustCompile(`googleapi: Error 404: Not found: Dataset .*, notFound`),
	},
	{
		Type:   model.ConcurrentQueriesError,
		Format: regexp.MustCompile(`googleapi: Error 400: Job exceeded rate limits: Your project_and_region exceeded quota for concurrent queries.`),
	},
	{
		Type:   model.ConcurrentQueriesError,
		Format: regexp.MustCompile(`googleapi: Error 400: Exceeded rate limits: too many concurrent queries for this project_and_region.`),
	},
	{
		Type:   model.ColumnCountError,
		Format: regexp.MustCompile(`googleapi: Error 400: Too many total leaf fields: .*, max allowed field count: 10000`),
	},
}

func New(conf *config.Config, log logger.Logger) *BigQuery {
	bq := &BigQuery{}

	bq.conf = conf
	bq.logger = log.Child("integrations").Child("bigquery")

	bq.config.setUsersLoadPartitionFirstEventFilter = conf.GetBool("Warehouse.bigquery.setUsersLoadPartitionFirstEventFilter", true)
	bq.config.customPartitionsEnabled = conf.GetBool("Warehouse.bigquery.customPartitionsEnabled", false)
	bq.config.enableDeleteByJobs = conf.GetBool("Warehouse.bigquery.enableDeleteByJobs", false)
	bq.config.customPartitionsEnabledWorkspaceIDs = conf.GetStringSlice("Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs", nil)
	bq.config.slowQueryThreshold = conf.GetDuration("Warehouse.bigquery.slowQueryThreshold", 5, time.Minute)

	return bq
}

func (bq *BigQuery) getMiddleware() *middleware.Client {
	if bq.middleware != nil {
		return bq.middleware
	}
	return middleware.New(
		bq.db,
		middleware.WithLogger(bq.logger),
		middleware.WithKeyAndValues(
			logfield.SourceID, bq.warehouse.Source.ID,
			logfield.SourceType, bq.warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, bq.warehouse.Destination.ID,
			logfield.DestinationType, bq.warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, bq.warehouse.WorkspaceID,
			logfield.Schema, bq.namespace,
		),
		middleware.WithSlowQueryThreshold(bq.config.slowQueryThreshold),
	)
}

func getTableSchema(tableSchema model.TableSchema) []*bigquery.FieldSchema {
	return lo.MapToSlice(tableSchema, func(columnName, columnType string) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]}
	})
}

func (bq *BigQuery) DeleteTable(ctx context.Context, tableName string) (err error) {
	tableRef := bq.db.Dataset(bq.namespace).Table(tableName)
	err = tableRef.Delete(ctx)
	return
}

func (bq *BigQuery) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) error {
	bq.logger.Infof("BQ: Creating table: %s in bigquery dataset: %s in project: %s", tableName, bq.namespace, bq.projectID)
	sampleSchema := getTableSchema(columnMap)
	metaData := &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{},
	}
	tableRef := bq.db.Dataset(bq.namespace).Table(tableName)
	err := tableRef.Create(ctx, metaData)
	if !checkAndIgnoreAlreadyExistError(err) {
		return fmt.Errorf("create table: %w", err)
	}

	if err = bq.createTableView(ctx, tableName, columnMap); err != nil {
		return fmt.Errorf("create view: %w", err)
	}

	return nil
}

func (bq *BigQuery) DropTable(ctx context.Context, tableName string) error {
	if err := bq.DeleteTable(ctx, tableName); err != nil {
		return err
	}
	return bq.DeleteTable(ctx, tableName+"_view")
}

func (bq *BigQuery) createTableView(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	var viewOrderByStmt string
	if _, ok := columnMap["loaded_at"]; ok {
		viewOrderByStmt = " ORDER BY loaded_at DESC "
	}

	// assuming it has field named id upon which dedup is done in view
	// the following view takes the last two months into consideration i.e. 60 * 60 * 24 * 60 * 1000000
	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + partitionKey + viewOrderByStmt + `) AS __row_number
			FROM ` + "`" + bq.projectID + "." + bq.namespace + "." + tableName + "`" + `
			WHERE
				_PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(
					TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 24 * 60 * 1000000),
					DAY,
					'UTC'
				)
				AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
		)
		WHERE __row_number = 1`
	metaData := &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}
	tableRef := bq.db.Dataset(bq.namespace).Table(tableName + "_view")
	err = tableRef.Create(ctx, metaData)
	return
}

func (bq *BigQuery) schemaExists(ctx context.Context, _, _ string) (exists bool, err error) {
	ds := bq.db.Dataset(bq.namespace)
	_, err = ds.Metadata(ctx)
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 404 {
			bq.logger.Debugf("BQ: Dataset %s not found", bq.namespace)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (bq *BigQuery) CreateSchema(ctx context.Context) (err error) {
	bq.logger.Infof("BQ: Creating bigquery dataset: %s in project: %s", bq.namespace, bq.projectID)
	location := strings.TrimSpace(bq.warehouse.GetStringDestinationConfig(bq.conf, model.LocationSetting))
	if location == "" {
		location = "US"
	}

	var schemaExists bool
	schemaExists, err = bq.schemaExists(ctx, bq.namespace, location)
	if err != nil {
		bq.logger.Errorf("BQ: Error checking if schema: %s exists: %v", bq.namespace, err)
		return err
	}
	if schemaExists {
		bq.logger.Infof("BQ: Skipping creating schema: %s since it already exists", bq.namespace)
		return
	}

	ds := bq.db.Dataset(bq.namespace)
	meta := &bigquery.DatasetMetadata{
		Location: location,
	}
	bq.logger.Infof("BQ: Creating schema: %s ...", bq.namespace)
	err = ds.Create(ctx, meta)
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 409 {
			bq.logger.Infof("BQ: Create schema %s failed as schema already exists", bq.namespace)
			return nil
		}
	}
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) {
			// 409 is returned when we try to create a table that already exists
			// 400 is returned for all kinds of invalid input - so we need to check the error message too
			if e.Code == 409 || (e.Code == 400 && strings.Contains(e.Message, "already exists in schema")) {
				return true
			}
		}
		return false
	}
	return true
}

func (bq *BigQuery) DeleteBy(ctx context.Context, tableNames []string, params warehouseutils.DeleteByParams) error {
	for _, tb := range tableNames {
		bq.logger.Infof("BQ: Cleaning up the following tables in bigquery for BQ:%s", tb)
		tableName := fmt.Sprintf("`%s`.`%s`", bq.namespace, tb)
		sqlStatement := fmt.Sprintf(`
			DELETE FROM
				%[1]s
			WHERE
				context_sources_job_run_id <>
			@jobrunid AND
				context_sources_task_run_id <> @taskrunid AND
				context_source_id = @sourceid AND
				received_at < @starttime;
			`,
			tableName,
		)

		bq.logger.Infof("PG: Deleting rows in table in bigquery for BQ:%s", bq.warehouse.Destination.ID)
		bq.logger.Debugf("PG: Executing the sql statement %v", sqlStatement)
		query := bq.db.Query(sqlStatement)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "jobrunid", Value: params.JobRunId},
			{Name: "taskrunid", Value: params.TaskRunId},
			{Name: "sourceid", Value: params.SourceId},
			{Name: "starttime", Value: params.StartTime},
		}
		if bq.config.enableDeleteByJobs {
			job, err := bq.getMiddleware().Run(ctx, query)
			if err != nil {
				bq.logger.Errorf("BQ: Error initiating load job: %v\n", err)
				return err
			}
			status, err := job.Wait(ctx)
			if err != nil {
				bq.logger.Errorf("BQ: Error running job: %v\n", err)
				return err
			}
			if status.Err() != nil {
				return status.Err()
			}
		}
	}
	return nil
}

func partitionedTable(tableName, partitionDate string) string {
	return fmt.Sprintf(`%s$%v`, tableName, strings.ReplaceAll(partitionDate, "-", ""))
}

func (bq *BigQuery) loadTable(ctx context.Context, tableName string) (
	*types.LoadTableStats, *loadTableResponse, error,
) {
	log := bq.logger.With(
		logfield.SourceID, bq.warehouse.Source.ID,
		logfield.SourceType, bq.warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, bq.warehouse.Destination.ID,
		logfield.DestinationType, bq.warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, bq.warehouse.WorkspaceID,
		logfield.Namespace, bq.namespace,
		logfield.TableName, tableName,
		logfield.ShouldMerge, false, // we don't support merging in BigQuery due to its cost limitations
	)
	log.Infow("started loading")

	loadFileLocations, err := bq.loadFileLocations(ctx, tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("getting load file locations: %w", err)
	}

	gcsRef := bigquery.NewGCSReference(warehouseutils.GetGCSLocations(
		loadFileLocations,
		warehouseutils.GCSLocationOptions{},
	)...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	return bq.loadTableByAppend(ctx, tableName, gcsRef, log)
}

func (bq *BigQuery) loadFileLocations(
	ctx context.Context,
	tableName string,
) ([]warehouseutils.LoadFile, error) {
	switch tableName {
	case warehouseutils.IdentityMappingsTable, warehouseutils.IdentityMergeRulesTable:
		loadfile, err := bq.uploader.GetSingleLoadFile(
			ctx,
			tableName,
		)
		if err != nil {
			return nil, fmt.Errorf("getting single load file for table %s: %w", tableName, err)
		}
		return []warehouseutils.LoadFile{loadfile}, nil
	default:
		return bq.uploader.GetLoadFilesMetadata(
			ctx,
			warehouseutils.GetLoadFilesOptions{Table: tableName},
		)
	}
}

// loadTableByAppend loads data into a table by appending to it
//
// In BigQuery, tables created by RudderStack are typically ingestion-time partitioned tables
// with a pseudo-column named _PARTITIONTIME. BigQuery automatically assigns rows to partitions
// based on the time when BigQuery ingests the data. To support custom field partitions, it is
// important to avoid loading data into partitioned tables with names like tableName$20191221.
// Instead, ensure that data is loaded into the appropriate ingestion-time partition, allowing
// BigQuery to manage partitioning based on the data's ingestion time.
//
// TODO: Support custom field partition on users & identifies tables
func (bq *BigQuery) loadTableByAppend(
	ctx context.Context,
	tableName string,
	gcsRef *bigquery.GCSReference,
	log logger.Logger,
) (*types.LoadTableStats, *loadTableResponse, error) {
	partitionDate := time.Now().Format("2006-01-02")

	outputTable := partitionedTable(
		tableName,
		partitionDate,
	)
	if bq.config.customPartitionsEnabled || slices.Contains(bq.config.customPartitionsEnabledWorkspaceIDs, bq.warehouse.WorkspaceID) {
		outputTable = tableName
	}

	log.Infow("loading data into main table")
	job, err := bq.db.Dataset(bq.namespace).Table(outputTable).LoaderFrom(gcsRef).Run(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("moving data into main table: %w", err)
	}

	log.Debugw("waiting for append job to complete", "jobID", job.ID())
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("waiting for append job: %w", err)
	}
	if err := status.Err(); err != nil {
		return nil, nil, fmt.Errorf("status for append job: %w", err)
	}

	log.Debugw("job statistics")
	statistics, err := bq.jobStatistics(ctx, job)
	if err != nil {
		return nil, nil, fmt.Errorf("append job statistics: %w", err)
	}

	log.Infow("completed loading")

	tableStats := &types.LoadTableStats{}
	if statistics.Load != nil {
		tableStats.RowsInserted = statistics.Load.OutputRows
	}
	response := &loadTableResponse{
		partitionDate: partitionDate,
	}
	return tableStats, response, nil
}

// jobStatistics returns statistics for a job
// In case of rate limit error, it returns empty statistics
func (bq *BigQuery) jobStatistics(
	ctx context.Context,
	job *bigquery.Job,
) (*bqService.JobStatistics, error) {
	serv, err := bqService.NewService(
		ctx,
		option.WithCredentialsJSON([]byte(bq.warehouse.GetStringDestinationConfig(bq.conf, model.CredentialsSetting))),
	)
	if err != nil {
		return nil, fmt.Errorf("creating service: %w", err)
	}

	bqJobGetCall := bqService.NewJobsService(serv).Get(
		job.ProjectID(),
		job.ID(),
	)
	bqJob, err := bqJobGetCall.Context(ctx).Location(job.Location()).Fields("statistics").Do()
	if err != nil {
		// In case of rate limit error, return empty statistics
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 429 {
			return &bqService.JobStatistics{}, nil
		}
		return nil, fmt.Errorf("getting job: %w", err)
	}
	return bqJob.Statistics, nil
}

func (bq *BigQuery) LoadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	bq.logger.Infof("BQ: Starting load for identifies and users tables\n")
	_, identifyLoadTable, err := bq.loadTable(ctx, warehouseutils.IdentifiesTable)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(bq.uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	bq.logger.Infof("BQ: Starting load for %s table", warehouseutils.UsersTable)

	firstValueSQL := func(column string) string {
		return fmt.Sprintf("FIRST_VALUE(`%[1]s` IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `%[1]s`", column)
	}

	loadedAtFilter := func() string {
		// get first event received_at time in this upload for identifies table
		firstEventAt := func() time.Time {
			return bq.uploader.GetLoadFileGenStartTIme()
		}

		firstEventTime := firstEventAt()
		if !bq.config.setUsersLoadPartitionFirstEventFilter || firstEventTime.IsZero() {
			return ""
		}

		// TODO: Add this filter to optimize reading from identifies table since first event in upload
		// rather than entire day's records
		// commented it since firstEventAt is not stored in UTC format in earlier versions
		firstEventAtFormatted := firstEventTime.Format(misc.RFC3339Milli)
		return fmt.Sprintf(`AND loaded_at >= TIMESTAMP('%v')`, firstEventAtFormatted)
	}

	userColMap := bq.uploader.GetTableSchemaInWarehouse("users")
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf("`%s`", colName))
		firstValProps = append(firstValProps, firstValueSQL(colName))
	}

	bqTable := func(name string) string { return fmt.Sprintf("`%s`.`%s`", bq.namespace, name) }

	bqUsersView := bqTable(warehouseutils.UsersView)
	viewExists, _ := bq.tableExists(ctx, warehouseutils.UsersView)
	if !viewExists {
		bq.logger.Infof("BQ: Creating view: %s in bigquery dataset: %s in project: %s", warehouseutils.UsersView, bq.namespace, bq.projectID)
		_ = bq.createTableView(ctx, warehouseutils.UsersTable, userColMap)
	}

	bqIdentifiesTable := bqTable(warehouseutils.IdentifiesTable)
	partition := fmt.Sprintf("TIMESTAMP('%s')", identifyLoadTable.partitionDate)
	identifiesFrom := fmt.Sprintf(`%s WHERE _PARTITIONTIME = %s AND user_id IS NOT NULL %s`, bqIdentifiesTable, partition, loadedAtFilter())
	sqlStatement := fmt.Sprintf(`SELECT DISTINCT * FROM (
			SELECT id, %[1]s FROM (
				(
					SELECT id, %[2]s FROM %[3]s WHERE (
						id in (SELECT user_id FROM %[4]s)
					)
				) UNION ALL (
					SELECT user_id, %[2]s FROM %[4]s
				)
			)
		)`,
		strings.Join(firstValProps, ","), // 1
		strings.Join(userColNames, ","),  // 2
		bqUsersView,                      // 3
		identifiesFrom,                   // 4
	)
	loadUserTableByAppend := func() {
		bq.logger.Infof(`BQ: Loading data into users table: %v`, sqlStatement)
		partitionedUsersTable := partitionedTable(warehouseutils.UsersTable, identifyLoadTable.partitionDate)
		query := bq.db.Query(sqlStatement)
		query.QueryConfig.Dst = bq.db.Dataset(bq.namespace).Table(partitionedUsersTable)
		query.WriteDisposition = bigquery.WriteAppend

		job, err := bq.getMiddleware().Run(ctx, query)
		if err != nil {
			bq.logger.Errorf("BQ: Error initiating load job: %v\n", err)
			errorMap[warehouseutils.UsersTable] = err
			return
		}
		status, err := job.Wait(ctx)
		if err != nil {
			bq.logger.Errorf("BQ: Error running load job: %v\n", err)
			errorMap[warehouseutils.UsersTable] = fmt.Errorf(`append: %v`, err.Error())
			return
		}

		if status.Err() != nil {
			errorMap[warehouseutils.UsersTable] = status.Err()
			return
		}
	}

	loadUserTableByAppend()

	return errorMap
}

type BQCredentials struct {
	ProjectID   string
	Credentials string
}

func Connect(context context.Context, cred *BQCredentials) (*bigquery.Client, error) {
	var opts []option.ClientOption
	if !googleutil.ShouldSkipCredentialsInit(cred.Credentials) {
		credBytes := []byte(cred.Credentials)
		if err := googleutil.CompatibleGoogleCredentialsJSON(credBytes); err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentialsJSON(credBytes))
	}
	c, err := bigquery.NewClient(context, cred.ProjectID, opts...)
	return c, err
}

func (bq *BigQuery) connect(ctx context.Context, cred BQCredentials) (*bigquery.Client, error) {
	bq.logger.Infof("BQ: Connecting to BigQuery in project: %s", cred.ProjectID)
	c, err := Connect(ctx, &cred)
	return c, err
}

func (bq *BigQuery) CrashRecover(ctx context.Context) error {
	return bq.dropDanglingStagingTables(ctx)
}

func (bq *BigQuery) dropDanglingStagingTables(ctx context.Context) error {
	sqlStatement := fmt.Sprintf(`
		SELECT
		  table_name
		FROM
		  %[1]s.INFORMATION_SCHEMA.TABLES
		WHERE
		  table_schema = '%[1]s'
		  AND table_name LIKE '%[2]s';
	`,
		bq.namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	query := bq.db.Query(sqlStatement)
	it, err := bq.getMiddleware().Read(ctx, query)
	if err != nil {
		return fmt.Errorf("reading dangling staging tables in dataset %v: %w", bq.namespace, err)
	}

	var stagingTableNames []string
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return fmt.Errorf("processing dangling staging tables in dataset %v: %w", bq.namespace, err)
		}
		if _, ok := values[0].(string); ok {
			stagingTableNames = append(stagingTableNames, values[0].(string))
		}
	}
	bq.logger.Infof("WH: PG: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	for _, stagingTableName := range stagingTableNames {
		err := bq.DeleteTable(ctx, stagingTableName)
		if err != nil {
			return fmt.Errorf("dropping dangling staging table: %w", err)
		}
	}

	return nil
}

func (bq *BigQuery) IsEmpty(
	ctx context.Context,
	warehouse model.Warehouse,
) (bool, error) {
	bq.warehouse = warehouse
	bq.namespace = warehouse.Namespace
	bq.projectID = strings.TrimSpace(bq.warehouse.GetStringDestinationConfig(bq.conf, model.ProjectSetting))

	var err error
	bq.db, err = bq.connect(ctx, BQCredentials{
		ProjectID:   bq.projectID,
		Credentials: bq.warehouse.GetStringDestinationConfig(bq.conf, model.CredentialsSetting),
	})
	if err != nil {
		return false, fmt.Errorf("connecting to bigquery: %v", err)
	}
	defer func() { _ = bq.db.Close() }()

	tables := []string{"tracks", "pages", "screens", "identifies", "aliases"}
	for _, tableName := range tables {
		exists, err := bq.tableExists(ctx, tableName)
		if err != nil {
			return false, fmt.Errorf("checking if table %s exists: %v", tableName, err)
		}
		if !exists {
			continue
		}

		metadata, err := bq.db.Dataset(bq.namespace).Table(tableName).Metadata(ctx)
		if err != nil {
			return false, fmt.Errorf("getting metadata for table %s: %v", tableName, err)
		}
		return metadata.NumRows == 0, nil
	}
	return true, nil
}

func (bq *BigQuery) Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	bq.warehouse = warehouse
	bq.namespace = warehouse.Namespace
	bq.uploader = uploader
	bq.projectID = strings.TrimSpace(bq.warehouse.GetStringDestinationConfig(bq.conf, model.ProjectSetting))

	bq.db, err = bq.connect(ctx, BQCredentials{
		ProjectID:   bq.projectID,
		Credentials: bq.warehouse.GetStringDestinationConfig(bq.conf, model.CredentialsSetting),
	})
	return err
}

func (*BigQuery) TestConnection(context.Context, model.Warehouse) (err error) {
	return nil
}

func (bq *BigQuery) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStat, _, err := bq.loadTable(ctx, tableName)
	return loadTableStat, err
}

func (bq *BigQuery) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	bq.logger.Infof("BQ: Adding columns for destinationID: %s, tableName: %s, dataset: %s, project: %s", bq.warehouse.Destination.ID, tableName, bq.namespace, bq.projectID)
	tableRef := bq.db.Dataset(bq.namespace).Table(tableName)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return
	}

	newSchema := meta.Schema
	for _, columnInfo := range columnsInfo {
		newSchema = append(newSchema,
			&bigquery.FieldSchema{Name: columnInfo.Name, Type: dataTypesMap[columnInfo.Type]},
		)
	}

	tableMetadataToUpdate := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	_, err = tableRef.Update(ctx, tableMetadataToUpdate, meta.ETag)

	// Handle error in case of single column
	if len(columnsInfo) == 1 {
		if err != nil {
			if checkAndIgnoreAlreadyExistError(err) {
				bq.logger.Infof("BQ: Column %s already exists on %s.%s \nResponse: %v", columnsInfo[0].Name, bq.namespace, tableName, err)
				err = nil
			}
		}
	}
	return
}

func (*BigQuery) AlterColumn(context.Context, string, string, string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// FetchSchema queries bigquery and returns the schema associated with provided namespace
func (bq *BigQuery) FetchSchema(ctx context.Context) (model.Schema, model.Schema, error) {
	schema := make(model.Schema)
	unrecognizedSchema := make(model.Schema)

	sqlStatement := fmt.Sprintf(`
		SELECT
		  t.table_name,
		  c.column_name,
		  c.data_type
		FROM
		  %[1]s.INFORMATION_SCHEMA.TABLES as t
		  LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name)
		WHERE
		  (t.table_type != 'VIEW')
		  and (
			c.column_name != '_PARTITIONTIME'
			OR c.column_name IS NULL
		  );
	`,
		bq.namespace,
	)
	query := bq.db.Query(sqlStatement)

	it, err := bq.getMiddleware().Read(ctx, query)
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 404 {
			// if dataset resource is not found, return empty schema
			return schema, unrecognizedSchema, nil
		}
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}

	for {
		var values []bigquery.Value

		err := it.Next(&values)
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, nil, fmt.Errorf("iterating schema: %w", err)
		}

		var tableName, columnName, columnType string

		tableName, _ = values[0].(string)
		if _, ok := schema[tableName]; !ok {
			schema[tableName] = make(model.TableSchema)
		}

		columnName, _ = values[1].(string)
		columnType, _ = values[2].(string)

		// lower case all column names from bigquery
		columnName = strings.ToLower(columnName)

		if datatype, ok := dataTypesMapToRudder[bigquery.FieldType(columnType)]; ok {
			schema[tableName][columnName] = datatype
		} else {
			if _, ok := unrecognizedSchema[tableName]; !ok {
				unrecognizedSchema[tableName] = make(model.TableSchema)
			}
			unrecognizedSchema[tableName][columnName] = warehouseutils.MissingDatatype

			warehouseutils.WHCounterStat(stats.Default, warehouseutils.RudderMissingDatatype, &bq.warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}

	return schema, unrecognizedSchema, nil
}

func (bq *BigQuery) Cleanup(context.Context) {
	if bq.db != nil {
		_ = bq.db.Close()
	}
}

func (bq *BigQuery) LoadIdentityMergeRulesTable(ctx context.Context) (err error) {
	identityMergeRulesTable := warehouseutils.IdentityMergeRulesWarehouseTableName(warehouseutils.BQ)
	_, err = bq.LoadTable(ctx, identityMergeRulesTable)
	return err
}

func (bq *BigQuery) LoadIdentityMappingsTable(ctx context.Context) (err error) {
	identityMappingsTable := warehouseutils.IdentityMappingsWarehouseTableName(warehouseutils.BQ)
	_, err = bq.LoadTable(ctx, identityMappingsTable)
	return err
}

func (bq *BigQuery) tableExists(ctx context.Context, tableName string) (exists bool, err error) {
	_, err = bq.db.Dataset(bq.namespace).Table(tableName).Metadata(ctx)
	if err == nil {
		return true, nil
	}
	var e *googleapi.Error
	if errors.As(err, &e) {
		if e.Code == 404 {
			return false, nil
		}
	}
	return false, err
}

func (bq *BigQuery) columnExists(ctx context.Context, columnName, tableName string) (exists bool, err error) {
	tableMetadata, err := bq.db.Dataset(bq.namespace).Table(tableName).Metadata(ctx)
	if err != nil {
		return false, err
	}

	schema := tableMetadata.Schema
	for _, column := range schema {
		if column.Name == columnName {
			return true, nil
		}
	}

	return false, nil
}

type identityRules struct {
	MergeProperty1Type  string `json:"merge_property_1_type"`
	MergeProperty1Value string `json:"merge_property_1_value"`
	MergeProperty2Type  string `json:"merge_property_2_type"`
	MergeProperty2Value string `json:"merge_property_2_value"`
}

func (bq *BigQuery) DownloadIdentityRules(ctx context.Context, gzWriter *misc.GZipWriter) (err error) {
	getFromTable := func(tableName string) (err error) {
		var exists bool
		exists, err = bq.tableExists(ctx, tableName)
		if err != nil || !exists {
			return
		}

		tableMetadata, err := bq.db.Dataset(bq.namespace).Table(tableName).Metadata(ctx)
		if err != nil {
			return err
		}
		totalRows := int64(tableMetadata.NumRows)
		// check if table in warehouse has anonymous_id and user_id and construct accordingly
		hasAnonymousID, err := bq.columnExists(ctx, "anonymous_id", tableName)
		if err != nil {
			return
		}
		hasUserID, err := bq.columnExists(ctx, "user_id", tableName)
		if err != nil {
			return
		}

		var toSelectFields string
		if hasAnonymousID && hasUserID {
			toSelectFields = `anonymous_id, user_id`
		} else if hasAnonymousID {
			toSelectFields = `anonymous_id, null as user_id`
		} else if hasUserID {
			toSelectFields = `null as anonymous_id", user_id`
		} else {
			bq.logger.Infof("BQ: anonymous_id, user_id columns not present in table: %s", tableName)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			sqlStatement := fmt.Sprintf(`SELECT DISTINCT %[1]s FROM %[2]s.%[3]s LIMIT %[4]d OFFSET %[5]d`, toSelectFields, bq.namespace, tableName, batchSize, offset)
			bq.logger.Infof("BQ: Downloading distinct combinations of anonymous_id, user_id: %s, totalRows: %d", sqlStatement, totalRows)
			query := bq.db.Query(sqlStatement)
			job, err := bq.getMiddleware().Run(ctx, query)
			if err != nil {
				break
			}
			status, err := job.Wait(ctx)
			if err != nil {
				return err
			}
			if err := status.Err(); err != nil {
				return err
			}
			it, err := job.Read(ctx)
			if err != nil {
				return err
			}
			for {
				var values []bigquery.Value

				err := it.Next(&values)
				if err != nil {
					if errors.Is(err, iterator.Done) {
						break
					}
					return err
				}
				var anonId, userId string
				if _, ok := values[0].(string); ok {
					anonId = values[0].(string)
				}
				if _, ok := values[1].(string); ok {
					userId = values[1].(string)
				}
				identityRule := identityRules{
					MergeProperty1Type:  "anonymous_id",
					MergeProperty1Value: anonId,
					MergeProperty2Type:  "user_id",
					MergeProperty2Value: userId,
				}
				if identityRule.MergeProperty1Value == "" && identityRule.MergeProperty2Value == "" {
					continue
				}
				bytes, err := json.Marshal(identityRule)
				if err != nil {
					break
				}
				_ = gzWriter.WriteGZ(string(bytes) + "\n")
			}

			offset += batchSize
			if offset >= totalRows {
				break
			}
		}
		return
	}

	tables := []string{"tracks", "pages", "screens", "identifies", "aliases"}
	for _, table := range tables {
		err = getFromTable(table)
		if err != nil {
			return
		}
	}
	return
}

func (bq *BigQuery) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	bq.warehouse = warehouse
	bq.namespace = warehouse.Namespace
	bq.projectID = strings.TrimSpace(bq.warehouse.GetStringDestinationConfig(bq.conf, model.ProjectSetting))
	dbClient, err := bq.connect(ctx, BQCredentials{
		ProjectID:   bq.projectID,
		Credentials: bq.warehouse.GetStringDestinationConfig(bq.conf, model.CredentialsSetting),
	})
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.BQClient, BQ: dbClient}, err
}

func (bq *BigQuery) LoadTestTable(ctx context.Context, location, tableName string, _ map[string]interface{}, _ string) (err error) {
	gcsLocations := warehouseutils.GetGCSLocation(location, warehouseutils.GCSLocationOptions{})
	gcsRef := bigquery.NewGCSReference([]string{gcsLocations}...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	outputTable := partitionedTable(tableName, time.Now().Format("2006-01-02"))
	loader := bq.db.Dataset(bq.namespace).Table(outputTable).LoaderFrom(gcsRef)

	job, err := loader.Run(ctx)
	if err != nil {
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return
	}

	if status.Err() != nil {
		err = status.Err()
		return
	}
	return
}

func (*BigQuery) SetConnectionTimeout(_ time.Duration) {
}

func (*BigQuery) ErrorMappings() []model.JobError {
	return errorsMappings
}

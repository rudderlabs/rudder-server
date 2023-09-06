package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/types"
	"github.com/samber/lo"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/exp/slices"
	bqService "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/middleware"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type BigQuery struct {
	db         *bigquery.Client
	middleware *middleware.Client
	namespace  string
	warehouse  model.Warehouse
	projectID  string
	uploader   whutils.Uploader
	logger     logger.Logger

	config struct {
		setUsersLoadPartitionFirstEventFilter bool
		customPartitionsEnabled               bool
		isUsersTableDedupEnabled              bool
		isDedupEnabled                        bool
		enableDeleteByJobs                    bool
		customPartitionsEnabledWorkspaceIDs   []string
		slowQueryThreshold                    time.Duration
	}
}

type loadTableResponse struct {
	partitionDate    string
	stagingTableName string
}

// String constants for bigquery destination config
const (
	project     = "project"
	credentials = "credentials"
	location    = "location"
)

const (
	provider       = whutils.BQ
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

var primaryKeyMap = map[string]string{
	"users":                         "id",
	"identifies":                    "id",
	whutils.DiscardsTable:           "row_id, column_name, table_name",
	whutils.IdentityMappingsTable:   "merge_property_type, merge_property_value",
	whutils.IdentityMergeRulesTable: "merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value",
}

var partitionKeyMap = map[string]string{
	"users":                         "id",
	"identifies":                    "id",
	whutils.DiscardsTable:           "row_id, column_name, table_name",
	whutils.IdentityMappingsTable:   "merge_property_type, merge_property_value",
	whutils.IdentityMergeRulesTable: "merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value",
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

	bq.logger = log.Child("integrations").Child("bigquery")

	bq.config.setUsersLoadPartitionFirstEventFilter = conf.GetBool("Warehouse.bigquery.setUsersLoadPartitionFirstEventFilter", true)
	bq.config.customPartitionsEnabled = conf.GetBool("Warehouse.bigquery.customPartitionsEnabled", false)
	bq.config.isUsersTableDedupEnabled = conf.GetBool("Warehouse.bigquery.isUsersTableDedupEnabled", false)
	bq.config.isDedupEnabled = conf.GetBool("Warehouse.bigquery.isDedupEnabled", false)
	bq.config.enableDeleteByJobs = conf.GetBool("Warehouse.bigquery.enableDeleteByJobs", false)
	bq.config.customPartitionsEnabledWorkspaceIDs = conf.GetStringSlice("Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs", nil)
	bq.config.slowQueryThreshold = conf.GetDuration("Warehouse.bigquery.slowQueryThreshold", 5, time.Minute)

	return bq
}

func (bq *BigQuery) getMiddleware() *middleware.Client {
	if bq.middleware != nil {
		return bq.middleware
	}
	middleware := middleware.New(
		bq.db,
		middleware.WithLogger(bq.logger),
		middleware.WithKeyAndValues(
			lf.SourceID, bq.warehouse.Source.ID,
			lf.SourceType, bq.warehouse.Source.SourceDefinition.Name,
			lf.DestinationID, bq.warehouse.Destination.ID,
			lf.DestinationType, bq.warehouse.Destination.DestinationDefinition.Name,
			lf.WorkspaceID, bq.warehouse.WorkspaceID,
			lf.Schema, bq.namespace,
		),
		middleware.WithSlowQueryThreshold(bq.config.slowQueryThreshold),
	)
	return middleware
}

func tableSchema(tableSchema model.TableSchema) []*bigquery.FieldSchema {
	return lo.MapToSlice(tableSchema, func(columnName string, columnType string) *bigquery.FieldSchema {
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
	sampleSchema := tableSchema(columnMap)
	metaData := &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{},
	}
	tableRef := bq.db.Dataset(bq.namespace).Table(tableName)
	err := tableRef.Create(ctx, metaData)
	if !checkAndIgnoreAlreadyExistError(err) {
		return fmt.Errorf("create table: %w", err)
	}

	if !bq.dedupEnabled() {
		if err = bq.createTableView(ctx, tableName, columnMap); err != nil {
			return fmt.Errorf("create view: %w", err)
		}
	}
	return nil
}

func (bq *BigQuery) DropTable(ctx context.Context, tableName string) (err error) {
	err = bq.DeleteTable(ctx, tableName)
	if err != nil {
		return
	}
	if !bq.dedupEnabled() {
		err = bq.DeleteTable(ctx, tableName+"_view")
	}
	return
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
	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + partitionKey + viewOrderByStmt + `) AS __row_number FROM ` + "`" + bq.projectID + "." + bq.namespace + "." + tableName + "`" + ` WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 60 * 24 * 1000000), DAY, 'UTC')
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
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			bq.logger.Debugf("BQ: Dataset %s not found", bq.namespace)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (bq *BigQuery) CreateSchema(ctx context.Context) (err error) {
	bq.logger.Infof("BQ: Creating bigquery dataset: %s in project: %s", bq.namespace, bq.projectID)
	location := strings.TrimSpace(whutils.GetConfigValue(location, bq.warehouse))
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
		if e, ok := err.(*googleapi.Error); ok && e.Code == 409 {
			bq.logger.Infof("BQ: Create schema %s failed as schema already exists", bq.namespace)
			return nil
		}
	}
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
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

func (bq *BigQuery) dropStagingTable(ctx context.Context, stagingTableName string) {
	bq.logger.Infof("BQ: Deleting table: %s in bigquery dataset: %s in project: %s", stagingTableName, bq.namespace, bq.projectID)
	err := bq.DeleteTable(ctx, stagingTableName)
	if err != nil {
		bq.logger.Errorf("BQ:  Error dropping staging table %s in bigquery dataset %s in project %s : %v", stagingTableName, bq.namespace, bq.projectID, err)
	}
}

func (bq *BigQuery) DeleteBy(ctx context.Context, tableNames []string, params whutils.DeleteByParams) error {
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

func (bq *BigQuery) loadTable(
	ctx context.Context,
	tableName string,
	skipTempTableDelete bool,
) (*types.LoadTableStats, *loadTableResponse, error) {
	log := bq.logger.With(
		lf.SourceID, bq.warehouse.Source.ID,
		lf.SourceType, bq.warehouse.Source.SourceDefinition.Name,
		lf.DestinationID, bq.warehouse.Destination.ID,
		lf.DestinationType, bq.warehouse.Destination.DestinationDefinition.Name,
		lf.WorkspaceID, bq.warehouse.WorkspaceID,
		lf.Namespace, bq.namespace,
		lf.TableName, tableName,
		lf.LoadTableStrategy, bq.loadTableStrategy(),
	)
	log.Infow("started loading")

	loadFileLocations, err := bq.loadFileLocations(ctx, tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("getting load file locations: %w", err)
	}

	gcsRef := bigquery.NewGCSReference(whutils.GetGCSLocations(
		loadFileLocations,
		whutils.GCSLocationOptions{},
	)...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	if bq.dedupEnabled() {
		return bq.loadTableByMerge(ctx, tableName, gcsRef, log, skipTempTableDelete)
	}
	return bq.loadTableByAppend(ctx, tableName, gcsRef, log)
}

func (bq *BigQuery) loadTableStrategy() string {
	if bq.dedupEnabled() {
		return "MERGE"
	}
	return "APPEND"
}

func (bq *BigQuery) loadFileLocations(
	ctx context.Context,
	tableName string,
) ([]whutils.LoadFile, error) {
	switch tableName {
	case whutils.IdentityMappingsTable, whutils.IdentityMergeRulesTable:
		if loadfile, err := bq.uploader.GetSingleLoadFile(ctx, tableName); err != nil {
			return nil, fmt.Errorf("getting single load file for table %s: %w", tableName, err)
		} else {
			return []whutils.LoadFile{loadfile}, nil
		}
	default:
		metadata := bq.uploader.GetLoadFilesMetadata(
			ctx,
			whutils.GetLoadFilesOptions{
				Table: tableName,
			},
		)
		return metadata, nil
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

	log.Infow("running loader")
	job, err := bq.db.Dataset(bq.namespace).Table(outputTable).LoaderFrom(gcsRef).Run(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("running loader: %w", err)
	}

	log.Infow("waiting for job to complete", "jobID", job.ID())
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("waiting for job: %w", err)
	}
	if err := status.Err(); err != nil {
		return nil, nil, fmt.Errorf("status: %w", err)
	}

	log.Infow("getting job statistics")
	serv, err := bqService.NewService(
		ctx,
		option.WithCredentialsJSON([]byte(whutils.GetConfigValue(credentials, bq.warehouse))),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating service: %w", err)
	}
	bqJob, err := bqService.NewJobsService(serv).Get(
		job.ProjectID(),
		job.ID(),
	).Context(ctx).Location(job.Location()).Fields("statistics").Do()
	if err != nil {
		return nil, nil, fmt.Errorf("getting job: %w", err)
	}

	log.Infow("completed loading")

	tableStats := &types.LoadTableStats{
		RowsInserted: bqJob.Statistics.Load.OutputRows,
	}
	response := &loadTableResponse{
		partitionDate: partitionDate,
	}
	return tableStats, response, nil
}

func (bq *BigQuery) loadTableByMerge(
	ctx context.Context,
	tableName string,
	gcsRef *bigquery.GCSReference,
	log logger.Logger,
	skipTempTableDelete bool,
) (*types.LoadTableStats, *loadTableResponse, error) {
	stagingTableName := whutils.StagingTableName(
		provider,
		tableName,
		tableNameLimit,
	)

	sampleSchema := tableSchema(bq.uploader.GetTableSchemaInWarehouse(
		tableName,
	))

	log.Infow("creating temporary table", lf.StagingTableName, stagingTableName)
	err := bq.db.Dataset(bq.namespace).Table(stagingTableName).Create(ctx, &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("creating staging table: %w", err)
	}

	log.Infow("running copy loader")
	job, err := bq.db.Dataset(bq.namespace).Table(stagingTableName).LoaderFrom(gcsRef).Run(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("running loader: %w", err)
	}

	log.Infow("waiting for copy job to complete", "jobID", job.ID())
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("waiting for job: %w", err)
	}
	if err := status.Err(); err != nil {
		return nil, nil, fmt.Errorf("status: %w", err)
	}

	if !skipTempTableDelete {
		defer bq.dropStagingTable(ctx, stagingTableName)
	}

	mergeIntoStmt := bq.mergeIntoStmt(
		tableName,
		stagingTableName,
		bq.uploader.GetTableSchemaInWarehouse(tableName),
	)

	log.Infow("running merge loader")
	job, err = bq.getMiddleware().Run(ctx, bq.db.Query(mergeIntoStmt))
	if err != nil {
		return nil, nil, fmt.Errorf("running loader: %w", err)
	}

	log.Infow("waiting for merge job to complete", "jobID", job.ID())
	status, err = job.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("waiting for job: %w", err)
	}
	if err := status.Err(); err != nil {
		return nil, nil, fmt.Errorf("status: %w", err)
	}

	log.Infow("getting job statistics")
	serv, err := bqService.NewService(
		ctx,
		option.WithCredentialsJSON([]byte(whutils.GetConfigValue(credentials, bq.warehouse))),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating service: %w", err)
	}
	bqJob, err := bqService.NewJobsService(serv).Get(
		job.ProjectID(),
		job.ID(),
	).Context(ctx).Location(job.Location()).Fields("statistics").Do()
	if err != nil {
		return nil, nil, fmt.Errorf("getting job: %w", err)
	}

	log.Infow("completed loading")

	tableStats := &types.LoadTableStats{
		RowsInserted: bqJob.Statistics.Query.DmlStats.InsertedRowCount,
		RowsUpdated:  bqJob.Statistics.Query.DmlStats.UpdatedRowCount,
	}
	response := &loadTableResponse{
		stagingTableName: stagingTableName,
	}
	return tableStats, response, nil
}

func (bq *BigQuery) mergeIntoStmt(
	tableName string,
	stagingTableName string,
	tableColMap map[string]string,
) string {
	tableColNames := lo.MapToSlice(tableColMap, func(colName string, colType string) string {
		return fmt.Sprintf("`%s`", colName)
	})

	columnNames := strings.Join(tableColNames, ",")

	stagingColumnNames := strings.Join(lo.Map(tableColNames, func(colName string, index int) string {
		return fmt.Sprintf(`staging.%s`, colName)
	}), ",")
	columnsWithValues := strings.Join(lo.Map(tableColNames, func(colName string, index int) string {
		return fmt.Sprintf(`original.%[1]s = staging.%[1]s`, colName)
	}), ",")

	primaryKey := "id"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	primaryJoinClause := strings.Join(lo.Map(strings.Split(primaryKey, ","), func(str string, index int) string {
		return fmt.Sprintf(`original.%[1]s = staging.%[1]s`, strings.Trim(str, " "))
	}), " AND ")

	bqTable := func(name string) string {
		return fmt.Sprintf("`%s`.`%s`", bq.namespace, name)
	}

	var orderByClause string
	if _, ok := tableColMap["received_at"]; ok {
		orderByClause = "ORDER BY received_at DESC"
	}

	return fmt.Sprintf(`
		MERGE INTO %[1]s AS original USING (
		  SELECT
			*
		  FROM
			(
			  SELECT
				*,
				row_number() OVER (PARTITION BY %[7]s %[8]s) AS _rudder_staging_row_number
			  FROM
				%[2]s
			) AS q
		  WHERE
			_rudder_staging_row_number = 1
		) AS staging ON (%[3]s) WHEN MATCHED THEN
		UPDATE
		SET
		  %[6]s WHEN NOT MATCHED THEN INSERT (%[4]s)
		VALUES
		  (%[5]s);
`,
		bqTable(tableName),
		bqTable(stagingTableName),
		primaryJoinClause,
		columnNames,
		stagingColumnNames,
		columnsWithValues,
		partitionKey,
		orderByClause,
	)
}

func (bq *BigQuery) LoadUserTables(ctx context.Context) (errorMap map[string]error) {
	errorMap = map[string]error{whutils.IdentifiesTable: nil}
	bq.logger.Infof("BQ: Starting load for identifies and users tables\n")
	_, identifyLoadTable, err := bq.loadTable(ctx, whutils.IdentifiesTable, true)
	if err != nil {
		errorMap[whutils.IdentifiesTable] = err
		return
	}

	if len(bq.uploader.GetTableSchemaInUpload(whutils.UsersTable)) == 0 {
		return
	}
	errorMap[whutils.UsersTable] = nil

	bq.logger.Infof("BQ: Starting load for %s table", whutils.UsersTable)

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

	bqUsersView := bqTable(whutils.UsersView)
	viewExists, _ := bq.tableExists(ctx, whutils.UsersView)
	if !viewExists {
		bq.logger.Infof("BQ: Creating view: %s in bigquery dataset: %s in project: %s", whutils.UsersView, bq.namespace, bq.projectID)
		_ = bq.createTableView(ctx, whutils.UsersTable, userColMap)
	}

	bqIdentifiesTable := bqTable(whutils.IdentifiesTable)
	partition := fmt.Sprintf("TIMESTAMP('%s')", identifyLoadTable.partitionDate)
	var identifiesFrom string
	if bq.dedupEnabled() {
		identifiesFrom = fmt.Sprintf(`%s WHERE user_id IS NOT NULL %s`, bqTable(identifyLoadTable.stagingTableName), loadedAtFilter())
	} else {
		identifiesFrom = fmt.Sprintf(`%s WHERE _PARTITIONTIME = %s AND user_id IS NOT NULL %s`, bqIdentifiesTable, partition, loadedAtFilter())
	}
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
		partitionedUsersTable := partitionedTable(whutils.UsersTable, identifyLoadTable.partitionDate)
		query := bq.db.Query(sqlStatement)
		query.QueryConfig.Dst = bq.db.Dataset(bq.namespace).Table(partitionedUsersTable)
		query.WriteDisposition = bigquery.WriteAppend

		job, err := bq.getMiddleware().Run(ctx, query)
		if err != nil {
			bq.logger.Errorf("BQ: Error initiating load job: %v\n", err)
			errorMap[whutils.UsersTable] = err
			return
		}
		status, err := job.Wait(ctx)
		if err != nil {
			bq.logger.Errorf("BQ: Error running load job: %v\n", err)
			errorMap[whutils.UsersTable] = fmt.Errorf(`append: %v`, err.Error())
			return
		}

		if status.Err() != nil {
			errorMap[whutils.UsersTable] = status.Err()
			return
		}
	}

	loadUserTableByMerge := func() {
		stagingTableName := whutils.StagingTableName(provider, whutils.UsersTable, tableNameLimit)
		bq.logger.Infof(`BQ: Creating staging table for users: %v`, sqlStatement)
		query := bq.db.Query(sqlStatement)
		query.QueryConfig.Dst = bq.db.Dataset(bq.namespace).Table(stagingTableName)
		query.WriteDisposition = bigquery.WriteAppend
		job, err := bq.getMiddleware().Run(ctx, query)
		if err != nil {
			bq.logger.Errorf("BQ: Error initiating staging table for users : %v\n", err)
			errorMap[whutils.UsersTable] = err
			return
		}

		status, err := job.Wait(ctx)
		if err != nil {
			bq.logger.Errorf("BQ: Error initiating staging table for users %v\n", err)
			errorMap[whutils.UsersTable] = fmt.Errorf(`merge: %v`, err.Error())
			return
		}

		if status.Err() != nil {
			errorMap[whutils.UsersTable] = status.Err()
			return
		}
		defer bq.dropStagingTable(ctx, identifyLoadTable.stagingTableName)
		defer bq.dropStagingTable(ctx, stagingTableName)

		primaryKey := "ID"
		columnNames := append([]string{"ID"}, userColNames...)
		columnNamesStr := strings.Join(columnNames, ",")
		var columnsWithValues, stagingColumnValues string
		for idx, colName := range columnNames {
			columnsWithValues += fmt.Sprintf(`original.%[1]s = staging.%[1]s`, colName)
			stagingColumnValues += fmt.Sprintf(`staging.%s`, colName)
			if idx != len(columnNames)-1 {
				columnsWithValues += `,`
				stagingColumnValues += `,`
			}
		}

		sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s AS original
										USING (
											SELECT %[3]s FROM %[2]s
										) AS staging
										ON (original.%[4]s = staging.%[4]s)
										WHEN MATCHED THEN
										UPDATE SET %[5]s
										WHEN NOT MATCHED THEN
										INSERT (%[3]s) VALUES (%[6]s)`, bqTable(whutils.UsersTable), bqTable(stagingTableName), columnNamesStr, primaryKey, columnsWithValues, stagingColumnValues)
		bq.logger.Infof("BQ: Dedup records for table:%s using staging table: %s\n", whutils.UsersTable, sqlStatement)

		bq.logger.Infof(`BQ: Loading data into users table: %v`, sqlStatement)
		// partitionedUsersTable := partitionedTable(warehouseutils.UsersTable, partitionDate)
		q := bq.db.Query(sqlStatement)
		job, err = bq.getMiddleware().Run(ctx, q)
		if err != nil {
			bq.logger.Errorf("BQ: Error initiating merge load job: %v\n", err)
			errorMap[whutils.UsersTable] = err
			return
		}
		status, err = job.Wait(ctx)
		if err != nil {
			bq.logger.Errorf("BQ: Error running merge load job: %v\n", err)
			errorMap[whutils.UsersTable] = fmt.Errorf(`merge: %v`, err.Error())
			return
		}

		if status.Err() != nil {
			errorMap[whutils.UsersTable] = status.Err()
			return
		}
	}

	if !bq.dedupEnabled() {
		loadUserTableByAppend()
		return
	}

	loadUserTableByMerge()
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
	client, err := bigquery.NewClient(context, cred.ProjectID, opts...)
	return client, err
}

func (bq *BigQuery) connect(ctx context.Context, cred BQCredentials) (*bigquery.Client, error) {
	bq.logger.Infof("BQ: Connecting to BigQuery in project: %s", cred.ProjectID)
	client, err := Connect(ctx, &cred)
	return client, err
}

func (bq *BigQuery) dedupEnabled() bool {
	return bq.config.isDedupEnabled || bq.config.isUsersTableDedupEnabled
}

func (bq *BigQuery) CrashRecover(ctx context.Context) {
	if !bq.dedupEnabled() {
		return
	}
	bq.dropDanglingStagingTables(ctx)
}

func (bq *BigQuery) dropDanglingStagingTables(ctx context.Context) bool {
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
		fmt.Sprintf(`%s%%`, whutils.StagingTablePrefix(provider)),
	)
	query := bq.db.Query(sqlStatement)
	it, err := bq.getMiddleware().Read(ctx, query)
	if err != nil {
		bq.logger.Errorf("WH: BQ: Error dropping dangling staging tables in BQ: %v\nQuery: %s\n", err, sqlStatement)
		return false
	}

	var stagingTableNames []string
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			bq.logger.Errorf("BQ: Error in processing fetched staging tables from information schema in dataset %v : %v", bq.namespace, err)
			return false
		}
		if _, ok := values[0].(string); ok {
			stagingTableNames = append(stagingTableNames, values[0].(string))
		}
	}
	bq.logger.Infof("WH: PG: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		err := bq.DeleteTable(ctx, stagingTableName)
		if err != nil {
			bq.logger.Errorf("WH: BQ:  Error dropping dangling staging table: %s in BQ: %v", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (bq *BigQuery) IsEmpty(ctx context.Context, warehouse model.Warehouse) (empty bool, err error) {
	empty = true
	bq.warehouse = warehouse
	bq.namespace = warehouse.Namespace
	bq.projectID = strings.TrimSpace(whutils.GetConfigValue(project, bq.warehouse))
	bq.logger.Infof("BQ: Connecting to BigQuery in project: %s", bq.projectID)
	bq.db, err = bq.connect(ctx, BQCredentials{
		ProjectID:   bq.projectID,
		Credentials: whutils.GetConfigValue(credentials, bq.warehouse),
	})
	if err != nil {
		return
	}
	defer func() { _ = bq.db.Close() }()

	tables := []string{"tracks", "pages", "screens", "identifies", "aliases"}
	for _, tableName := range tables {
		var exists bool
		exists, err = bq.tableExists(ctx, tableName)
		if err != nil {
			return
		}
		if !exists {
			continue
		}
		count, err := bq.GetTotalCountInTable(ctx, tableName)
		if err != nil {
			return empty, err
		}
		if count > 0 {
			empty = false
			return empty, nil
		}
	}
	return
}

func (bq *BigQuery) Setup(ctx context.Context, warehouse model.Warehouse, uploader whutils.Uploader) (err error) {
	bq.warehouse = warehouse
	bq.namespace = warehouse.Namespace
	bq.uploader = uploader
	bq.projectID = strings.TrimSpace(whutils.GetConfigValue(project, bq.warehouse))

	bq.db, err = bq.connect(ctx, BQCredentials{
		ProjectID:   bq.projectID,
		Credentials: whutils.GetConfigValue(credentials, bq.warehouse),
	})
	return err
}

func (*BigQuery) TestConnection(context.Context, model.Warehouse) (err error) {
	return nil
}

func (bq *BigQuery) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStat, _, err := bq.loadTable(
		ctx,
		tableName,
		false,
	)
	return loadTableStat, err
}

func (bq *BigQuery) AddColumns(ctx context.Context, tableName string, columnsInfo []whutils.ColumnInfo) (err error) {
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
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			// if dataset resource is not found, return empty schema
			return schema, unrecognizedSchema, nil
		}
		return nil, nil, fmt.Errorf("fetching schema: %w", err)
	}

	for {
		var values []bigquery.Value

		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
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
			unrecognizedSchema[tableName][columnName] = whutils.MissingDatatype

			whutils.WHCounterStat(whutils.RudderMissingDatatype, &bq.warehouse, whutils.Tag{Name: "datatype", Value: columnType}).Count(1)
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
	identityMergeRulesTable := whutils.IdentityMergeRulesWarehouseTableName(whutils.BQ)
	_, err = bq.LoadTable(ctx, identityMergeRulesTable)
	return err
}

func (bq *BigQuery) LoadIdentityMappingsTable(ctx context.Context) (err error) {
	identityMappingsTable := whutils.IdentityMappingsWarehouseTableName(whutils.BQ)
	_, err = bq.LoadTable(ctx, identityMappingsTable)
	return err
}

func (bq *BigQuery) tableExists(ctx context.Context, tableName string) (exists bool, err error) {
	_, err = bq.db.Dataset(bq.namespace).Table(tableName).Metadata(ctx)
	if err == nil {
		return true, nil
	}
	if e, ok := err.(*googleapi.Error); ok {
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
				if err == iterator.Done {
					break
				}
				if err != nil {
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

func (bq *BigQuery) GetTotalCountInTable(ctx context.Context, tableName string) (int64, error) {
	var (
		total        int64
		err          error
		sqlStatement string
		ok           bool

		it     *bigquery.RowIterator
		values []bigquery.Value
	)
	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM %[1]s.%[2]s;
	`,
		bq.namespace,
		tableName,
	)

	query := bq.db.Query(sqlStatement)
	if it, err = bq.getMiddleware().Read(ctx, query); err != nil {
		return 0, fmt.Errorf("creating row iterator: %w", err)
	}

	err = it.Next(&values)
	if err == iterator.Done {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("iterating through rows: %w", err)
	}

	if total, ok = values[0].(int64); !ok {
		return 0, fmt.Errorf("converting value to int64: %w", err)
	}

	return total, nil
}

func (bq *BigQuery) Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error) {
	bq.warehouse = warehouse
	bq.namespace = warehouse.Namespace
	bq.projectID = strings.TrimSpace(whutils.GetConfigValue(project, bq.warehouse))
	dbClient, err := bq.connect(ctx, BQCredentials{
		ProjectID:   bq.projectID,
		Credentials: whutils.GetConfigValue(credentials, bq.warehouse),
	})
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.BQClient, BQ: dbClient}, err
}

func (bq *BigQuery) LoadTestTable(ctx context.Context, location, tableName string, _ map[string]interface{}, _ string) (err error) {
	gcsLocations := whutils.GetGCSLocation(location, whutils.GCSLocationOptions{})
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

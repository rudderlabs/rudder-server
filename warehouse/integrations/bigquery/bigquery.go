package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/samber/lo"
	bqservice "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/middleware"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type BigQuery struct {
	db        *middleware.Client
	namespace string
	warehouse model.Warehouse
	projectID string
	uploader  warehouseutils.Uploader
	conf      *config.Config
	logger    logger.Logger
	now       func() time.Time

	config struct {
		setUsersLoadPartitionFirstEventFilter bool
		customPartitionsEnabled               bool
		enableDeleteByJobs                    bool
		customPartitionsEnabledWorkspaceIDs   []string
		slowQueryThreshold                    time.Duration
		loadByFolderPath                      bool
	}
}

type loadTableResponse struct {
	partitionDate string
}

const (
	provider       = warehouseutils.BQ
	tableNameLimit = 127
)

// dataTypesMap maps datatype stored in rudder to datatype in bigquery
var dataTypesMap = map[string]bigquery.FieldType{
	"boolean":  bigquery.BooleanFieldType,
	"int":      bigquery.IntegerFieldType,
	"float":    bigquery.FloatFieldType,
	"string":   bigquery.StringFieldType,
	"datetime": bigquery.TimestampFieldType,
}

// dataTypesMapToRudder maps datatype in bigquery to datatype stored in rudder
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
	warehouseutils.UsersTable:              "id",
	warehouseutils.IdentifiesTable:         "id",
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
	bq.now = timeutil.Now

	bq.config.setUsersLoadPartitionFirstEventFilter = conf.GetBool("Warehouse.bigquery.setUsersLoadPartitionFirstEventFilter", true)
	bq.config.customPartitionsEnabled = conf.GetBool("Warehouse.bigquery.customPartitionsEnabled", false)
	bq.config.enableDeleteByJobs = conf.GetBool("Warehouse.bigquery.enableDeleteByJobs", false)
	bq.config.customPartitionsEnabledWorkspaceIDs = conf.GetStringSlice("Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs", nil)
	bq.config.slowQueryThreshold = conf.GetDuration("Warehouse.bigquery.slowQueryThreshold", 5, time.Minute)
	bq.config.loadByFolderPath = conf.GetBool("Warehouse.bigquery.loadByFolderPath", false)

	return bq
}

func getTableSchema(tableSchema model.TableSchema) []*bigquery.FieldSchema {
	return lo.MapToSlice(tableSchema, func(columnName, columnType string) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]}
	})
}

func (bq *BigQuery) DeleteTable(ctx context.Context, tableName string) error {
	return bq.db.Dataset(bq.namespace).Table(tableName).Delete(ctx)
}

// CreateTable creates a table in BigQuery with the provided schema
// It also creates a view for the table to deduplicate the data
// If custom partitioning is enabled, it creates a table with custom partitioning based on the partition column and type
// only if the partition column exists in the schema.
// Otherwise, it creates a table with ingestion-time partitioning
func (bq *BigQuery) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) error {
	sampleSchema := getTableSchema(columnMap)
	partitionColumn, partitionType := bq.partitionColumn(), bq.partitionType()

	log := bq.logger.Withn(
		logger.NewStringField(logfield.ProjectID, bq.projectID),
		obskit.Namespace(bq.namespace),
		logger.NewStringField(logfield.TableName, tableName),
		logger.NewStringField("partitionColumn", partitionColumn),
		logger.NewStringField("partitionType", partitionType),
	)

	var timePartitioning *bigquery.TimePartitioning
	if partitionColumn == "" || partitionType == "" {
		log.Infon("Creating table: Partition column or partition type not provided, using ingestion-time partitioning")
		timePartitioning = &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		}
	} else {
		if err := bq.checkValidPartitionColumn(partitionColumn); err != nil {
			return fmt.Errorf("check valid partition column: %w", err)
		}
		bqPartitionType, err := bq.bigqueryPartitionType(partitionType)
		if err != nil {
			return fmt.Errorf("bigquery partition type: %w", err)
		}

		// If partition column is _PARTITIONTIME and partition type is not empty, then we only set the partition type
		if partitionColumn == "_PARTITIONTIME" {
			log.Infon("Creating table: Partition column is _PARTITIONTIME")
			timePartitioning = &bigquery.TimePartitioning{
				Type: bqPartitionType,
			}
		} else {
			// Checking if the partition column exists in the schema, because in case of
			// 1. rudder_discards: we only have timestamp column.
			// 2. rudder_identity_merge_rules: we don't have any column.
			// 3. rudder_identity_mappings: we don't have any column.
			_, ok := columnMap[partitionColumn]
			if ok {
				log.Infon("Creating table: Partition column found in schema")
				timePartitioning = &bigquery.TimePartitioning{
					Field: partitionColumn,
					Type:  bqPartitionType,
				}
			} else {
				log.Infon("Creating table: Partition column not found in schema, using ingestion-time partitioning")
				timePartitioning = &bigquery.TimePartitioning{
					Type: bigquery.DayPartitioningType,
				}
			}
		}
	}

	err := bq.db.Dataset(bq.namespace).Table(tableName).Create(ctx, &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: timePartitioning,
	})
	if !checkAndIgnoreAlreadyExistError(err) {
		return fmt.Errorf("create table: %w", err)
	}
	if err = bq.createTableView(ctx, tableName, columnMap); err != nil {
		return fmt.Errorf("create view: %w", err)
	}
	return nil
}

// createTableView creates a view for the table to deduplicate the data
// If custom partition is enabled, it creates a view with the partition column and type. Otherwise, it creates a view with ingestion-time partitioning
func (bq *BigQuery) createTableView(ctx context.Context, tableName string, columnMap model.TableSchema) error {
	partitionKey := "id"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	var viewOrderByStmt string
	if _, ok := columnMap["loaded_at"]; ok {
		viewOrderByStmt = " ORDER BY loaded_at DESC "
	}

	var (
		granularity, partitionFilter   string
		partitionColumn, partitionType = bq.partitionColumn(), bq.partitionType()
	)

	if partitionColumn == "" || partitionType == "" {
		granularity = "DAY"
		partitionFilter = "_PARTITIONTIME"
	} else {
		if err := bq.checkValidPartitionColumn(partitionColumn); err != nil {
			return fmt.Errorf("check valid partition column: %w", err)
		}
		bqPartitionType, err := bq.bigqueryPartitionType(partitionType)
		if err != nil {
			return fmt.Errorf("bigquery partition type: %w", err)
		}

		if partitionColumn == "_PARTITIONTIME" {
			granularity = string(bqPartitionType)
			partitionFilter = "_PARTITIONTIME"
		} else {
			_, ok := columnMap[partitionColumn]
			if ok {
				bq.logger.Infon("Creating view: Partition column found in schema",
					logger.NewStringField("partitionColumn", partitionColumn),
				)
				granularity = string(bqPartitionType)
				partitionFilter = `TIMESTAMP_TRUNC(` + partitionColumn + `, ` + granularity + `, 'UTC')`
			} else {
				bq.logger.Warnn("Creating view: Partition column not found in schema",
					logger.NewStringField("partitionColumn", partitionColumn),
				)
				granularity = "DAY"
				partitionFilter = "_PARTITIONTIME"
			}
		}
	}

	bq.logger.Infon("Creating view",
		logger.NewStringField("view", tableName+"_view"),
		logger.NewStringField("partitionColumn", partitionColumn),
		logger.NewStringField("partitionKey", partitionKey),
	)

	// assuming it has field named id upon which dedup is done in view
	// the following view takes the last two months into consideration i.e. 60 * 60 * 24 * 60 * 1000000
	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + partitionKey + viewOrderByStmt + `) AS __row_number
			FROM ` + "`" + bq.projectID + "." + bq.namespace + "." + tableName + "`" + `
			WHERE
				` + partitionFilter + ` BETWEEN TIMESTAMP_TRUNC(
					TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 24 * 60 * 1000000),
					` + granularity + `,
					'UTC'
				)
				AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), ` + granularity + `, 'UTC')
		)
		WHERE __row_number = 1`
	metaData := &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}

	return bq.db.Dataset(bq.namespace).Table(tableName+"_view").Create(ctx, metaData)
}

func (bq *BigQuery) DropTable(ctx context.Context, tableName string) error {
	if err := bq.DeleteTable(ctx, tableName); err != nil {
		return err
	}
	return bq.DeleteTable(ctx, tableName+"_view")
}

func (bq *BigQuery) schemaExists(ctx context.Context, _, _ string) (exists bool, err error) {
	ds := bq.db.Dataset(bq.namespace)
	_, err = ds.Metadata(ctx)
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 404 {
			bq.logger.Debugn("Dataset not found",
				logger.NewStringField(logfield.ProjectID, bq.projectID),
				obskit.Namespace(bq.namespace),
			)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (bq *BigQuery) CreateSchema(ctx context.Context) (err error) {
	location := strings.TrimSpace(bq.warehouse.GetStringDestinationConfig(bq.conf, model.LocationSetting))
	if location == "" {
		location = "US"
	}

	log := bq.logger.Withn(
		logger.NewStringField(logfield.ProjectID, bq.projectID),
		logger.NewStringField("location", location),
		obskit.Namespace(bq.namespace),
	)

	log.Infon("Creating bigquery dataset")

	var schemaExists bool
	schemaExists, err = bq.schemaExists(ctx, bq.namespace, location)
	if err != nil {
		log.Warnn("Checking if schema exists")
		return err
	}
	if schemaExists {
		log.Infon("Skipping creating schema since it already exists")
		return
	}

	ds := bq.db.Dataset(bq.namespace)
	meta := &bigquery.DatasetMetadata{
		Location: location,
	}
	log.Infon("Creating schema")
	err = ds.Create(ctx, meta)
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 409 {
			log.Warnn("Create schema failed as schema already exists")
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

		bq.logger.Infon("Cleaning up the following table",
			obskit.DestinationID(bq.warehouse.Destination.ID),
			logger.NewStringField(logfield.ProjectID, bq.projectID),
			obskit.Namespace(bq.namespace),
			logger.NewStringField(logfield.TableName, tb),
			logger.NewStringField(logfield.Query, sqlStatement),
		)

		if !bq.config.enableDeleteByJobs {
			continue
		}

		query := bq.db.Query(sqlStatement)
		query.Parameters = []bigquery.QueryParameter{
			{Name: "jobrunid", Value: params.JobRunId},
			{Name: "taskrunid", Value: params.TaskRunId},
			{Name: "sourceid", Value: params.SourceId},
			{Name: "starttime", Value: params.StartTime},
		}
		job, err := bq.db.Run(ctx, query)
		if err != nil {
			bq.logger.Warnn("Error initiating load job", obskit.Error(err))
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			bq.logger.Warnn("Error running job", obskit.Error(err))
			return err
		}
		if status.Err() != nil {
			return status.Err()
		}
	}
	return nil
}

func (bq *BigQuery) loadTable(ctx context.Context, tableName string) (
	*types.LoadTableStats, *loadTableResponse, error,
) {
	log := bq.logger.Withn(
		obskit.SourceID(bq.warehouse.Source.ID),
		obskit.SourceType(bq.warehouse.Source.SourceDefinition.Name),
		obskit.DestinationID(bq.warehouse.Destination.ID),
		obskit.DestinationType(bq.warehouse.Destination.DestinationDefinition.Name),
		obskit.WorkspaceID(bq.warehouse.WorkspaceID),
		obskit.Namespace(bq.namespace),
		logger.NewStringField(logfield.TableName, tableName),
		logger.NewBoolField(logfield.ShouldMerge, false), // we don't support merging in BigQuery due to its cost limitations
	)
	log.Infon("started loading")

	gcsReferences, err := bq.gcsReferences(ctx, tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("getting gcs references: %w", err)
	}

	gcsRef := bigquery.NewGCSReference(gcsReferences...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	return bq.loadTableByAppend(ctx, tableName, gcsRef, log)
}

func (bq *BigQuery) gcsReferences(
	ctx context.Context,
	tableName string,
) ([]string, error) {
	switch tableName {
	case warehouseutils.IdentityMappingsTable, warehouseutils.IdentityMergeRulesTable:
		loadfile, err := bq.uploader.GetSingleLoadFile(
			ctx,
			tableName,
		)
		if err != nil {
			return nil, fmt.Errorf("getting single load file for table %s: %w", tableName, err)
		}

		locations := warehouseutils.GetGCSLocations([]warehouseutils.LoadFile{loadfile}, warehouseutils.GCSLocationOptions{})
		return locations, nil
	default:
		if bq.config.loadByFolderPath {
			objectLocation, err := bq.uploader.GetSampleLoadFileLocation(ctx, tableName)
			if err != nil {
				return nil, fmt.Errorf("getting sample load file location for table %s: %w", tableName, err)
			}
			gcsLocation := warehouseutils.GetGCSLocation(objectLocation, warehouseutils.GCSLocationOptions{})
			gcsLocationFolder := loadFolder(gcsLocation)

			return []string{gcsLocationFolder}, nil
		} else {
			loadFilesMetadata, err := bq.uploader.GetLoadFilesMetadata(
				ctx,
				warehouseutils.GetLoadFilesOptions{Table: tableName},
			)
			if err != nil {
				return nil, fmt.Errorf("getting load files metadata for table %s: %w", tableName, err)
			}

			locations := warehouseutils.GetGCSLocations(loadFilesMetadata, warehouseutils.GCSLocationOptions{})
			return locations, nil
		}
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
	partitionDate, err := bq.partitionDate()
	if err != nil {
		return nil, nil, fmt.Errorf("partition date: %w", err)
	}

	var outputTable string
	if bq.avoidPartitionDecorator() {
		outputTable = tableName
	} else {
		outputTable = partitionedTable(tableName, partitionDate)
	}

	log.Infon("loading data into main table")
	job, err := bq.db.Dataset(bq.namespace).Table(outputTable).LoaderFrom(gcsRef).Run(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("moving data into main table: %w", err)
	}

	log.Debugn("waiting for append job to complete",
		logger.NewStringField("jobID", job.ID()),
	)
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("waiting for append job: %w", err)
	}
	if err := status.Err(); err != nil {
		return nil, nil, fmt.Errorf("status for append job: %w", err)
	}

	log.Debugn("job statistics")
	statistics, err := bq.jobStatistics(ctx, job)
	if err != nil {
		return nil, nil, fmt.Errorf("append job statistics: %w", err)
	}

	log.Infon("completed loading")

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
) (*bqservice.JobStatistics, error) {
	serv, err := bqservice.NewService(
		ctx,
		option.WithCredentialsJSON([]byte(bq.warehouse.GetStringDestinationConfig(bq.conf, model.CredentialsSetting))),
	)
	if err != nil {
		return nil, fmt.Errorf("creating service: %w", err)
	}

	bqJobGetCall := bqservice.NewJobsService(serv).Get(
		job.ProjectID(),
		job.ID(),
	)
	bqJob, err := bqJobGetCall.Context(ctx).Location(job.Location()).Fields("statistics").Do()
	if err != nil {
		// In case of rate limit error, return empty statistics
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 429 {
			return &bqservice.JobStatistics{}, nil
		}
		return nil, fmt.Errorf("getting job: %w", err)
	}
	return bqJob.Statistics, nil
}

func (bq *BigQuery) LoadUserTables(ctx context.Context) (errorMap map[string]error) {
	log := bq.logger.Withn(
		logger.NewStringField(logfield.ProjectID, bq.projectID),
		obskit.SourceID(bq.warehouse.Source.ID),
		obskit.SourceType(bq.warehouse.Source.SourceDefinition.Name),
		obskit.DestinationID(bq.warehouse.Destination.ID),
		obskit.DestinationType(bq.warehouse.Destination.DestinationDefinition.Name),
		obskit.WorkspaceID(bq.warehouse.WorkspaceID),
		obskit.Namespace(bq.namespace),
		logger.NewBoolField(logfield.ShouldMerge, false),
	)
	log.Infon("Starting load for identifies and users tables")

	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}

	_, identifyLoadTable, err := bq.loadTable(ctx, warehouseutils.IdentifiesTable)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(bq.uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	log = bq.logger.Withn(logger.NewStringField(logfield.TableName, warehouseutils.UsersTable))
	log.Infon("started loading")

	stagingUsersTableName := warehouseutils.StagingTableName(provider, warehouseutils.UsersTable, tableNameLimit)
	err = bq.createAndLoadStagingUsersTable(ctx, stagingUsersTableName)
	if err != nil {
		log.Warnn("Creating and loading staging table", obskit.Error(err))
		errorMap[warehouseutils.UsersTable] = fmt.Errorf(`creating and loading staging table: %w`, err)
		return
	}

	firstValueSQL := func(column string) string {
		return fmt.Sprintf("FIRST_VALUE(`%[1]s` IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `%[1]s`", column)
	}

	userColMap := bq.uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, fmt.Sprintf("`%s`", colName))
		firstValProps = append(firstValProps, firstValueSQL(colName))
	}

	bqTable := func(name string) string {
		return fmt.Sprintf("`%s`.`%s`", bq.namespace, name)
	}

	bqUsersView := bqTable(warehouseutils.UsersView)
	viewExists, _ := bq.tableExists(ctx, warehouseutils.UsersView)
	if !viewExists {
		log.Infon("Creating view",
			logger.NewStringField("view", warehouseutils.UsersView),
		)
		if err := bq.createTableView(ctx, warehouseutils.UsersTable, userColMap); err != nil {
			log.Warnn("Creating view failed",
				obskit.Error(err),
			)
		}
	}

	sqlStatement := fmt.Sprintf(`SELECT DISTINCT * FROM (
			SELECT id, %[1]s FROM (
				(
					SELECT id, %[2]s FROM %[3]s WHERE (
						id in (SELECT id FROM %[4]s)
					)
				) UNION ALL (
					SELECT id, %[2]s FROM %[4]s
				)
			)
		)`,
		strings.Join(firstValProps, ","),
		strings.Join(userColNames, ","),
		bqUsersView,
		bqTable(stagingUsersTableName),
	)

	log.Infon("Loading data")
	var partitionedUsersTable string
	if bq.avoidPartitionDecorator() {
		partitionedUsersTable = warehouseutils.UsersTable
	} else {
		partitionedUsersTable = partitionedTable(warehouseutils.UsersTable, identifyLoadTable.partitionDate)
	}

	query := bq.db.Query(sqlStatement)
	query.QueryConfig.Dst = bq.db.Dataset(bq.namespace).Table(partitionedUsersTable)
	query.WriteDisposition = bigquery.WriteAppend

	job, err := bq.db.Run(ctx, query)
	if err != nil {
		log.Warnn("Initiating load job", obskit.Error(err))
		errorMap[warehouseutils.UsersTable] = fmt.Errorf(`executing users append: %w`, err)
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Warnn("Running load job", obskit.Error(err))
		errorMap[warehouseutils.UsersTable] = fmt.Errorf(`waiting for users append: %w`, err)
		return
	}
	if status.Err() != nil {
		log.Warnn("Job status", obskit.Error(status.Err()))
		errorMap[warehouseutils.UsersTable] = fmt.Errorf(`status for users append: %w`, status.Err())
		return
	}
	return errorMap
}

func (bq *BigQuery) createAndLoadStagingUsersTable(ctx context.Context, stagingTable string) error {
	gcsReferences, err := bq.gcsReferences(ctx, warehouseutils.UsersTable)
	if err != nil {
		return fmt.Errorf("getting gcs references: %w", err)
	}

	gcsRef := bigquery.NewGCSReference(gcsReferences...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	usersSchema := getTableSchema(bq.uploader.GetTableSchemaInWarehouse(warehouseutils.UsersTable))
	metaData := &bigquery.TableMetadata{
		Schema: usersSchema,
	}

	err = bq.db.Dataset(bq.namespace).Table(stagingTable).Create(ctx, metaData)
	if err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	job, err := bq.db.Dataset(bq.namespace).Table(stagingTable).LoaderFrom(gcsRef).Run(ctx)
	if err != nil {
		return fmt.Errorf("moving data into staging table: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for staging table load job: %w", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("status for staging table load job: %w", err)
	}
	return nil
}

func (bq *BigQuery) connect(ctx context.Context) (*middleware.Client, error) {
	var (
		projectID   = bq.projectID
		credentials = bq.warehouse.GetStringDestinationConfig(bq.conf, model.CredentialsSetting)
	)

	bq.logger.Infon("Connecting to BigQuery", logger.NewStringField(projectID, projectID))

	var opts []option.ClientOption
	if !googleutil.ShouldSkipCredentialsInit(credentials) {
		credBytes := []byte(credentials)
		if err := googleutil.CompatibleGoogleCredentialsJSON(credBytes); err != nil {
			return nil, err
		}
		opts = append(opts, option.WithCredentialsJSON(credBytes))
	}

	bqClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}

	middlewareClient := middleware.New(
		bqClient,
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
	return middlewareClient, nil
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
	it, err := bq.db.Read(ctx, query)
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
	bq.logger.Infon("Dropping dangling staging tables",
		logger.NewStringField(logfield.ProjectID, bq.projectID),
		obskit.Namespace(bq.namespace),
		logger.NewIntField("length", int64(len(stagingTableNames))),
		logger.NewField("stagingTableNames", stagingTableNames),
	)
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
	bq.db, err = bq.connect(ctx)
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

	bq.db, err = bq.connect(ctx)
	return err
}

func (*BigQuery) TestConnection(_ context.Context, _ model.Warehouse) (err error) {
	return nil
}

func (bq *BigQuery) LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error) {
	loadTableStat, _, err := bq.loadTable(ctx, tableName)
	return loadTableStat, err
}

func (bq *BigQuery) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	bq.logger.Infon("Adding columns",
		obskit.DestinationID(bq.warehouse.Destination.ID),
		logger.NewStringField(logfield.ProjectID, bq.projectID),
		obskit.Namespace(bq.namespace),
		logger.NewStringField(logfield.TableName, tableName),
	)

	tableRef := bq.db.Dataset(bq.namespace).Table(tableName)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("getting metadata for table %s: %w", tableName, err)
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
				bq.logger.Infon("Column %s already exists on %s.%s \nResponse: %v",
					obskit.Namespace(bq.namespace),
					logger.NewStringField(logfield.TableName, tableName),
					logger.NewStringField(logfield.ColumnName, columnsInfo[0].Name),
					obskit.Error(err),
				)
				return nil
			}
		}
	}
	return err
}

func (*BigQuery) AlterColumn(_ context.Context, _, _, _ string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

// FetchSchema queries bigquery and returns the schema associated with provided namespace
func (bq *BigQuery) FetchSchema(ctx context.Context) (model.Schema, error) {
	schema := make(model.Schema)

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
		  AND
		  (t.table_name NOT LIKE '%s')
		  AND (
			c.column_name != '_PARTITIONTIME'
			OR c.column_name IS NULL
		  );
	`,
		bq.namespace,
		fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider)),
	)
	query := bq.db.Query(sqlStatement)

	it, err := bq.db.Read(ctx, query)
	if err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 404 {
			// if dataset resource is not found, return empty schema
			return schema, nil
		}
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	for {
		var values []bigquery.Value

		err := it.Next(&values)
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, fmt.Errorf("iterating schema: %w", err)
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
			warehouseutils.WHCounterStat(stats.Default, warehouseutils.RudderMissingDatatype, &bq.warehouse, warehouseutils.Tag{Name: "datatype", Value: columnType}).Count(1)
		}
	}

	return schema, nil
}

func (bq *BigQuery) Cleanup(ctx context.Context) {
	if bq.db != nil {
		err := bq.dropDanglingStagingTables(ctx)
		if err != nil {
			bq.logger.Warnn("Error dropping dangling staging tables",
				obskit.DestinationID(bq.warehouse.Destination.ID),
				obskit.DestinationType(bq.warehouse.Destination.DestinationDefinition.Name),
				obskit.SourceID(bq.warehouse.Source.ID),
				obskit.SourceType(bq.warehouse.Source.SourceDefinition.Name),
				obskit.WorkspaceID(bq.warehouse.WorkspaceID),
				obskit.Namespace(bq.warehouse.Namespace),
				obskit.Error(err),
			)
		}

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
			bq.logger.Infon("anonymous_id, user_id columns not present",
				logger.NewStringField(logfield.TableName, tableName),
			)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			sqlStatement := fmt.Sprintf(`SELECT DISTINCT %[1]s FROM %[2]s.%[3]s LIMIT %[4]d OFFSET %[5]d`, toSelectFields, bq.namespace, tableName, batchSize, offset)
			bq.logger.Infon("Downloading distinct combinations of anonymous_id, user_id",
				logger.NewStringField(logfield.Query, sqlStatement),
				logger.NewIntField(logfield.TotalRows, totalRows),
			)
			query := bq.db.Query(sqlStatement)
			job, err := bq.db.Run(ctx, query)
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
	dbClient, err := bq.connect(ctx)
	if err != nil {
		return client.Client{}, err
	}
	return client.Client{Type: client.BQClient, BQ: dbClient.Client}, err
}

func (bq *BigQuery) LoadTestTable(ctx context.Context, location, tableName string, _ map[string]interface{}, _ string) error {
	gcsLocation := warehouseutils.GetGCSLocation(location, warehouseutils.GCSLocationOptions{})

	var gcsReference string
	if bq.config.loadByFolderPath {
		gcsReference = loadFolder(gcsLocation)
	} else {
		gcsReference = gcsLocation
	}

	gcsRef := bigquery.NewGCSReference(gcsReference)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	partitionDate, err := bq.partitionDate()
	if err != nil {
		return fmt.Errorf("partition date: %w", err)
	}

	var outputTable string
	if bq.avoidPartitionDecorator() {
		outputTable = tableName
	} else {
		outputTable = partitionedTable(tableName, partitionDate)
	}

	loader := bq.db.Dataset(bq.namespace).Table(outputTable).LoaderFrom(gcsRef)

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("loading test data into table: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for test data load job: %w", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("status for test data load job: %w", status.Err())
	}
	return nil
}

func loadFolder(objectLocation string) string {
	return warehouseutils.GetLocationFolder(objectLocation) + "/*"
}

func (*BigQuery) SetConnectionTimeout(_ time.Duration) {
}

func (*BigQuery) ErrorMappings() []model.JobError {
	return errorsMappings
}

package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	partitionExpiryUpdated                map[string]bool
	partitionExpiryUpdatedLock            sync.RWMutex
	pkgLogger                             logger.LoggerI
	setUsersLoadPartitionFirstEventFilter bool
	stagingTablePrefix                    string
	isUsersTableDedupEnabled              bool
)

type HandleT struct {
	BQContext context.Context
	Db        *bigquery.Client
	Namespace string
	Warehouse warehouseutils.WarehouseT
	ProjectID string
	Uploader  warehouseutils.UploaderI
}

// String constants for bigquery destination config
const (
	GCPProjectID   = "project"
	GCPCredentials = "credentials"
	GCPLocation    = "location"
)

const PROVIDER = "BQ"

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

func getTableSchema(columns map[string]string) []*bigquery.FieldSchema {
	var schema []*bigquery.FieldSchema
	for columnName, columnType := range columns {
		schema = append(schema, &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]})
	}
	return schema
}
func (bq *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	pkgLogger.Infof("BQ: Creating table: %s in bigquery dataset: %s in project: %s", tableName, bq.Namespace, bq.ProjectID)
	sampleSchema := getTableSchema(columnMap)
	metaData := &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{},
	}
	tableRef := bq.Db.Dataset(bq.Namespace).Table(tableName)
	err = tableRef.Create(bq.BQContext, metaData)
	if !checkAndIgnoreAlreadyExistError(err) {
		return
	}

	err = bq.createTableView(tableName, columnMap)
	return
}

func (bq *HandleT) createTableView(tableName string, columnMap map[string]string) (err error) {
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
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + partitionKey + viewOrderByStmt + `) AS __row_number FROM ` + "`" + bq.ProjectID + "." + bq.Namespace + "." + tableName + "`" + ` WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 60 * 24 * 1000000), DAY, 'UTC')
					AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
			)
		WHERE __row_number = 1`
	metaData := &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}
	tableRef := bq.Db.Dataset(bq.Namespace).Table(tableName + "_view")
	err = tableRef.Create(bq.BQContext, metaData)
	return
}

func (bq *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	pkgLogger.Infof("BQ: Adding columns in table %s in bigquery dataset: %s in project: %s", tableName, bq.Namespace, bq.ProjectID)
	tableRef := bq.Db.Dataset(bq.Namespace).Table(tableName)
	meta, err := tableRef.Metadata(bq.BQContext)
	if err != nil {
		return err
	}
	newSchema := append(meta.Schema,
		&bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]},
	)
	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	_, err = tableRef.Update(bq.BQContext, update, meta.ETag)
	return
}

func (bq *HandleT) schemaExists(schemaname string, location string) (exists bool, err error) {
	ds := bq.Db.Dataset(bq.Namespace)
	_, err = ds.Metadata(bq.BQContext)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			pkgLogger.Debugf("BQ: Dataset %s not found", bq.Namespace)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (bq *HandleT) CreateSchema() (err error) {
	pkgLogger.Infof("BQ: Creating bigquery dataset: %s in project: %s", bq.Namespace, bq.ProjectID)
	location := strings.TrimSpace(warehouseutils.GetConfigValue(GCPLocation, bq.Warehouse))
	if location == "" {
		location = "US"
	}

	var schemaExists bool
	schemaExists, err = bq.schemaExists(bq.Namespace, location)
	if err != nil {
		pkgLogger.Errorf("BQ: Error checking if schema: %s exists: %v", bq.Namespace, err)
		return err
	}
	if schemaExists {
		pkgLogger.Infof("BQ: Skipping creating schema: %s since it already exists", bq.Namespace)
		return
	}

	ds := bq.Db.Dataset(bq.Namespace)
	meta := &bigquery.DatasetMetadata{
		Location: location,
	}
	pkgLogger.Infof("BQ: Creating schema: %s ...", bq.Namespace)
	err = ds.Create(bq.BQContext, meta)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 409 {
			pkgLogger.Infof("BQ: Create schema %s failed as schema already exists", bq.Namespace)
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
				pkgLogger.Debugf("BQ: Google API returned error with code: %v", e.Code)
				return true
			}
		}
		return false
	}
	return true
}

func (bq *HandleT) dropStagingTable(stagingTableName string) {
	pkgLogger.Infof("BQ: Deleting table: %s in bigquery dataset: %s in project: %s", stagingTableName, bq.Namespace, bq.ProjectID)
	tableRef := bq.Db.Dataset(bq.Namespace).Table(stagingTableName)
	err := tableRef.Delete(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ:  Error dropping staging table %s in bigquery dataset %s in project %s : %v", stagingTableName, bq.Namespace, bq.ProjectID, err)
	}
}

func partitionedTable(tableName string, partitionDate string) string {
	return fmt.Sprintf(`%s$%v`, tableName, strings.ReplaceAll(partitionDate, "-", ""))
}

func (bq *HandleT) loadTable(tableName string, forceLoad bool, getLoadFileLocFromTableUploads bool) (partitionDate string, err error) {
	pkgLogger.Infof("BQ: Starting load for table:%s\n", tableName)
	var loadFiles []warehouseutils.LoadFileT
	if getLoadFileLocFromTableUploads {
		loadFile, err := bq.Uploader.GetSingleLoadFile(tableName)
		if err != nil {
			return "", err
		}
		loadFiles = append(loadFiles, loadFile)
	} else {
		loadFiles = bq.Uploader.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName})
	}
	gcsLocations := warehouseutils.GetGCSLocations(loadFiles, warehouseutils.GCSLocationOptionsT{})
	pkgLogger.Infof("BQ: Loading data into table: %s in bigquery dataset: %s in project: %s from %v", tableName, bq.Namespace, bq.ProjectID, loadFiles)
	gcsRef := bigquery.NewGCSReference(gcsLocations...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 0
	gcsRef.IgnoreUnknownValues = false

	partitionDate = time.Now().Format("2006-01-02")
	outputTable := partitionedTable(tableName, partitionDate)

	// create partitioned table in format tableName$20191221
	loader := bq.Db.Dataset(bq.Namespace).Table(outputTable).LoaderFrom(gcsRef)

	job, err := loader.Run(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ: Error initiating load job: %v\n", err)
		return
	}
	status, err := job.Wait(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("BQ: Error running load job: %v\n", err)
		return
	}

	if status.Err() != nil {
		return "", status.Err()
	}
	return
}

func (bq *HandleT) LoadUserTables() (errorMap map[string]error) {
	errorMap = map[string]error{warehouseutils.IdentifiesTable: nil}
	pkgLogger.Infof("BQ: Starting load for identifies and users tables\n")
	partitionDate, err := bq.loadTable(warehouseutils.IdentifiesTable, true, false)
	if err != nil {
		errorMap[warehouseutils.IdentifiesTable] = err
		return
	}

	if len(bq.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) == 0 {
		return
	}
	errorMap[warehouseutils.UsersTable] = nil

	pkgLogger.Infof("BQ: Starting load for %s table", warehouseutils.UsersTable)

	firstValueSQL := func(column string) string {
		return fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY id ORDER BY received_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, column)
	}

	loadedAtFilter := func() string {
		// get first event received_at time in this upload for identifies table
		firstEventAt := func() time.Time {
			return bq.Uploader.GetLoadFileGenStartTIme()
		}

		firstEventTime := firstEventAt()
		if !setUsersLoadPartitionFirstEventFilter || firstEventTime.IsZero() {
			return ""
		}

		// TODO: Add this filter to optimize reading from identifies table since first event in upload
		// rather than entire day's records
		// commented it since firstEventAt is not stored in UTC format in earlier versions
		firstEventAtFormatted := firstEventTime.Format(misc.RFC3339Milli)
		return fmt.Sprintf(`AND loaded_at >= TIMESTAMP('%v')`, firstEventAtFormatted)
	}

	userColMap := bq.Uploader.GetTableSchemaInWarehouse("users")
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "id" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, firstValueSQL(colName))
	}

	bqTable := func(name string) string { return fmt.Sprintf("`%s`.`%s`", bq.Namespace, name) }

	bqUsersView := bqTable(warehouseutils.UsersView)
	viewExists, _ := bq.tableExists(warehouseutils.UsersView)
	if !viewExists {
		pkgLogger.Infof("BQ: Creating view: %s in bigquery dataset: %s in project: %s", warehouseutils.UsersView, bq.Namespace, bq.ProjectID)
		bq.createTableView(warehouseutils.UsersTable, userColMap)
	}

	bqIdentifiesTable := bqTable(warehouseutils.IdentifiesTable)
	partition := fmt.Sprintf("TIMESTAMP('%s')", partitionDate)
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
		pkgLogger.Infof(`BQ: Loading data into users table: %v`, sqlStatement)
		partitionedUsersTable := partitionedTable(warehouseutils.UsersTable, partitionDate)
		query := bq.Db.Query(sqlStatement)
		query.QueryConfig.Dst = bq.Db.Dataset(bq.Namespace).Table(partitionedUsersTable)
		query.WriteDisposition = bigquery.WriteAppend

		job, err := query.Run(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("BQ: Error initiating load job: %v\n", err)
			errorMap[warehouseutils.UsersTable] = err
			return
		}
		status, err := job.Wait(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("BQ: Error running load job: %v\n", err)
			errorMap[warehouseutils.UsersTable] = errors.New(fmt.Sprintf(`append: %v`, err.Error()))
			return
		}

		if status.Err() != nil {
			errorMap[warehouseutils.UsersTable] = status.Err()
			return
		}
	}

	loadUserTableByMerge := func() {
		stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""), warehouseutils.UsersTable), 127)
		pkgLogger.Infof(`BQ: Creating staging table for users: %v`, sqlStatement)
		query := bq.Db.Query(sqlStatement)
		query.QueryConfig.Dst = bq.Db.Dataset(bq.Namespace).Table(stagingTableName)
		query.WriteDisposition = bigquery.WriteAppend
		job, err := query.Run(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("BQ: Error initiating staging table for users : %v\n", err)
			errorMap[warehouseutils.UsersTable] = err
			return
		}

		status, err := job.Wait(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("BQ: Error initiating staging table for users %v\n", err)
			errorMap[warehouseutils.UsersTable] = errors.New(fmt.Sprintf(`merge: %v`, err.Error()))
			return
		}

		if status.Err() != nil {
			errorMap[warehouseutils.UsersTable] = status.Err()
			return
		}
		defer bq.dropStagingTable(stagingTableName)

		primaryKey := `ID`
		columnNames := append([]string{`ID`}, userColNames...)
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
										INSERT (%[3]s) VALUES (%[6]s)`, bqTable(warehouseutils.UsersTable), bqTable(stagingTableName), columnNamesStr, primaryKey, columnsWithValues, stagingColumnValues)
		pkgLogger.Infof("BQ: Dedup records for table:%s using staging table: %s\n", warehouseutils.UsersTable, sqlStatement)

		pkgLogger.Infof(`BQ: Loading data into users table: %v`, sqlStatement)
		// partitionedUsersTable := partitionedTable(warehouseutils.UsersTable, partitionDate)
		q := bq.Db.Query(sqlStatement)
		job, err = q.Run(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("BQ: Error initiating merge load job: %v\n", err)
			errorMap[warehouseutils.UsersTable] = err
			return
		}
		status, err = job.Wait(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("BQ: Error running merge load job: %v\n", err)
			errorMap[warehouseutils.UsersTable] = errors.New(fmt.Sprintf(`merge: %v`, err.Error()))
			return
		}

		if status.Err() != nil {
			errorMap[warehouseutils.UsersTable] = status.Err()
			return
		}
	}

	if !isUsersTableDedupEnabled {
		loadUserTableByAppend()
		return
	}

	loadUserTableByMerge()
	return errorMap
}

type BQCredentialsT struct {
	projectID   string
	credentials string
}

func (bq *HandleT) connect(cred BQCredentialsT) (*bigquery.Client, error) {
	pkgLogger.Infof("BQ: Connecting to BigQuery in project: %s", cred.projectID)
	bq.BQContext = context.Background()
	client, err := bigquery.NewClient(bq.BQContext, cred.projectID, option.WithCredentialsJSON([]byte(cred.credentials)))
	return client, err
}

func loadConfig() {
	partitionExpiryUpdated = make(map[string]bool)
	stagingTablePrefix = "RUDDER_STAGING_"
	config.RegisterBoolConfigVariable(true, &setUsersLoadPartitionFirstEventFilter, true, "Warehouse.bigquery.setUsersLoadPartitionFirstEventFilter")
	config.RegisterBoolConfigVariable(false, &isUsersTableDedupEnabled, true, "Warehouse.bigquery.isUsersTableDedupEnabled")

}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse").Child("bigquery")
}

func (bq *HandleT) removePartitionExpiry() (err error) {
	partitionExpiryUpdatedLock.Lock()
	defer partitionExpiryUpdatedLock.Unlock()
	identifier := fmt.Sprintf(`%s::%s`, bq.Warehouse.Source.ID, bq.Warehouse.Destination.ID)
	if _, ok := partitionExpiryUpdated[identifier]; ok {
		return
	}
	for tName := range bq.Uploader.GetSchemaInWarehouse() {
		var m *bigquery.TableMetadata
		m, err = bq.Db.Dataset(bq.Namespace).Table(tName).Metadata(bq.BQContext)
		if err != nil {
			return
		}
		if m.TimePartitioning != nil && m.TimePartitioning.Expiration > 0 {
			_, err = bq.Db.Dataset(bq.Namespace).Table(tName).Update(bq.BQContext, bigquery.TableMetadataToUpdate{TimePartitioning: &bigquery.TimePartitioning{Expiration: time.Duration(0)}}, "")
			if err != nil {
				return
			}
		}
	}
	partitionExpiryUpdated[identifier] = true
	return
}

func (bq *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	if !isUsersTableDedupEnabled {
		return
	}
	bq.Warehouse = warehouse
	bq.Namespace = warehouse.Namespace
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))
	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
	})
	if err != nil {
		return
	}
	defer bq.Db.Close()
	bq.dropDanglingStagingTables()
	return
}
func (bq *HandleT) dropDanglingStagingTables() bool {
	sqlStatement := fmt.Sprintf(`SELECT table_name
								 FROM %[1]s.INFORMATION_SCHEMA.TABLES
								 WHERE table_schema = '%[1]s' AND table_name LIKE '%[2]s'`, bq.Namespace, fmt.Sprintf("%s%s", stagingTablePrefix, "%"))
	query := bq.Db.Query(sqlStatement)
	it, err := query.Read(bq.BQContext)
	if err != nil {
		pkgLogger.Errorf("WH: BQ: Error dropping dangling staging tables in BQ: %v\nQuery: %s\n", err, sqlStatement)
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
			pkgLogger.Errorf("BQ: Error in processing fetched staging tables from information schema in dataset %v : %v", bq.Namespace, err)
			return false
		}
		if _, ok := values[0].(string); ok {
			stagingTableNames = append(stagingTableNames, values[0].(string))
		}
	}
	pkgLogger.Infof("WH: PG: Dropping dangling staging tables: %+v  %+v\n", len(stagingTableNames), stagingTableNames)
	delSuccess := true
	for _, stagingTableName := range stagingTableNames {
		tableRef := bq.Db.Dataset(bq.Namespace).Table(stagingTableName)
		err := tableRef.Delete(bq.BQContext)
		if err != nil {
			pkgLogger.Errorf("WH: BQ:  Error dropping dangling staging table: %s in BQ: %v", stagingTableName, err)
			delSuccess = false
		}
	}
	return delSuccess
}

func (bq *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	empty = true
	bq.Warehouse = warehouse
	bq.Namespace = warehouse.Namespace
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))
	pkgLogger.Infof("BQ: Connecting to BigQuery in project: %s", bq.ProjectID)
	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
	})
	if err != nil {
		return
	}
	defer bq.Db.Close()

	tables := []string{"tracks", "pages", "screens", "identifies", "aliases"}
	for _, tableName := range tables {
		var exists bool
		exists, err = bq.tableExists(tableName)
		if err != nil {
			return
		}
		if !exists {
			continue
		}
		count, err := bq.GetTotalCountInTable(tableName)
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

func (bq *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	bq.Warehouse = warehouse
	bq.Namespace = warehouse.Namespace
	bq.Uploader = uploader
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))

	pkgLogger.Infof("BQ: Connecting to BigQuery in project: %s", bq.ProjectID)
	bq.BQContext = context.Background()
	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
	})
	return err
}

func (bq *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	bq.Warehouse = warehouse
	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
	})
	if err != nil {
		return
	}
	defer bq.Db.Close()
	return
}

func (bq *HandleT) LoadTable(tableName string) error {
	var getLoadFileLocFromTableUploads bool
	if misc.ContainsString([]string{warehouseutils.IdentityMappingsTable, warehouseutils.IdentityMergeRulesTable}, tableName) {
		getLoadFileLocFromTableUploads = true
	}
	_, err := bq.loadTable(tableName, false, getLoadFileLocFromTableUploads)
	return err
}

func (bq *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	err = bq.addColumn(tableName, columnName, columnType)
	if err != nil {
		if checkAndIgnoreAlreadyExistError(err) {
			pkgLogger.Infof("BQ: Column %s already exists on %s.%s \nResponse: %v", columnName, bq.Namespace, tableName, err)
			err = nil
		}
	}
	return err
}

func (bq *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

// FetchSchema queries bigquery and returns the schema assoiciated with provided namespace
func (bq *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	bq.Warehouse = warehouse
	bq.Namespace = warehouse.Namespace
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))
	dbClient, err := bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
		// location:    warehouseutils.GetConfigValue(GCPLocation, bq.Warehouse),
	})
	if err != nil {
		return
	}
	defer dbClient.Close()

	schema = make(warehouseutils.SchemaT)
	query := dbClient.Query(fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type
							 FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c
							 ON (t.table_name = c.table_name) and (t.table_type != 'VIEW') and (c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL)`, bq.Namespace))

	it, err := query.Read(bq.BQContext)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			// if dataset resource is not found, return empty schema
			if e.Code == 404 {
				pkgLogger.Infof("BQ: No rows, while fetching schema from  destination:%v, query: %v", bq.Warehouse.Identifier, query)
				return schema, nil
			}
			pkgLogger.Errorf("BQ: Error in fetching schema from bigquery destination:%v, query: %v", bq.Warehouse.Destination.ID, query)
			return schema, e
		}
		pkgLogger.Errorf("BQ: Error in fetching schema from bigquery destination:%v, query: %v", bq.Warehouse.Destination.ID, query)
		return
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			pkgLogger.Errorf("BQ: Error in processing fetched schema from redshift destination:%v, error: %v", bq.Warehouse.Destination.ID, err)
			return nil, err
		}
		var tName, cName, cType string
		tName, _ = values[0].(string)
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		cName, _ = values[1].(string)
		cType, _ = values[2].(string)
		if datatype, ok := dataTypesMapToRudder[bigquery.FieldType(cType)]; ok {
			// lower case all column names from bigquery
			schema[tName][strings.ToLower(cName)] = datatype
		}
	}

	return
}

func (bq *HandleT) Cleanup() {
	if bq.Db != nil {
		bq.Db.Close()
	}
}

func (bq *HandleT) LoadIdentityMergeRulesTable() (err error) {
	identityMergeRulesTable := warehouseutils.IdentityMergeRulesWarehouseTableName(PROVIDER)
	return bq.LoadTable(identityMergeRulesTable)
}

func (bq *HandleT) LoadIdentityMappingsTable() (err error) {
	identityMappingsTable := warehouseutils.IdentityMappingsWarehouseTableName(PROVIDER)
	return bq.LoadTable(identityMappingsTable)
}

func (bq *HandleT) tableExists(tableName string) (exists bool, err error) {
	_, err = bq.Db.Dataset(bq.Namespace).Table(tableName).Metadata(context.Background())
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

func (bq *HandleT) columnExists(columnName string, tableName string) (exists bool, err error) {
	tableMetadata, err := bq.Db.Dataset(bq.Namespace).Table(tableName).Metadata(context.Background())
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

type identityRulesT struct {
	MergeProperty1Type  string `json:"merge_property_1_type"`
	MergeProperty1Value string `json:"merge_property_1_value"`
	MergeProperty2Type  string `json:"merge_property_2_type"`
	MergeProperty2Value string `json:"merge_property_2_value"`
}

func (bq *HandleT) DownloadIdentityRules(gzWriter *misc.GZipWriter) (err error) {
	getFromTable := func(tableName string) (err error) {
		var exists bool
		exists, err = bq.tableExists(tableName)
		if err != nil || !exists {
			return
		}

		tableMetadata, err := bq.Db.Dataset(bq.Namespace).Table(tableName).Metadata(context.Background())
		if err != nil {
			return err
		}
		totalRows := int64(tableMetadata.NumRows)
		// check if table in warehouse has anonymous_id and user_id and construct accordingly
		hasAnonymousID, err := bq.columnExists("anonymous_id", tableName)
		if err != nil {
			return
		}
		hasUserID, err := bq.columnExists("user_id", tableName)
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
			pkgLogger.Infof("BQ: anonymous_id, user_id columns not present in table: %s", tableName)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			sqlStatement := fmt.Sprintf(`SELECT DISTINCT %[1]s FROM %[2]s.%[3]s LIMIT %[4]d OFFSET %[5]d`, toSelectFields, bq.Namespace, tableName, batchSize, offset)
			pkgLogger.Infof("BQ: Downloading distinct combinations of anonymous_id, user_id: %s, totalRows: %d", sqlStatement, totalRows)
			ctx := context.Background()
			query := bq.Db.Query(sqlStatement)
			job, err := query.Run(ctx)
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
				identityRule := identityRulesT{
					MergeProperty1Type:  "anonymous_id",
					MergeProperty1Value: anonId,
					MergeProperty2Type:  "user_id",
					MergeProperty2Value: userId,
				}
				if len(identityRule.MergeProperty1Value) == 0 && len(identityRule.MergeProperty2Value) == 0 {
					continue
				}
				bytes, err := json.Marshal(identityRule)
				if err != nil {
					break
				}
				gzWriter.WriteGZ(string(bytes) + "\n")
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

func (bq *HandleT) GetTotalCountInTable(tableName string) (total int64, err error) {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %[1]s.%[2]s`, bq.Namespace, tableName)
	it, err := bq.Db.Query(sqlStatement).Read(bq.BQContext)
	if err != nil {
		return 0, err
	}
	var values []bigquery.Value
	err = it.Next(&values)
	if err == iterator.Done {
		return 0, nil
	}
	if err != nil {
		pkgLogger.Errorf("BQ: Error in processing totalRowsCount: %v", err)
		return
	}
	total, _ = values[0].(int64)
	return
}

func (bq *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	bq.Warehouse = warehouse
	bq.Namespace = warehouse.Namespace
	bq.ProjectID = strings.TrimSpace(warehouseutils.GetConfigValue(GCPProjectID, bq.Warehouse))
	dbClient, err := bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: warehouseutils.GetConfigValue(GCPCredentials, bq.Warehouse),
	})
	if err != nil {
		return client.Client{}, err
	}

	return client.Client{Type: client.BQClient, BQ: dbClient}, err
}

func (*HandleT) RefreshPartitions(_ string, _ []warehouseutils.LoadFileT) (err error) {
	return nil
}

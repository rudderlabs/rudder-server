package snowflake

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	snowflake "github.com/snowflakedb/gosnowflake" //blank comment
)

var (
	warehouseUploadsTable string
	stagingTablePrefix    string
)

type HandleT struct {
	Db            *sql.DB
	Namespace     string
	CloudProvider string
	ObjectStorage string
	Stage         string
	Warehouse     warehouseutils.WarehouseT
	Uploader      warehouseutils.UploaderI
}

// String constants for snowflake destination config
const (
	AWSAccessKey       = "accessKey"
	AWSAccessSecret    = "accessKeyID"
	StorageIntegration = "storageIntegration"
	SFAccount          = "account"
	SFWarehouse        = "warehouse"
	SFDbName           = "database"
	SFUserName         = "user"
	SFPassword         = "password"
)

const PROVIDER = "SNOWFLAKE"

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "number",
	"bigint":   "number",
	"float":    "double precision",
	"string":   "varchar",
	"datetime": "timestamp",
}

var dataTypesMapToRudder = map[string]string{
	"NUMBER":           "int",
	"DECIMAL":          "int",
	"NUMERIC":          "int",
	"INT":              "int",
	"INTEGER":          "int",
	"BIGINT":           "int",
	"SMALLINT":         "int",
	"FLOAT":            "float",
	"FLOAT4":           "float",
	"FLOAT8":           "float",
	"DOUBLE":           "float",
	"REAL":             "float",
	"DOUBLE PRECISION": "float",
	"BOOLEAN":          "boolean",
	"TEXT":             "string",
	"VARCHAR":          "string",
	"CHAR":             "string",
	"CHARACTER":        "string",
	"STRING":           "string",
	"BINARY":           "string",
	"VARBINARY":        "string",
	"TIMESTAMP_NTZ":    "datetime",
	"DATE":             "datetime",
	"DATETIME":         "datetime",
	"TIME":             "datetime",
	"TIMESTAMP":        "datetime",
	"TIMESTAMP_LTZ":    "datetime",
	"TIMESTAMP_TZ":     "datetime",
}

var primaryKeyMap = map[string]string{
	usersTable:      "ID",
	identifiesTable: "ID",
	discardsTable:   "ROW_ID",
}

var partitionKeyMap = map[string]string{
	usersTable:      "ID",
	identifiesTable: "ID",
	discardsTable:   "ROW_ID, COLUMN_NAME, TABLE_NAME",
}

var (
	usersTable              = strings.ToUpper(warehouseutils.UsersTable)
	identifiesTable         = strings.ToUpper(warehouseutils.IdentifiesTable)
	discardsTable           = strings.ToUpper(warehouseutils.DiscardsTable)
	identityMergeRulesTable = strings.ToUpper(warehouseutils.IdentityMergeRulesTable)
	identityMappingsTable   = strings.ToUpper(warehouseutils.IdentityMappingsTable)
	aliasTable              = strings.ToUpper(warehouseutils.AliasTable)
)

type tableLoadRespT struct {
	dbHandle     *sql.DB
	stagingTable string
}

func columnsWithDataTypes(columns map[string]string, prefix string) string {
	arr := []string{}
	for name, dataType := range columns {
		arr = append(arr, fmt.Sprintf(`%s%s %s`, prefix, name, dataTypesMap[dataType]))
	}
	return strings.Join(arr[:], ",")
}

func (sf *HandleT) createTable(name string, columns map[string]string) (err error) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ( %v )`, name, columnsWithDataTypes(columns, ""))
	logger.Infof("Creating table in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) tableExists(tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.tables
   								 WHERE  table_schema = '%s'
   								 AND    table_name = '%s'
								   )`, sf.Namespace, tableName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) columnExists(columnName string, tableName string) (exists bool, err error) {
	sqlStatement := fmt.Sprintf(`SELECT EXISTS ( SELECT 1
   								 FROM   information_schema.columns
   								 WHERE  table_schema = '%s'
									AND table_name = '%s'
									AND column_name = '%s'
								   )`, sf.Namespace, tableName, columnName)
	err = sf.Db.QueryRow(sqlStatement).Scan(&exists)
	return
}

func (sf *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	sqlStatement := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, tableName, columnName, dataTypesMap[columnType])
	logger.Infof("Adding column in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func (sf *HandleT) createSchema() (err error) {
	sqlStatement := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, sf.Namespace)
	logger.Infof("Creating schemaname in snowflake for SF:%s : %v", sf.Warehouse.Destination.ID, sqlStatement)
	_, err = sf.Db.Exec(sqlStatement)
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		// TODO: throw error if column already exists but of different type
		if e, ok := err.(*snowflake.SnowflakeError); ok {
			if e.SQLState == "42601" {
				return true
			}
		}
		return false
	}
	return true
}

func (sf *HandleT) authString() string {
	var auth string
	if sf.CloudProvider == "AWS" && warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse) == "" {
		auth = fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s')`, warehouseutils.GetConfigValue(AWSAccessSecret, sf.Warehouse), warehouseutils.GetConfigValue(AWSAccessKey, sf.Warehouse))
	} else {
		auth = fmt.Sprintf(`STORAGE_INTEGRATION = %s`, warehouseutils.GetConfigValue(StorageIntegration, sf.Warehouse))
	}
	return auth
}

func (sf *HandleT) loadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, dbHandle *sql.DB, skipClosingDBSession bool) (tableLoadResp tableLoadRespT, err error) {
	logger.Infof("SF: Starting load for table:%s\n", tableName)

	if dbHandle == nil {
		dbHandle, err = connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
		if err != nil {
			logger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", tableName, err)
			return
		}
	}
	tableLoadResp.dbHandle = dbHandle
	if !skipClosingDBSession {
		defer dbHandle.Close()
	}

	// sort column names
	keys := reflect.ValueOf(tableSchemaInUpload).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	sort.Strings(strkeys)
	var sortedColumnNames string
	for index, key := range strkeys {
		if index > 0 {
			sortedColumnNames += fmt.Sprintf(`, `)
		}
		sortedColumnNames += fmt.Sprintf(`%s`, key)
	}

	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), tableName), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %s LIKE %s`, stagingTableName, tableName)

	logger.Infof("SF: Creating temporary table for table:%s at %s\n", tableName, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error creating temporary table for table:%s: %v\n", tableName, err)
		return
	}
	tableLoadResp.stagingTable = stagingTableName

	csvObjectLocation, err := sf.Uploader.GetSampleLoadFileLocation(tableName)
	if err != nil {
		panic(err)
	}
	loadFolder := warehouseutils.GetObjectFolder(sf.ObjectStorage, csvObjectLocation)

	sqlStatement = fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`%s.%s`, sf.Namespace, stagingTableName), sortedColumnNames, loadFolder, sf.authString())

	sanitisedSQLStmt, regexErr := misc.ReplaceMultiRegex(sqlStatement, map[string]string{
		"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
		"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
	})
	if regexErr == nil {
		logger.Infof("SF: Running COPY command for table:%s at %s\n", tableName, sanitisedSQLStmt)
	}

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running COPY command: %v\n", err)
		return
	}

	primaryKey := "ID"
	if column, ok := primaryKeyMap[tableName]; ok {
		primaryKey = column
	}

	partitionKey := "ID"
	if column, ok := partitionKeyMap[tableName]; ok {
		partitionKey = column
	}

	var columnNames, stagingColumnNames, columnsWithValues string
	for idx, str := range strkeys {
		columnNames += fmt.Sprintf(`%s`, str)
		stagingColumnNames += fmt.Sprintf(`staging.%s`, str)
		columnsWithValues += fmt.Sprintf(`original.%[1]s = staging.%[1]s`, str)
		if idx != len(strkeys)-1 {
			columnNames += fmt.Sprintf(`,`)
			stagingColumnNames += fmt.Sprintf(`,`)
			columnsWithValues += fmt.Sprintf(`,`)
		}
	}

	var additionalJoinClause string
	if tableName == discardsTable {
		additionalJoinClause = fmt.Sprintf(`AND original.%[1]s = staging.%[1]s`, "TABLE_NAME")
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY %[8]s ORDER BY RECEIVED_AT ASC) AS _rudder_staging_row_number FROM %[2]s
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original.%[3]s = staging.%[3]s %[7]s)
									WHEN MATCHED THEN
									UPDATE SET %[6]s
									WHEN NOT MATCHED THEN
									INSERT (%[4]s) VALUES (%[5]s)`, tableName, stagingTableName, primaryKey, columnNames, stagingColumnNames, columnsWithValues, additionalJoinClause, partitionKey)
	logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", tableName, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}

	logger.Infof("SF: Complete load for table:%s\n", tableName)
	return
}

func (sf *HandleT) loadMergeRulesTable() (err error) {
	if sf.isTableExported(identityMergeRulesTable) {
		logger.Infof("SF: Skipping load for table:%s as it has been succesfully loaded earlier", identityMergeRulesTable)
		return
	}

	logger.Infof("SF: Starting load for table:%s\n", identityMergeRulesTable)

	logger.Infof("SF: Fetching load file location for %s", identityMergeRulesTable)
	var location string
	location, err = sf.Uploader.GetSingleLoadFileLocation(identityMergeRulesTable)
	if err != nil {
		return err
	}

	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		logger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMergeRulesTable, err)
		return
	}

	sortedColumnNames := strings.Join([]string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE", "MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"}, ",")
	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, location)
	sqlStatement := fmt.Sprintf(`COPY INTO %v(%v) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`%s.%s`, sf.Namespace, identityMergeRulesTable), sortedColumnNames, loadLocation, sf.authString())

	// TODO: sanitise log statement
	logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMergeRulesTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	logger.Infof("SF: Complete load for table:%s\n", identityMergeRulesTable)
	return
}

func (sf *HandleT) loadMappingsTable() (err error) {
	if sf.isTableExported(identityMappingsTable) {
		logger.Infof("SF: Skipping load for table:%s as it has been succesfully loaded earlier", identityMappingsTable)
		return
	}

	logger.Infof("SF: Starting load for table:%s\n", identityMappingsTable)
	logger.Infof("SF: Fetching load file location for %s", identityMappingsTable)
	var location string

	location, err = sf.Uploader.GetSingleLoadFileLocation(identityMappingsTable)
	if err != nil {
		return err
	}

	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{schemaName: sf.Namespace}))
	if err != nil {
		logger.Errorf("SF: Error establishing connection for copying table:%s: %v\n", identityMappingsTable, err)
		return
	}

	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), identityMappingsTable), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %s LIKE %s`, stagingTableName, identityMappingsTable)

	logger.Infof("SF: Creating temporary table for table:%s at %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error creating temporary table for table:%s: %v\n", identityMappingsTable, err)
		return
	}

	loadLocation := warehouseutils.GetObjectLocation(sf.ObjectStorage, location)
	sqlStatement = fmt.Sprintf(`COPY INTO %v(MERGE_PROPERTY_TYPE, MERGE_PROPERTY_VALUE, RUDDER_ID, UPDATED_AT) FROM '%v' %s PATTERN = '.*\.csv\.gz'
		FILE_FORMAT = ( TYPE = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE )`, fmt.Sprintf(`%s.%s`, sf.Namespace, stagingTableName), loadLocation, sf.authString())

	logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}

	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s AS original
									USING (
										SELECT * FROM (
											SELECT *, row_number() OVER (PARTITION BY MERGE_PROPERTY_TYPE, MERGE_PROPERTY_VALUE ORDER BY UPDATED_AT DESC) AS _rudder_staging_row_number FROM %[2]s
										) AS q WHERE _rudder_staging_row_number = 1
									) AS staging
									ON (original.MERGE_PROPERTY_TYPE = staging.MERGE_PROPERTY_TYPE AND original.MERGE_PROPERTY_VALUE = staging.MERGE_PROPERTY_VALUE)
									WHEN MATCHED THEN
									UPDATE SET original.RUDDER_ID = staging.RUDDER_ID, original.UPDATED_AT =  staging.UPDATED_AT
									WHEN NOT MATCHED THEN
									INSERT (MERGE_PROPERTY_TYPE, MERGE_PROPERTY_VALUE, RUDDER_ID, UPDATED_AT) VALUES (staging.MERGE_PROPERTY_TYPE, staging.MERGE_PROPERTY_VALUE, staging.RUDDER_ID, staging.UPDATED_AT)`, identityMappingsTable, stagingTableName)
	logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", identityMappingsTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	logger.Infof("SF: Complete load for table:%s\n", identityMappingsTable)
	return
}

func (sf *HandleT) isTableExported(tableName string) bool {
	status, _ := sf.Uploader.GetTableUploadStatus(tableName)
	// TODO: remove hard coded value
	return status == "exported_data"
}

func (sf *HandleT) loadUserTables() (errorMap map[string]error) {
	if len(sf.Uploader.GetTableSchemaInUpload(identifiesTable)) == 0 {
		return
	}
	errorMap = map[string]error{identifiesTable: nil}
	logger.Infof("SF: Starting load for identifies and users tables\n")

	resp, err := sf.loadTable(identifiesTable, sf.Uploader.GetTableSchemaInUpload(identifiesTable), nil, true)
	if err != nil {
		errorMap[identifiesTable] = err
		return
	}
	defer resp.dbHandle.Close()

	if len(sf.Uploader.GetTableSchemaInUpload(usersTable)) == 0 {
		return
	}
	errorMap[usersTable] = nil

	userColMap := sf.Uploader.GetTableSchemaAfterUpload(usersTable)
	var userColNames, firstValProps []string
	for colName := range userColMap {
		if colName == "ID" {
			continue
		}
		userColNames = append(userColNames, colName)
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), usersTable), 127)
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[2]s AS (SELECT DISTINCT * FROM
										(
											SELECT
											ID, %[3]s
											FROM (
												(
													SELECT ID, %[6]s FROM "%[1]s"."%[4]s" WHERE ID in (SELECT USER_ID FROM %[5]s)
												) UNION
												(
													SELECT USER_ID, %[6]s FROM %[5]s
												)
											)
										)
									)`,
		sf.Namespace,
		stagingTableName,
		strings.Join(firstValProps, ","),
		usersTable,
		resp.stagingTable,
		strings.Join(userColNames, ","),
	)
	logger.Infof("SF: Creating staging table for users: %s\n", sqlStatement)
	_, err = resp.dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error creating temporary table for table:%s: %v\n", usersTable, err)
		return
	}

	primaryKey := "ID"
	columnNames := append([]string{"ID"}, userColNames...)
	columnNamesStr := strings.Join(columnNames, ",")
	columnsWithValuesArr := []string{}
	stagingColumnValuesArr := []string{}
	for _, colName := range columnNames {
		columnsWithValuesArr = append(columnsWithValuesArr, fmt.Sprintf(`original.%[1]s = staging.%[1]s`, colName))
		stagingColumnValuesArr = append(stagingColumnValuesArr, fmt.Sprintf(`staging.%s`, colName))
	}
	columnsWithValues := strings.Join(columnsWithValuesArr, ",")
	stagingColumnValues := strings.Join(stagingColumnValuesArr, ",")

	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s AS original
									USING (
										SELECT %[3]s FROM %[2]s
									) AS staging
									ON (original.%[4]s = staging.%[4]s)
									WHEN MATCHED THEN
									UPDATE SET %[5]s
									WHEN NOT MATCHED THEN
									INSERT (%[3]s) VALUES (%[6]s)`, usersTable, stagingTableName, columnNamesStr, primaryKey, columnsWithValues, stagingColumnValues)
	logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", usersTable, sqlStatement)
	_, err = resp.dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		return
	}
	return
}

func (sf *HandleT) LoadIdentityTables() (errorMap map[string]error) {
	logger.Infof("SF: Starting load for identity tables\n")
	errorMap = make(map[string]error)
	var err error

	haveToUploadIdentifies := len(sf.Uploader.GetTableSchemaInUpload(identifiesTable)) > 0
	haveToUploadAliases := len(sf.Uploader.GetTableSchemaInUpload(aliasTable)) > 0
	haveToUploadMergeRules := len(sf.Uploader.GetTableSchemaInUpload(identityMergeRulesTable)) > 0
	hasUserRecordsToUpload := len(sf.Uploader.GetTableSchemaInUpload(usersTable)) > 0
	haveToUploadUsers := (haveToUploadIdentifies || haveToUploadAliases) && hasUserRecordsToUpload

	// if haveToUploadUsers and users is exported, return
	if haveToUploadUsers && sf.isTableExported(usersTable) {
		logger.Infof("SF: Skipping load for identity tables as they have been succesfully loaded earlier")
		return
	}

	var identifiesLoadResp tableLoadRespT
	if haveToUploadIdentifies {
		errorMap[identifiesTable] = nil
		identifiesLoadResp, err = sf.loadTable(identifiesTable, sf.Uploader.GetTableSchemaInUpload(identifiesTable), nil, true)
		if err != nil {
			errorMap[identifiesTable] = err
			return
		}
		if identifiesLoadResp.dbHandle != nil {
			defer identifiesLoadResp.dbHandle.Close()
		}
	}

	var aliasLoadResp tableLoadRespT
	if haveToUploadAliases {
		errorMap[aliasTable] = nil
		aliasLoadResp, err = sf.loadTable(aliasTable, sf.Uploader.GetTableSchemaInUpload(aliasTable), identifiesLoadResp.dbHandle, true)
		if err != nil {
			errorMap[aliasTable] = err
			return
		}
		if aliasLoadResp.dbHandle != nil {
			defer aliasLoadResp.dbHandle.Close()
		}
	}

	if haveToUploadMergeRules {
		var generated bool
		if generated, err = sf.Uploader.AreIdentityTablesLoadFilesGenerated(); !generated {
			err = sf.Uploader.ResolveIdentities(false)
			if err != nil {
				logger.Errorf(`SF: ID Resolution operation failed: %v`, err)
				return
			}
		}

		if !sf.isTableExported(identityMergeRulesTable) {
			errorMap[identityMergeRulesTable] = nil
			err = sf.loadMergeRulesTable()
			if err != nil {
				errorMap[identityMergeRulesTable] = err
				return
			}
		}

		if !sf.isTableExported(identityMappingsTable) {
			errorMap[identityMappingsTable] = nil
			err = sf.loadMappingsTable()
			if err != nil {
				errorMap[identityMappingsTable] = nil
				return
			}
		}
	}

	if !haveToUploadUsers {
		return
	}
	logger.Infof("SF: Starting load for table:%s\n", usersTable)
	errorMap[usersTable] = nil

	var dbHandle *sql.DB
	if haveToUploadIdentifies {
		dbHandle = identifiesLoadResp.dbHandle
	} else {
		dbHandle = aliasLoadResp.dbHandle
	}

	var rudderIDsListTable string
	// get all rudder_id's in staging tables of identifies & aliases
	// by joining staging table with rudder_identity_mapping table (which is upto data after loadMappingsTable())
	if haveToUploadIdentifies && haveToUploadAliases {
		rudderIDsListTable = fmt.Sprintf(`WITH RUDDER_IDS_LIST_TABLE AS (
				SELECT * FROM (
					(
						SELECT m.RUDDER_ID AS RUDDER_ID FROM %[1]s i_st JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'anonymous_id' AND i_st.ANONYMOUS_ID = m.MERGE_PROPERTY_VALUE
					) UNION
					(
						SELECT m.RUDDER_ID AS RUDDER_ID FROM %[2]s a_st JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'user_id' AND (a_st.USER_ID = m.MERGE_PROPERTY_VALUE OR a_st.PREVIOUS_ID = m.MERGE_PROPERTY_VALUE)
					)
				)
			)`, identifiesLoadResp.stagingTable, aliasLoadResp.stagingTable)
	} else if haveToUploadIdentifies {
		rudderIDsListTable = fmt.Sprintf(`WITH RUDDER_IDS_LIST_TABLE AS (
				SELECT m.RUDDER_ID AS RUDDER_ID FROM %[1]s i_st JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'anonymous_id' AND i_st.ANONYMOUS_ID = m.MERGE_PROPERTY_VALUE
			)`, identifiesLoadResp.stagingTable)
	} else if haveToUploadAliases {
		rudderIDsListTable = fmt.Sprintf(`WITH RUDDER_IDS_LIST_TABLE AS (
				SELECT m.RUDDER_ID AS RUDDER_ID FROM %[1]s a_st JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'user_id' AND (a_st.USER_ID = m.MERGE_PROPERTY_VALUE OR a_st.PREVIOUS_ID = m.MERGE_PROPERTY_VALUE)
			)`, aliasLoadResp.stagingTable)
	}

	userColMap := sf.Uploader.GetTableSchemaAfterUpload(usersTable)
	var userColNamesAliased, firstValProps, userColNames []string
	for colName := range userColMap {
		if colName == "ID" {
			continue
		}
		userColNames = append(userColNames, colName)
		userColNamesAliased = append(userColNamesAliased, fmt.Sprintf(`i.%[1]s AS %[1]s`, colName))
		firstValProps = append(firstValProps, fmt.Sprintf(`FIRST_VALUE(%[1]s IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS %[1]s`, colName))
	}
	stagingTableName := misc.TruncateStr(fmt.Sprintf(`%s%s_%s`, stagingTablePrefix, strings.Replace(uuid.NewV4().String(), "-", "", -1), usersTable), 127)
	// Example query
	// 1. Make IDENTIFIES_WITH_RUDDER_IDS table by filtering identifies tables with rudder_id's from RUDDER_IDS_LIST_TABLE
	// 2. From IDENTIFIES_WITH_RUDDER_IDS get all first_value props for each user
	// TODO: Can scanning entire identifies be avoided? Since users might be linked later, scanned entire identifies table with the rudder_ids in identifies staging
	/*
		CREATE TEMPORARY TABLE RUDDER_STAGING_c3fda15371d34ca98a7878afc920ee4c_USERS AS
			WITH RUDDER_IDS_LIST_TABLE AS (
				SELECT m.RUDDER_ID AS RUDDER_ID FROM RUDDER_STAGING_833aaa75be904d05a2a7b0bf64de5282_IDENTIFIES i_st JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'anonymous_id' AND i_st.ANONYMOUS_ID = m.MERGE_PROPERTY_VALUE
			),
			IDENTIFIES_WITH_RUDDER_IDS AS (
				SELECT USER_ID AS ID, i.CONTEXT_LIBRARY_NAME AS CONTEXT_LIBRARY_NAME,i.PROP_1 AS PROP_1,i.PROP_2 AS PROP_2,i.UUID_TS AS UUID_TS,i.CONTEXT_IP AS CONTEXT_IP,i.RECEIVED_AT AS RECEIVED_AT, m.RUDDER_ID AS RUDDER_ID FROM (SELECT * FROM IDENTIFIES WHERE ANONYMOUS_ID IN (SELECT DISTINCT(MERGE_PROPERTY_VALUE) FROM RUDDER_IDENTITY_MAPPINGS WHERE MERGE_PROPERTY_TYPE ='anonymous_id' AND RUDDER_ID IN (SELECT DISTINCT(RUDDER_ID) FROM RUDDER_IDS_LIST_TABLE))) i JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'anonymous_id' AND i.ANONYMOUS_ID = m.MERGE_PROPERTY_VALUE
			)
			(SELECT DISTINCT * FROM
				(
					SELECT
					ID, FIRST_VALUE(CONTEXT_LIBRARY_NAME IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS CONTEXT_LIBRARY_NAME,FIRST_VALUE(PROP_1 IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS PROP_1,FIRST_VALUE(PROP_2 IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS PROP_2,FIRST_VALUE(UUID_TS IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS UUID_TS,FIRST_VALUE(CONTEXT_IP IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS CONTEXT_IP,FIRST_VALUE(RECEIVED_AT IGNORE NULLS) OVER (PARTITION BY RUDDER_ID ORDER BY RECEIVED_AT DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS RECEIVED_AT
					FROM IDENTIFIES_WITH_RUDDER_IDS
				) WHERE ID IS NOT NULL
			)
	*/
	sqlStatement := fmt.Sprintf(`CREATE TEMPORARY TABLE %[1]s AS
									%[5]s, IDENTIFIES_WITH_RUDDER_IDS AS (
										SELECT USER_ID AS ID, %[4]s, m.RUDDER_ID AS RUDDER_ID FROM (SELECT * FROM %[3]s WHERE ANONYMOUS_ID IN (SELECT DISTINCT(MERGE_PROPERTY_VALUE) FROM RUDDER_IDENTITY_MAPPINGS WHERE MERGE_PROPERTY_TYPE ='anonymous_id' AND RUDDER_ID IN (SELECT DISTINCT(RUDDER_ID) FROM RUDDER_IDS_LIST_TABLE))) i JOIN RUDDER_IDENTITY_MAPPINGS m ON m.MERGE_PROPERTY_TYPE = 'anonymous_id' AND i.ANONYMOUS_ID = m.MERGE_PROPERTY_VALUE
									)
									(SELECT DISTINCT * FROM
										(
											SELECT
											ID, %[2]s
											FROM IDENTIFIES_WITH_RUDDER_IDS
										) WHERE ID IS NOT NULL
									)`,
		stagingTableName,
		strings.Join(firstValProps, ","),
		identifiesTable,
		strings.Join(userColNamesAliased, ","),
		rudderIDsListTable,
	)
	logger.Infof("SF: Creating staging table for users: %s\n", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error creating temporary table for table:%s: %v\n", usersTable, err)
		errorMap[usersTable] = err
		return
	}

	primaryKey := "ID"
	columnNames := append([]string{"ID"}, userColNames...)
	columnNamesStr := strings.Join(columnNames, ",")
	columnsWithValuesArr := []string{}
	stagingColumnValuesArr := []string{}
	for _, colName := range columnNames {
		columnsWithValuesArr = append(columnsWithValuesArr, fmt.Sprintf(`original.%[1]s = staging.%[1]s`, colName))
		stagingColumnValuesArr = append(stagingColumnValuesArr, fmt.Sprintf(`staging.%s`, colName))
	}
	columnsWithValues := strings.Join(columnsWithValuesArr, ",")
	stagingColumnValues := strings.Join(stagingColumnValuesArr, ",")

	sqlStatement = fmt.Sprintf(`MERGE INTO %[1]s AS original
									USING (
										SELECT %[3]s FROM %[2]s
									) AS staging
									ON (original.%[4]s = staging.%[4]s)
									WHEN MATCHED THEN
									UPDATE SET %[5]s
									WHEN NOT MATCHED THEN
									INSERT (%[3]s) VALUES (%[6]s)`, usersTable, stagingTableName, columnNamesStr, primaryKey, columnsWithValues, stagingColumnValues)
	logger.Infof("SF: Dedup records for table:%s using staging table: %s\n", usersTable, sqlStatement)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		logger.Errorf("SF: Error running MERGE for dedup: %v\n", err)
		errorMap[usersTable] = err
		return
	}
	logger.Infof("SF: Complete load for table:%s\n", usersTable)
	return
}

// func (sf *HandleT) load() (errList []error) {
// 	logger.Infof("SF: Starting load for all %v tables\n", len(sf.Upload.Schema))
// 	identityTables := []string{}
// 	var err error
// 	if warehouseutils.IDResolutionEnabled() {
// 		identityTables = []string{identifiesTable, usersTable, aliasTable, identityMergeRulesTable, identityMappingsTable}
// 		err = sf.loadIdentityTables()
// 	} else {
// 		identityTables = []string{identifiesTable, usersTable, identityMergeRulesTable, identityMappingsTable}
// 		err = sf.loadUserTables()
// 	}
// 	if err != nil {
// 		errList = append(errList, err)
// 	}
// 	var wg sync.WaitGroup
// 	wg.Add(len(sf.Upload.Schema))
// 	for tableName, columnMap := range sf.Upload.Schema {
// 		if misc.ContainsString(identityTables, tableName) {
// 			wg.Done()
// 			continue
// 		}
// 		tName := tableName
// 		cMap := columnMap
// 		loadChan <- struct{}{}
// 		rruntime.Go(func() {
// 			_, loadError := sf.loadTable(tName, cMap, nil, false, false)
// 			if loadError != nil {
// 				errList = append(errList, loadError)
// 			}
// 			wg.Done()
// 			<-loadChan
// 		})
// 	}
// 	wg.Wait()
// 	logger.Infof("SF: Completed load for all tables\n")
// 	return
// }

type SnowflakeCredentialsT struct {
	account    string
	whName     string
	dbName     string
	username   string
	password   string
	schemaName string
}

func connect(cred SnowflakeCredentialsT) (*sql.DB, error) {
	url := fmt.Sprintf("%s:%s@%s/%s?warehouse=%s",
		cred.username,
		cred.password,
		cred.account,
		cred.dbName,
		cred.whName)

	if cred.schemaName != "" {
		url += fmt.Sprintf("&schema=%s", cred.schemaName)
	}

	var err error
	var db *sql.DB
	if db, err = sql.Open("snowflake", url); err != nil {
		return nil, fmt.Errorf("SF: snowflake connect error : (%v)", err)
	}

	alterStatement := fmt.Sprintf(`ALTER SESSION SET ABORT_DETACHED_QUERY=TRUE`)
	logger.Infof("SF: Altering session with abort_detached_query for snowflake account:%s, database:%s %v", cred.account, cred.dbName, alterStatement)
	_, err = db.Exec(alterStatement)
	if err != nil {
		return nil, fmt.Errorf("SF: snowflake alter session error : (%v)", err)
	}
	return db, nil
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	stagingTablePrefix = "RUDDER_STAGING_"
}

func init() {
	loadConfig()
}

func (sf *HandleT) MigrateSchema(diff warehouseutils.SchemaDiffT) (err error) {
	if len(sf.Uploader.GetSchemaInWarehouse()) == 0 {
		err = sf.createSchema()
		if err != nil {
			return
		}
	}

	sqlStatement := fmt.Sprintf(`USE SCHEMA "%s"`, sf.Namespace)
	_, err = sf.Db.Exec(sqlStatement)
	if err != nil {
		return
	}

	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		var tableExists bool
		tableExists, err = sf.tableExists(tableName)
		if err != nil {
			return
		}
		if !tableExists {
			err = sf.createTable(fmt.Sprintf(`%s`, tableName), diff.ColumnMaps[tableName])
			if err != nil {
				return
			}
			processedTables[tableName] = true
		}
	}
	for tableName, columnMap := range diff.ColumnMaps {
		// skip adding columns when table didn't exist previously and was created in the prev statement
		// this to make sure all columns in the the columnMap exists in the table in snowflake
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			for columnName, columnType := range columnMap {
				err = sf.addColumn(tableName, columnName, columnType)
				if err != nil {
					if checkAndIgnoreAlreadyExistError(err) {
						logger.Infof("SF: Column %s already exists on %s.%s \nResponse: %v", columnName, sf.Namespace, tableName, err)
					} else {
						return
					}
				}
			}
		}
	}
	return
}

// DownloadIdentityRules gets distinct combinations of anonymous_id, user_id from tables in warehouse
func (sf *HandleT) DownloadIdentityRules(gzWriter *misc.GZipWriter) (err error) {

	getFromTable := func(tableName string) (err error) {
		var exists bool
		exists, err = sf.tableExists(tableName)
		if err != nil || !exists {
			return
		}

		sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s.%s`, sf.Namespace, tableName)
		var totalRows int64
		err = sf.Db.QueryRow(sqlStatement).Scan(&totalRows)
		if err != nil {
			return
		}

		// check if table in warehouse has anonymous_id and user_id and construct accordingly
		hasAnonymousID, err := sf.columnExists("ANONYMOUS_ID", tableName)
		if err != nil {
			return
		}
		hasUserID, err := sf.columnExists("USER_ID", tableName)
		if err != nil {
			return
		}

		var toSelectFields string
		if hasAnonymousID && hasUserID {
			toSelectFields = "ANONYMOUS_ID, USER_ID"
		} else if hasAnonymousID {
			toSelectFields = "ANONYMOUS_ID, NULL AS USER_ID"
		} else if hasUserID {
			toSelectFields = "NULL AS ANONYMOUS_ID, USER_ID"
		} else {
			logger.Infof("SF: ANONYMOUS_ID, USER_ID columns not present in table: %s", tableName)
			return nil
		}

		batchSize := int64(10000)
		var offset int64
		for {
			// TODO: Handle case for missing anonymous_id, user_id columns
			sqlStatement = fmt.Sprintf(`SELECT DISTINCT %s FROM %s.%s LIMIT %d OFFSET %d`, toSelectFields, sf.Namespace, tableName, batchSize, offset)
			logger.Infof("SF: Downloading distinct combinations of anonymous_id, user_id: %s", sqlStatement)
			var rows *sql.Rows
			rows, err = sf.Db.Query(sqlStatement)
			if err != nil {
				return
			}

			for rows.Next() {
				var buff bytes.Buffer
				csvWriter := csv.NewWriter(&buff)
				var csvRow []string

				var anonymousID, userID sql.NullString
				err = rows.Scan(&anonymousID, &userID)
				if err != nil {
					return
				}
				csvRow = append(csvRow, "anonymous_id", anonymousID.String, "user_id", userID.String)
				csvWriter.Write(csvRow)
				csvWriter.Flush()
				gzWriter.WriteGZ(buff.String())
			}

			offset += batchSize
			if offset >= totalRows {
				break
			}
		}
		return
	}

	tables := []string{"TRACKS", "PAGES", "SCREENS", "IDENTIFIES", "ALIASES"}
	for _, table := range tables {
		err = getFromTable(table)
		if err != nil {
			return
		}
	}
	return nil
}

func (sf *HandleT) PreLoadIdentityTables() (err error) {
	// idr := identity.HandleT{
	// 	Warehouse:        sf.Warehouse,
	// 	DbHandle:         sf.DbHandle,
	// 	Upload:           sf.Upload,
	// 	WarehouseManager: sf,
	// }

	// check if identity table uploads have location field
	// var generated bool
	// if generated, err = sf.areIdentityTablesLoadFilesGenerated(); !generated {
	// 	err = idr.Resolve(true)
	// 	if err != nil {
	// 		logger.Errorf(`SF: ID Resolution operation failed: %v`, err)
	// 		return
	// 	}
	// }

	var generated bool
	if generated, err = sf.Uploader.AreIdentityTablesLoadFilesGenerated(); !generated {
		err = sf.Uploader.ResolveIdentities(true)
		if err != nil {
			logger.Errorf(`SF: ID Resolution operation failed: %v`, err)
			return
		}
	}

	if !sf.isTableExported(identityMergeRulesTable) {
		err = sf.loadMergeRulesTable()
		if err != nil {
			return
		}
	}

	if !sf.isTableExported(identityMappingsTable) {
		err = sf.loadMappingsTable()
		if err != nil {
			return
		}
	}
	return
}

func (sf *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return
}

func (sf *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (empty bool, err error) {
	empty = true

	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.Db.Close()

	tables := []string{"TRACKS", "PAGES", "SCREENS", "IDENTIFIES", "ALIASES"}
	for _, tableName := range tables {
		var exists bool
		exists, err = sf.tableExists(tableName)
		if err != nil {
			return
		}
		if exists {
			sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s`, sf.Namespace, tableName)
			var count int64
			err = sf.Db.QueryRow(sqlStatement).Scan(&count)
			if err != nil {
				return
			}
			if count > 0 {
				empty = false
				return
			}
		} else {
			continue
		}
	}
	return
}

type OptionalCredsT struct {
	schemaName string
}

func (sf *HandleT) getConnectionCredentials(opts OptionalCredsT) SnowflakeCredentialsT {
	return SnowflakeCredentialsT{
		account:    warehouseutils.GetConfigValue(SFAccount, sf.Warehouse),
		whName:     warehouseutils.GetConfigValue(SFWarehouse, sf.Warehouse),
		dbName:     warehouseutils.GetConfigValue(SFDbName, sf.Warehouse),
		username:   warehouseutils.GetConfigValue(SFUserName, sf.Warehouse),
		password:   warehouseutils.GetConfigValue(SFPassword, sf.Warehouse),
		schemaName: opts.schemaName,
	}
}

// func (sf *HandleT) Process(config warehouseutils.ConfigT) (err error) {
// 	sf.DbHandle = config.DbHandle
// 	sf.Warehouse = config.Warehouse
// 	sf.Upload = config.Upload
// 	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(config.Warehouse.Destination.Config)
// 	sf.ObjectStorage = warehouseutils.ObjectStorageType("SNOWFLAKE", config.Warehouse.Destination.Config)

// 	currSchema, err := warehouseutils.GetCurrentSchema(sf.DbHandle, sf.Warehouse)
// 	if err != nil {
// 		panic(err)
// 	}
// 	sf.CurrentSchema = currSchema
// 	sf.Namespace = strings.ToUpper(sf.Upload.Namespace)

// 	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
// 	if err != nil {
// 		warehouseutils.SetUploadError(sf.Upload, err, warehouseutils.UpdatingSchemaFailedState, sf.DbHandle)
// 		return err
// 	}
// 	defer sf.Db.Close()

// 	err = sf.MigrateSchema()
// 	if err != nil {
// 		return
// 	}

// 	if warehouseutils.IDResolutionEnabled() && config.Stage == warehouseutils.PreLoadingIdentities {
// 		err = sf.PreLoadIdentityTables()
// 		if err != nil {
// 			uploadError := warehouseutils.SetUploadError(sf.Upload, err, warehouseutils.PreLoadIdentitiesFailed, sf.DbHandle)
// 			if uploadError != nil {
// 				panic(uploadError)
// 			}
// 		}
// 	} else {
// 		err = sf.Export()
// 	}
// 	return err
// }

func (sf *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	sf.CloudProvider = warehouseutils.SnowflakeCloudProvider(warehouse.Destination.Config)
	sf.ObjectStorage = warehouseutils.ObjectStorageType("SNOWFLAKE", warehouse.Destination.Config)
	sf.Uploader = uploader

	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
	return err
}

func (sf *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) (err error) {
	sf.Warehouse = warehouse
	sf.Db, err = connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer sf.Db.Close()
	pingResultChannel := make(chan error, 1)
	rruntime.Go(func() {
		pingResultChannel <- sf.Db.Ping()
	})
	select {
	case err = <-pingResultChannel:
	case <-time.After(5 * time.Second):
		err = errors.New("connection testing timed out")
	}
	return
}

// FetchSchema queries snowflake and returns the schema assoiciated with provided namespace
func (sf *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (schema warehouseutils.SchemaT, err error) {
	sf.Warehouse = warehouse
	sf.Namespace = warehouse.Namespace
	dbHandle, err := connect(sf.getConnectionCredentials(OptionalCredsT{}))
	if err != nil {
		return
	}
	defer dbHandle.Close()

	schema = make(warehouseutils.SchemaT)
	sqlStatement := fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type
									FROM INFORMATION_SCHEMA.TABLES as t
									JOIN INFORMATION_SCHEMA.COLUMNS as c
									ON t.table_schema = c.table_schema and t.table_name = c.table_name
									WHERE t.table_schema = '%s'`, sf.Namespace)

	rows, err := dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		logger.Errorf("SF: Error in fetching schema from snowflake destination:%v, query: %v", sf.Warehouse.Destination.ID, sqlStatement)
		return
	}
	if err == sql.ErrNoRows {
		return schema, nil
	}
	defer rows.Close()
	for rows.Next() {
		var tName, cName, cType string
		err = rows.Scan(&tName, &cName, &cType)
		if err != nil {
			logger.Errorf("SF: Error in processing fetched schema from snowflake destination:%v", sf.Warehouse.Destination.ID)
			return
		}
		if _, ok := schema[tName]; !ok {
			schema[tName] = make(map[string]string)
		}
		if datatype, ok := dataTypesMapToRudder[cType]; ok {
			schema[tName][cName] = datatype
		}
	}
	return
}

func (sf *HandleT) Cleanup() {
	if sf.Db != nil {
		sf.Db.Close()
	}
}

func (sf *HandleT) LoadUserTables() map[string]error {
	return sf.loadUserTables()
}

func (sf *HandleT) LoadTable(tableName string) error {
	_, err := sf.loadTable(tableName, sf.Uploader.GetTableSchemaInUpload(tableName), nil, false)
	return err
}

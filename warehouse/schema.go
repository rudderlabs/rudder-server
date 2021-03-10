package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type SchemaHandleT struct {
	dbHandle          *sql.DB
	stagingFiles      []*StagingFileT
	warehouse         warehouseutils.WarehouseT
	localSchema       warehouseutils.SchemaT
	schemaInWarehouse warehouseutils.SchemaT
	uploadSchema      warehouseutils.SchemaT
}

func handleSchemaChange(existingDataType string, columnType string, columnVal interface{}) (newColumnVal interface{}, ok bool) {
	if existingDataType == "string" || existingDataType == "text" {
		newColumnVal = fmt.Sprintf("%v", columnVal)
	} else if (columnType == "int" || columnType == "bigint") && existingDataType == "float" {
		newColumnVal = columnVal
	} else if columnType == "float" && (existingDataType == "int" || existingDataType == "bigint") {
		floatVal, ok := columnVal.(float64)
		if !ok {
			newColumnVal = nil
		} else {
			newColumnVal = int(floatVal)
		}
	} else {
		return nil, false
	}

	return newColumnVal, true
}

func (jobRun *JobRunT) handleDiscardTypes(tableName string, columnName string, columnVal interface{}, columnData DataT, gzWriter misc.GZipWriter) error {
	job := jobRun.job
	rowID, hasID := columnData[job.getColumnName("id")]
	receivedAt, hasReceivedAt := columnData[job.getColumnName("received_at")]
	if hasID && hasReceivedAt {
		eventLoader := warehouseutils.GetNewEventLoader(job.DestinationType)
		eventLoader.AddColumn("column_name", columnName)
		eventLoader.AddColumn("column_value", fmt.Sprintf("%v", columnVal))
		eventLoader.AddColumn("received_at", receivedAt)
		eventLoader.AddColumn("row_id", rowID)
		eventLoader.AddColumn("table_name", tableName)
		if eventLoader.IsLoadTimeColumn("uuid_ts") {
			timestampFormat := eventLoader.GetLoadTimeFomat("uuid_ts")
			eventLoader.AddColumn("uuid_ts", jobRun.uuidTS.Format(timestampFormat))
		}
		if eventLoader.IsLoadTimeColumn("loaded_at") {
			timestampFormat := eventLoader.GetLoadTimeFomat("loaded_at")
			eventLoader.AddColumn("loaded_at", jobRun.uuidTS.Format(timestampFormat))
		}

		eventData, err := eventLoader.WriteToString()
		if err != nil {
			return err
		}
		gzWriter.WriteGZ(eventData)
	}
	return nil
}

func (sHandle *SchemaHandleT) getLocalSchema() (currentSchema warehouseutils.SchemaT) {
	destID := sHandle.warehouse.Destination.ID
	namespace := sHandle.warehouse.Namespace

	var rawSchema json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT schema FROM %[1]s WHERE (%[1]s.destination_id='%[2]s' AND %[1]s.namespace='%[3]s') ORDER BY %[1]s.id DESC`, warehouseutils.WarehouseSchemasTable, destID, namespace)
	pkgLogger.Infof("[WH]: Fetching current schema from wh postgresql: %s", sqlStatement)

	err := dbHandle.QueryRow(sqlStatement).Scan(&rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			pkgLogger.Infof("[WH]: No current schema found for %s with namespace: %s", destID, namespace)
			return
		}
		if err != nil {
			panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
		}
	}
	var schemaMapInterface map[string]interface{}
	err = json.Unmarshal(rawSchema, &schemaMapInterface)
	if err != nil {
		panic(fmt.Errorf("Unmarshalling: %s failed with Error : %w", rawSchema, err))
	}
	currentSchema = warehouseutils.SchemaT{}
	for tname, columnMapInterface := range schemaMapInterface {
		columnMap := make(map[string]string)
		columns := columnMapInterface.(map[string]interface{})
		for cName, cTypeInterface := range columns {
			columnMap[cName] = cTypeInterface.(string)
		}
		currentSchema[tname] = columnMap
	}
	return currentSchema
}

func (sHandle *SchemaHandleT) updateLocalSchema(updatedSchema warehouseutils.SchemaT) error {
	namespace := sHandle.warehouse.Namespace
	sourceID := sHandle.warehouse.Source.ID
	destID := sHandle.warehouse.Destination.ID
	destType := sHandle.warehouse.Type
	marshalledSchema, err := json.Marshal(updatedSchema)
	if err != nil {
		return err
	}

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, schema, created_at, updated_at)
								VALUES ($1, $2, $3, $4, $5, $6, $7)
								ON CONFLICT (source_id, destination_id, namespace)
								DO
								UPDATE SET schema=$5, updated_at = $7 RETURNING id
								`, warehouseutils.WarehouseSchemasTable)
	updatedAt := timeutil.Now()
	_, err = dbHandle.Exec(sqlStatement, sourceID, namespace, destID, destType, marshalledSchema, timeutil.Now(), updatedAt)
	return err
}

func (sHandle *SchemaHandleT) fetchSchemaFromWarehouse() (schemaInWarehouse warehouseutils.SchemaT, err error) {
	whManager, err := manager.New(sHandle.warehouse.Type)
	if err != nil {
		panic(err)
	}

	schemaInWarehouse, err = whManager.FetchSchema(sHandle.warehouse)
	if err != nil {
		pkgLogger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
		return warehouseutils.SchemaT{}, err
	}
	return schemaInWarehouse, nil
}

func mergeSchema(currentSchema warehouseutils.SchemaT, schemaList []warehouseutils.SchemaT, currentMergedSchema warehouseutils.SchemaT, warehouseType string) warehouseutils.SchemaT {
	if len(currentMergedSchema) == 0 {
		currentMergedSchema = warehouseutils.SchemaT{}
	}

	setColumnTypeFromExistingSchema := func(refSchema warehouseutils.SchemaT, tableName, refTableName, columnName, refColumnName, columnType string) bool {
		columnTypeInDB, ok := refSchema[refTableName][refColumnName]
		if !ok {
			return false
		}
		if columnTypeInDB == "string" && columnType == "text" {
			currentMergedSchema[tableName][columnName] = columnType
			return true
		}
		currentMergedSchema[tableName][columnName] = columnTypeInDB
		return true
	}

	usersTableName := warehouseutils.ToProviderCase(warehouseType, "users")
	identifiesTableName := warehouseutils.ToProviderCase(warehouseType, "identifies")

	for _, schema := range schemaList {
		for tableName, columnMap := range schema {
			if currentMergedSchema[tableName] == nil {
				currentMergedSchema[tableName] = make(map[string]string)
			}
			var toInferFromIdentifies bool
			var refSchema warehouseutils.SchemaT
			if tableName == usersTableName {
				if _, ok := currentSchema[identifiesTableName]; ok {
					toInferFromIdentifies = true
					refSchema = currentSchema
				} else if _, ok := currentMergedSchema[identifiesTableName]; ok { // also check in identifies of currentMergedSchema if identifies table not present in warehouse
					toInferFromIdentifies = true
					refSchema = currentMergedSchema
				}
			}
			for columnName, columnType := range columnMap {
				// if column already has a type in db, use that
				// check for data type in identifies for users table before check in users table
				// to ensure same data type is set for the same column in both users and identifies
				if tableName == usersTableName && toInferFromIdentifies {
					refColumnName := columnName
					if columnName == warehouseutils.ToProviderCase(warehouseType, "id") {
						refColumnName = warehouseutils.ToProviderCase(warehouseType, "user_id")
					}
					if setColumnTypeFromExistingSchema(refSchema, tableName, identifiesTableName, columnName, refColumnName, columnType) {
						continue
					}
				}

				if _, ok := currentSchema[tableName]; ok {
					if setColumnTypeFromExistingSchema(currentSchema, tableName, tableName, columnName, columnName, columnType) {
						continue
					}
				}
				// check if we already set the columnType in currentMergedSchema
				if _, ok := currentMergedSchema[tableName][columnName]; !ok {
					currentMergedSchema[tableName][columnName] = columnType
				}
			}
		}
	}
	return currentMergedSchema
}

func (sHandle *SchemaHandleT) safeName(columnName string) string {
	return warehouseutils.ToProviderCase(sHandle.warehouse.Type, columnName)
}

func (sh *SchemaHandleT) getDiscardsSchema() map[string]string {
	discards := map[string]string{
		sh.safeName("table_name"):   "string",
		sh.safeName("row_id"):       "string",
		sh.safeName("column_name"):  "string",
		sh.safeName("column_value"): "string",
		sh.safeName("received_at"):  "datetime",
		sh.safeName("uuid_ts"):      "datetime",
	}
	// add loaded_at for bq to be segment compatible
	if sh.warehouse.Type == "BQ" {
		discards[sh.safeName("loaded_at")] = "datetime"
	}
	return discards
}

func (sh *SchemaHandleT) getMergeRulesSchema() map[string]string {
	return map[string]string{
		sh.safeName("merge_property_1_type"):  "string",
		sh.safeName("merge_property_1_value"): "string",
		sh.safeName("merge_property_2_type"):  "string",
		sh.safeName("merge_property_2_value"): "string",
	}
}

func (sh *SchemaHandleT) getIdentitiesMappingsSchema() map[string]string {
	return map[string]string{
		sh.safeName("merge_property_type"):  "string",
		sh.safeName("merge_property_value"): "string",
		sh.safeName("rudder_id"):            "string",
		sh.safeName("updated_at"):           "datetime",
	}
}

func (sh *SchemaHandleT) isIDResolutionEnabled() bool {
	return warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, sh.warehouse.Type)
}

func (sh *SchemaHandleT) consolidateStagingFilesSchemaUsingWarehouseSchema() warehouseutils.SchemaT {
	schemaInLocalDB := sh.localSchema

	consolidatedSchema := warehouseutils.SchemaT{}
	count := 0
	for {
		lastIndex := count + stagingFilesSchemaPaginationSize
		if lastIndex >= len(sh.stagingFiles) {
			lastIndex = len(sh.stagingFiles)
		}

		var ids []int64
		for _, stagingFile := range sh.stagingFiles[count:lastIndex] {
			ids = append(ids, stagingFile.ID)
		}

		sqlStatement := fmt.Sprintf(`SELECT schema FROM %s WHERE id IN (%s)`, warehouseutils.WarehouseStagingFilesTable, misc.IntArrayToString(ids, ","))
		rows, err := sh.dbHandle.Query(sqlStatement)
		if err != nil && err != sql.ErrNoRows {
			panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
		}

		var schemas []warehouseutils.SchemaT
		for rows.Next() {
			var s json.RawMessage
			err := rows.Scan(&s)
			if err != nil {
				panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
			}
			var schema warehouseutils.SchemaT
			err = json.Unmarshal(s, &schema)
			if err != nil {
				panic(fmt.Errorf("Unmarshalling: %s failed with Error : %w", string(s), err))
			}

			schemas = append(schemas, schema)
		}
		rows.Close()

		consolidatedSchema = mergeSchema(schemaInLocalDB, schemas, consolidatedSchema, sh.warehouse.Type)

		count += stagingFilesSchemaPaginationSize
		if count >= len(sh.stagingFiles) {
			break
		}
	}

	// add rudder_discards Schema
	consolidatedSchema[sh.safeName(warehouseutils.DiscardsTable)] = sh.getDiscardsSchema()

	// add rudder_identity_mappings Schema
	if sh.isIDResolutionEnabled() {
		if _, ok := consolidatedSchema[sh.safeName(warehouseutils.IdentityMergeRulesTable)]; ok {
			consolidatedSchema[sh.safeName(warehouseutils.IdentityMergeRulesTable)] = sh.getMergeRulesSchema()
			consolidatedSchema[sh.safeName(warehouseutils.IdentityMappingsTable)] = sh.getIdentitiesMappingsSchema()
		}
	}

	return consolidatedSchema
}

func compareSchema(sch1, sch2 map[string]map[string]string) bool {
	eq := reflect.DeepEqual(sch1, sch2)
	return eq
}

func getTableSchemaDiff(tableName string, currentSchema, uploadSchema warehouseutils.SchemaT) (diff warehouseutils.TableSchemaDiffT) {
	diff = warehouseutils.TableSchemaDiffT{
		ColumnMap:     make(map[string]string),
		UpdatedSchema: make(map[string]string),
	}

	var currentTableSchema map[string]string
	var ok bool
	if currentTableSchema, ok = currentSchema[tableName]; !ok {
		if _, ok := uploadSchema[tableName]; !ok {
			return
		}
		diff.Exists = true
		diff.TableToBeCreated = true
		diff.ColumnMap = uploadSchema[tableName]
		diff.UpdatedSchema = uploadSchema[tableName]
		return diff
	}

	for columnName, columnType := range currentSchema[tableName] {
		diff.UpdatedSchema[columnName] = columnType
	}

	diff.ColumnMap = make(map[string]string)
	for columnName, columnType := range uploadSchema[tableName] {
		if _, ok := currentTableSchema[columnName]; !ok {
			diff.ColumnMap[columnName] = columnType
			diff.UpdatedSchema[columnName] = columnType
			diff.Exists = true
		} else if columnType == "text" && currentTableSchema[columnName] == "string" {
			diff.StringColumnsToBeAlteredToText = append(diff.StringColumnsToBeAlteredToText, columnName)
			diff.UpdatedSchema[columnName] = columnType
			diff.Exists = true
		}
	}
	return diff
}

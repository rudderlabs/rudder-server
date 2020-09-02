package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/warehousemanager"
)

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

func (wh *HandleT) syncSchemaFromWarehouse(job ProcessStagingFilesJobT) (bool, error) {
	whManager, err := warehousemanager.NewWhManager(wh.destType)
	if err != nil {
		panic(err)
	}
	// consolidate schema if not already done
	schemaInDB, err := warehouseutils.GetCurrentSchema(wh.dbHandle, job.Warehouse)

	syncedSchema, err := whManager.FetchSchema(job.Warehouse, job.Warehouse.Namespace)
	if err != nil {
		logger.Errorf(`WH: Failed fetching schema from warehouse: %v`, err)
		return false, err
	}

	hasSchemaChanged := !warehouseutils.CompareSchema(schemaInDB, syncedSchema)
	if hasSchemaChanged {
		err = warehouseutils.UpdateCurrentSchema(job.Warehouse.Namespace, job.Warehouse, job.Upload.ID, syncedSchema, wh.dbHandle)
		if err != nil {
			panic(err)
		}
	}

	return hasSchemaChanged, nil
}

func (wh *HandleT) mergeSchema(currentSchema map[string]map[string]string, schemaList []map[string]map[string]string, currentMergedSchema map[string]map[string]string) map[string]map[string]string {
	if len(currentMergedSchema) == 0 {
		currentMergedSchema = make(map[string]map[string]string)
	}
	for _, schema := range schemaList {
		for tableName, columnMap := range schema {
			if currentMergedSchema[tableName] == nil {
				currentMergedSchema[tableName] = make(map[string]string)
			}
			for columnName, columnType := range columnMap {
				// if column already has a type in db, use that
				if len(currentSchema) > 0 {
					if _, ok := currentSchema[tableName]; ok {
						if columnTypeInDB, ok := currentSchema[tableName][columnName]; ok {
							if columnTypeInDB == "string" && columnType == "text" {
								currentMergedSchema[tableName][columnName] = columnType
								continue
							}
							currentMergedSchema[tableName][columnName] = columnTypeInDB
							continue
						}
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

func (wh *HandleT) consolidateSchema(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) map[string]map[string]string {
	schemaInDB, err := warehouseutils.GetCurrentSchema(wh.dbHandle, warehouse)
	if err != nil {
		panic(err)
	}

	consolidatedSchema := make(map[string]map[string]string)
	count := 0
	for {
		lastIndex := count + stagingFilesSchemaPaginationSize
		if lastIndex >= len(jsonUploadsList) {
			lastIndex = len(jsonUploadsList)
		}

		var ids []int64
		for _, upload := range jsonUploadsList[count:lastIndex] {
			ids = append(ids, upload.ID)
		}

		sqlStatement := fmt.Sprintf(`SELECT schema FROM %s WHERE id IN (%s)`, warehouseutils.WarehouseStagingFilesTable, misc.IntArrayToString(ids, ","))
		rows, err := wh.dbHandle.Query(sqlStatement)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		var schemas []map[string]map[string]string
		for rows.Next() {
			var s json.RawMessage
			err := rows.Scan(&s)
			if err != nil {
				panic(err)
			}
			var schema map[string]map[string]string
			err = json.Unmarshal(s, &schema)
			if err != nil {
				panic(err)
			}

			schemas = append(schemas, schema)
		}
		rows.Close()

		consolidatedSchema = wh.mergeSchema(schemaInDB, schemas, consolidatedSchema)

		count += stagingFilesSchemaPaginationSize
		if count >= len(jsonUploadsList) {
			break
		}
	}

	// add rudder_discards table
	destType := warehouse.Destination.DestinationDefinition.Name
	discards := map[string]string{
		warehouseutils.ToProviderCase(destType, "table_name"):   "string",
		warehouseutils.ToProviderCase(destType, "row_id"):       "string",
		warehouseutils.ToProviderCase(destType, "column_name"):  "string",
		warehouseutils.ToProviderCase(destType, "column_value"): "string",
		warehouseutils.ToProviderCase(destType, "received_at"):  "datetime",
		warehouseutils.ToProviderCase(destType, "uuid_ts"):      "datetime",
	}
	// add loaded_at for bq to be segment compatible
	if destType == "BQ" {
		discards[warehouseutils.ToProviderCase(destType, "loaded_at")] = "datetime"
	}
	consolidatedSchema[warehouseutils.ToProviderCase(destType, warehouseutils.DiscardsTable)] = discards

	// add rudder_identity_mappings table
	if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, wh.destType) {
		identityMappings := map[string]string{
			warehouseutils.ToProviderCase(destType, "merge_property_type"):  "string",
			warehouseutils.ToProviderCase(destType, "merge_property_value"): "string",
			warehouseutils.ToProviderCase(destType, "rudder_id"):            "string",
			warehouseutils.ToProviderCase(destType, "updated_at"):           "datetime",
		}
		consolidatedSchema[warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMappingsTable)] = identityMappings
	}

	return consolidatedSchema
}

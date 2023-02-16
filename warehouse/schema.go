package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type SchemaHandleT struct {
	dbHandle                      *sql.DB
	stagingFiles                  []*model.StagingFile
	warehouse                     warehouseutils.Warehouse
	localSchema                   warehouseutils.SchemaT
	schemaInWarehouse             warehouseutils.SchemaT
	unrecognizedSchemaInWarehouse warehouseutils.SchemaT
	uploadSchema                  warehouseutils.SchemaT
}

func HandleSchemaChange(existingDataType, currentDataType model.SchemaType, value any) (any, error) {
	var (
		newColumnVal any
		err          error
	)

	if existingDataType == model.StringDataType || existingDataType == model.TextDataType {
		// only stringify if the previous type is non-string/text/json
		if currentDataType != model.StringDataType && currentDataType != model.TextDataType && currentDataType != model.JSONDataType {
			newColumnVal = fmt.Sprintf("%v", value)
		} else {
			newColumnVal = value
		}
	} else if (currentDataType == model.IntDataType || currentDataType == model.BigIntDataType) && existingDataType == model.FloatDataType {
		intVal, ok := value.(int)
		if !ok {
			err = ErrIncompatibleSchemaConversion
		} else {
			newColumnVal = float64(intVal)
		}
	} else if currentDataType == model.FloatDataType && (existingDataType == model.IntDataType || existingDataType == model.BigIntDataType) {
		floatVal, ok := value.(float64)
		if !ok {
			err = ErrIncompatibleSchemaConversion
		} else {
			newColumnVal = int(floatVal)
		}
	} else if existingDataType == model.JSONDataType {
		var interfaceSliceSample []any
		if currentDataType == model.IntDataType || currentDataType == model.FloatDataType || currentDataType == model.BooleanDataType {
			newColumnVal = fmt.Sprintf("%v", value)
		} else if reflect.TypeOf(value) == reflect.TypeOf(interfaceSliceSample) {
			newColumnVal = value
		} else {
			newColumnVal = fmt.Sprintf(`"%v"`, value)
		}
	} else {
		err = ErrSchemaConversionNotSupported
	}

	return newColumnVal, err
}

func (sh *SchemaHandleT) getLocalSchema() (currentSchema warehouseutils.SchemaT) {
	sourceID := sh.warehouse.Source.ID
	destID := sh.warehouse.Destination.ID
	namespace := sh.warehouse.Namespace

	var rawSchema json.RawMessage
	sqlStatement := fmt.Sprintf(`
		SELECT
		  schema
		FROM
		  %[1]s ST
		WHERE
		  (
			ST.destination_id = '%[2]s'
			AND ST.namespace = '%[3]s'
			AND ST.source_id = '%[4]s'
		  )
		ORDER BY
		  ST.id DESC;
`,
		warehouseutils.WarehouseSchemasTable,
		destID,
		namespace,
		sourceID,
	)
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
		panic(fmt.Errorf("unmarshalling: %s failed with Error : %w", rawSchema, err))
	}
	currentSchema = warehouseutils.SchemaT{}
	for tableName, columnMapInterface := range schemaMapInterface {
		columnMap := make(map[string]string)
		columns := columnMapInterface.(map[string]interface{})
		for cName, cTypeInterface := range columns {
			columnMap[cName] = cTypeInterface.(string)
		}
		currentSchema[tableName] = columnMap
	}
	return currentSchema
}

func (sh *SchemaHandleT) updateLocalSchema(updatedSchema warehouseutils.SchemaT) error {
	namespace := sh.warehouse.Namespace
	sourceID := sh.warehouse.Source.ID
	destID := sh.warehouse.Destination.ID
	destType := sh.warehouse.Type
	marshalledSchema, err := json.Marshal(updatedSchema)
	defer func() {
		if err != nil {
			pkgLogger.Infof("Failed to update local schema for with error: %s", err.Error())
		}
	}()
	if err != nil {
		return err
	}

	sqlStatement := fmt.Sprintf(`
		INSERT INTO %s (
		  source_id, namespace, destination_id,
		  destination_type, schema, created_at,
		  updated_at
		)
		VALUES
		  ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (
			source_id, destination_id, namespace
		  ) DO
		UPDATE
		SET
		  schema = $5,
		  updated_at = $7 RETURNING id;
`,
		warehouseutils.WarehouseSchemasTable,
	)
	updatedAt := timeutil.Now()
	_, err = dbHandle.Exec(
		sqlStatement,
		sourceID,
		namespace,
		destID,
		destType,
		marshalledSchema,
		timeutil.Now(),
		updatedAt,
	)
	return err
}

func (sh *SchemaHandleT) fetchSchemaFromWarehouse(whManager manager.Manager) (schemaInWarehouse, unrecognizedSchemaInWarehouse warehouseutils.SchemaT, err error) {
	schemaInWarehouse, unrecognizedSchemaInWarehouse, err = whManager.FetchSchema(sh.warehouse)
	if err != nil {
		pkgLogger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
		return warehouseutils.SchemaT{}, warehouseutils.SchemaT{}, err
	}
	return schemaInWarehouse, unrecognizedSchemaInWarehouse, nil
}

func MergeSchema(currentSchema warehouseutils.SchemaT, schemaList []warehouseutils.SchemaT, currentMergedSchema warehouseutils.SchemaT, warehouseType string) warehouseutils.SchemaT {
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
		// if columnTypeInDB is text, then we should not change it to string
		if currentMergedSchema[tableName][columnName] == "text" {
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

func (sh *SchemaHandleT) safeName(columnName string) string {
	return warehouseutils.ToProviderCase(sh.warehouse.Type, columnName)
}

func (sh *SchemaHandleT) getDiscardsSchema() map[string]string {
	discards := map[string]string{}
	for colName, colType := range warehouseutils.DiscardsSchema {
		discards[sh.safeName(colName)] = colType
	}

	// add loaded_at for bq to be segment compatible
	if sh.warehouse.Type == warehouseutils.BQ {
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
	return warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, sh.warehouse.Type)
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

		sqlStatement := fmt.Sprintf(`
			SELECT
			  schema
			FROM
			  %s
			WHERE
			  id IN (%s);
`,
			warehouseutils.WarehouseStagingFilesTable,
			misc.IntArrayToString(ids, ","),
		)
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
				panic(fmt.Errorf("unmarshalling: %s failed with Error : %w", string(s), err))
			}

			schemas = append(schemas, schema)
		}
		_ = rows.Close()

		consolidatedSchema = MergeSchema(schemaInLocalDB, schemas, consolidatedSchema, sh.warehouse.Type)

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

// hasSchemaChanged Default behaviour is to do the deep equals.
// If we are skipping deep equals, then we are validating local schemas against warehouse schemas only.
// Not the other way around.
func hasSchemaChanged(localSchema, schemaInWarehouse warehouseutils.SchemaT) bool {
	if !skipDeepEqualSchemas {
		eq := reflect.DeepEqual(localSchema, schemaInWarehouse)
		return !eq
	}
	// Iterating through all tableName in the localSchema
	for tableName := range localSchema {
		localColumns := localSchema[tableName]
		warehouseColumns, whColumnsExist := schemaInWarehouse[tableName]

		// If warehouse does  not contain the specified table return true.
		if !whColumnsExist {
			return true
		}
		for columnName := range localColumns {
			localColumn := localColumns[columnName]
			warehouseColumn := warehouseColumns[columnName]

			// If warehouse does not contain the specified column return true.
			// If warehouse column does not match with the local one return true
			if localColumn != warehouseColumn {
				return true
			}
		}
	}
	return false
}

func getTableSchemaDiff(tableName string, currentSchema, uploadSchema warehouseutils.SchemaT) (diff warehouseutils.TableSchemaDiffT) {
	diff = warehouseutils.TableSchemaDiffT{
		ColumnMap:        make(map[string]string),
		UpdatedSchema:    make(map[string]string),
		AlteredColumnMap: make(map[string]string),
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
			diff.AlteredColumnMap[columnName] = columnType
			diff.UpdatedSchema[columnName] = columnType
			diff.Exists = true
		}
	}
	return diff
}

// returns the merged schema(uploadSchema+schemaInWarehousePreUpload) for all tables in uploadSchema
func mergeUploadAndLocalSchemas(uploadSchema, schemaInWarehousePreUpload warehouseutils.SchemaT) warehouseutils.SchemaT {
	mergedSchema := warehouseutils.SchemaT{}
	// iterate over all tables in uploadSchema
	for uploadTableName, uploadTableSchema := range uploadSchema {
		if _, ok := mergedSchema[uploadTableName]; !ok {
			// init map if it does not exist
			mergedSchema[uploadTableName] = map[string]string{}
		}

		// uploadSchema becomes the merged schema if the table does not exist in local Schema
		localTableSchema, ok := schemaInWarehousePreUpload[uploadTableName]
		if !ok {
			mergedSchema[uploadTableName] = uploadTableSchema
			continue
		}

		// iterate over all columns in localSchema and add them to merged schema
		for localColName, localColType := range localTableSchema {
			mergedSchema[uploadTableName][localColName] = localColType
		}

		// iterate over all columns in uploadSchema and add them to merged schema if required
		for uploadColName, uploadColType := range uploadTableSchema {
			localColType, ok := localTableSchema[uploadColName]
			// add uploadCol to mergedSchema if the col does not exist in localSchema
			if !ok {
				mergedSchema[uploadTableName][uploadColName] = uploadColType
				continue
			}
			// change type of uploadCol to text if it was string in localSchema
			if uploadColType == "text" && localColType == "string" {
				mergedSchema[uploadTableName][uploadColName] = uploadColType
			}
		}
	}
	return mergedSchema
}

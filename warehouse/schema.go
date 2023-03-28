package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type SchemaHandle struct {
	dbHandle                      *sql.DB
	stagingFiles                  []*model.StagingFile
	warehouse                     model.Warehouse
	localSchema                   model.Schema
	schemaInWarehouse             model.Schema
	unrecognizedSchemaInWarehouse model.Schema
	uploadSchema                  model.Schema
	whSchemaRepo                  *repo.WHSchema
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

func (sh *SchemaHandle) getLocalSchema() (model.Schema, error) {
	whSchema, err := sh.whSchemaRepo.GetForNamespace(
		context.TODO(),
		sh.warehouse.Source.ID,
		sh.warehouse.Destination.ID,
		sh.warehouse.Namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("get schema for namespace: %w", err)
	}
	if whSchema.Schema == nil {
		return model.Schema{}, nil
	}
	return whSchema.Schema, nil
}

func (sh *SchemaHandle) updateLocalSchema(uploadId int64, updatedSchema model.Schema) error {
	_, err := sh.whSchemaRepo.Insert(context.TODO(), &model.WHSchema{
		UploadID:        uploadId,
		SourceID:        sh.warehouse.Source.ID,
		Namespace:       sh.warehouse.Namespace,
		DestinationID:   sh.warehouse.Destination.ID,
		DestinationType: sh.warehouse.Type,
		Schema:          updatedSchema,
	})
	return err
}

func (sh *SchemaHandle) fetchSchemaFromWarehouse(whManager manager.Manager) (schemaInWarehouse, unrecognizedSchemaInWarehouse model.Schema, err error) {
	schemaInWarehouse, unrecognizedSchemaInWarehouse, err = whManager.FetchSchema(sh.warehouse)
	if err != nil {
		pkgLogger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
		return model.Schema{}, model.Schema{}, err
	}

	sh.SkipDeprecatedColumns(schemaInWarehouse)
	sh.SkipDeprecatedColumns(unrecognizedSchemaInWarehouse)
	return schemaInWarehouse, unrecognizedSchemaInWarehouse, nil
}

func (sh *SchemaHandle) SkipDeprecatedColumns(schema model.Schema) {
	for tableName, columnMap := range schema {
		for columnName := range columnMap {
			if warehouseutils.DeprecatedColumnsRegex.MatchString(columnName) {
				pkgLogger.Debugw("skipping deprecated column",
					logfield.SourceID, sh.warehouse.Source.ID,
					logfield.DestinationID, sh.warehouse.Destination.ID,
					logfield.DestinationType, sh.warehouse.Destination.DestinationDefinition.Name,
					logfield.WorkspaceID, sh.warehouse.WorkspaceID,
					logfield.Namespace, sh.warehouse.Namespace,
					logfield.TableName, tableName,
					logfield.ColumnName, columnName,
				)
				delete(schema[tableName], columnName)
				continue
			}
		}
	}
}

func MergeSchema(currentSchema model.Schema, schemaList []model.Schema, currentMergedSchema model.Schema, warehouseType string) model.Schema {
	if len(currentMergedSchema) == 0 {
		currentMergedSchema = model.Schema{}
	}

	setColumnTypeFromExistingSchema := func(refSchema model.Schema, tableName, refTableName, columnName, refColumnName, columnType string) bool {
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
				currentMergedSchema[tableName] = make(model.TableSchema)
			}
			var toInferFromIdentifies bool
			var refSchema model.Schema
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

func (sh *SchemaHandle) safeName(columnName string) string {
	return warehouseutils.ToProviderCase(sh.warehouse.Type, columnName)
}

func (sh *SchemaHandle) getDiscardsSchema() model.TableSchema {
	discards := model.TableSchema{}
	for colName, colType := range warehouseutils.DiscardsSchema {
		discards[sh.safeName(colName)] = colType
	}

	// add loaded_at for bq to be segment compatible
	if sh.warehouse.Type == warehouseutils.BQ {
		discards[sh.safeName("loaded_at")] = "datetime"
	}
	return discards
}

func (sh *SchemaHandle) getMergeRulesSchema() model.TableSchema {
	return model.TableSchema{
		sh.safeName("merge_property_1_type"):  "string",
		sh.safeName("merge_property_1_value"): "string",
		sh.safeName("merge_property_2_type"):  "string",
		sh.safeName("merge_property_2_value"): "string",
	}
}

func (sh *SchemaHandle) getIdentitiesMappingsSchema() model.TableSchema {
	return model.TableSchema{
		sh.safeName("merge_property_type"):  "string",
		sh.safeName("merge_property_value"): "string",
		sh.safeName("rudder_id"):            "string",
		sh.safeName("updated_at"):           "datetime",
	}
}

func (sh *SchemaHandle) isIDResolutionEnabled() bool {
	return warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, sh.warehouse.Type)
}

func (sh *SchemaHandle) consolidateStagingFilesSchemaUsingWarehouseSchema() model.Schema {
	schemaInLocalDB := sh.localSchema

	consolidatedSchema := model.Schema{}
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

		var schemas []model.Schema
		for rows.Next() {
			var s json.RawMessage
			err := rows.Scan(&s)
			if err != nil {
				panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
			}
			var schema model.Schema
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
func hasSchemaChanged(localSchema, schemaInWarehouse model.Schema) bool {
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

func getTableSchemaDiff(tableName string, currentSchema, uploadSchema model.Schema) (diff warehouseutils.TableSchemaDiff) {
	diff = warehouseutils.TableSchemaDiff{
		ColumnMap:        make(model.TableSchema),
		UpdatedSchema:    make(model.TableSchema),
		AlteredColumnMap: make(model.TableSchema),
	}

	var currentTableSchema model.TableSchema
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

	diff.ColumnMap = make(model.TableSchema)
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

package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/errors"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	"reflect"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Handler struct {
	DB                               *sql.DB
	StagingFiles                     []*model.StagingFile
	Warehouse                        warehouseutils.Warehouse
	LocalSchema                      warehouseutils.Schema
	SchemaInWarehouse                warehouseutils.Schema
	UnrecognizedSchemaInWarehouse    warehouseutils.Schema
	UploadSchema                     warehouseutils.Schema
	StagingRepo                      *repo.StagingFiles
	Logger                           logger.Logger
	StagingFilesSchemaPaginationSize int
	StagingFilesBatchSize            int
	SkipDeepEqualSchemas             bool
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
			err = errors.ErrIncompatibleSchemaConversion
		} else {
			newColumnVal = float64(intVal)
		}
	} else if currentDataType == model.FloatDataType && (existingDataType == model.IntDataType || existingDataType == model.BigIntDataType) {
		floatVal, ok := value.(float64)
		if !ok {
			err = errors.ErrIncompatibleSchemaConversion
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
		err = errors.ErrSchemaConversionNotSupported
	}

	return newColumnVal, err
}

func (sh *Handler) GetLocalSchema() (currentSchema warehouseutils.Schema) {
	sourceID := sh.Warehouse.Source.ID
	destID := sh.Warehouse.Destination.ID
	namespace := sh.Warehouse.Namespace

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
	sh.Logger.Infof("[WH]: Fetching current schema from wh postgresql: %s", sqlStatement)

	err := sh.DB.QueryRow(sqlStatement).Scan(&rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			sh.Logger.Infof("[WH]: No current schema found for %s with namespace: %s", destID, namespace)
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
	currentSchema = warehouseutils.Schema{}
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

func (sh *Handler) UpdateLocalSchema(updatedSchema warehouseutils.Schema) error {
	namespace := sh.Warehouse.Namespace
	sourceID := sh.Warehouse.Source.ID
	destID := sh.Warehouse.Destination.ID
	destType := sh.Warehouse.Type
	marshalledSchema, err := json.Marshal(updatedSchema)
	defer func() {
		if err != nil {
			sh.Logger.Infof("Failed to update local schema for with error: %s", err.Error())
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
	_, err = sh.DB.Exec(
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

func (sh *Handler) FetchSchemaFromWarehouse(whManager manager.Manager) (schemaInWarehouse, unrecognizedSchemaInWarehouse warehouseutils.Schema, err error) {
	schemaInWarehouse, unrecognizedSchemaInWarehouse, err = whManager.FetchSchema(sh.Warehouse)
	if err != nil {
		sh.Logger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
		return warehouseutils.Schema{}, warehouseutils.Schema{}, err
	}

	sh.SkipDeprecatedColumns(schemaInWarehouse)
	sh.SkipDeprecatedColumns(unrecognizedSchemaInWarehouse)
	return schemaInWarehouse, unrecognizedSchemaInWarehouse, nil
}

func (sh *Handler) SkipDeprecatedColumns(schema warehouseutils.Schema) {
	for tableName, columnMap := range schema {
		for columnName := range columnMap {
			if warehouseutils.DeprecatedColumnsRegex.MatchString(columnName) {
				sh.Logger.Debugw("skipping deprecated column",
					logfield.SourceID, sh.Warehouse.Source.ID,
					logfield.DestinationID, sh.Warehouse.Destination.ID,
					logfield.DestinationType, sh.Warehouse.Destination.DestinationDefinition.Name,
					logfield.WorkspaceID, sh.Warehouse.WorkspaceID,
					logfield.Namespace, sh.Warehouse.Namespace,
					logfield.TableName, tableName,
					logfield.ColumnName, columnName,
				)
				delete(schema[tableName], columnName)
				continue
			}
		}
	}
}

func MergeSchema(currentSchema warehouseutils.Schema, schemaList []warehouseutils.Schema, currentMergedSchema warehouseutils.Schema, warehouseType string) warehouseutils.Schema {
	if len(currentMergedSchema) == 0 {
		currentMergedSchema = warehouseutils.Schema{}
	}

	setColumnTypeFromExistingSchema := func(refSchema warehouseutils.Schema, tableName, refTableName, columnName, refColumnName, columnType string) bool {
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
				currentMergedSchema[tableName] = make(warehouseutils.TableSchema)
			}
			var toInferFromIdentifies bool
			var refSchema warehouseutils.Schema
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

func (sh *Handler) SafeName(columnName string) string {
	return warehouseutils.ToProviderCase(sh.Warehouse.Type, columnName)
}

func (sh *Handler) DiscardsSchema() warehouseutils.TableSchema {
	discards := warehouseutils.TableSchema{}
	for colName, colType := range warehouseutils.DiscardsSchema {
		discards[sh.SafeName(colName)] = colType
	}

	// add loaded_at for bq to be segment compatible
	if sh.Warehouse.Type == warehouseutils.BQ {
		discards[sh.SafeName("loaded_at")] = "datetime"
	}
	return discards
}

func (sh *Handler) MergeRulesSchema() warehouseutils.TableSchema {
	return warehouseutils.TableSchema{
		sh.SafeName("merge_property_1_type"):  "string",
		sh.SafeName("merge_property_1_value"): "string",
		sh.SafeName("merge_property_2_type"):  "string",
		sh.SafeName("merge_property_2_value"): "string",
	}
}

func (sh *Handler) IdentitiesMappingsSchema() warehouseutils.TableSchema {
	return warehouseutils.TableSchema{
		sh.SafeName("merge_property_type"):  "string",
		sh.SafeName("merge_property_value"): "string",
		sh.SafeName("rudder_id"):            "string",
		sh.SafeName("updated_at"):           "datetime",
	}
}

func (sh *Handler) isIDResolutionEnabled() bool {
	return warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, sh.Warehouse.Type)
}

// consolidateStagingFiles consolidates the staging files into a single schema
// 1. Gives preference to the text type over any other type
// 2. If a column is present in multiple schemas, it takes the type of the column in the first schema
func (sh *Handler) consolidateStagingFiles() (warehouseutils.Schema, error) {
	stagingFiles := make([]model.StagingFile, len(sh.StagingFiles))
	for i := range sh.StagingFiles {
		stagingFiles[i] = *sh.StagingFiles[i]
	}

	batchIDs := service.StagingFileBatchIDs(stagingFiles, sh.StagingFilesBatchSize)
	consolidatedSchema := warehouseutils.Schema{}

	for _, batch := range batchIDs {
		schemaJsons, err := sh.StagingRepo.GetSchemaByIDs(context.TODO(), batch)
		if err != nil {
			return nil, fmt.Errorf("get schema by ids: %w", err)
		}

		var schemas []warehouseutils.Schema
		for _, schemaJson := range schemaJsons {
			var schema warehouseutils.Schema
			err = json.Unmarshal(schemaJson, &schema)
			if err != nil {
				return nil, fmt.Errorf("unmarshalling schema: %w", err)
			}

			schemas = append(schemas, schema)
		}

		for _, tableSchema := range schemas {
			for table, columnsMap := range tableSchema {
				if _, ok := consolidatedSchema[table]; !ok {
					consolidatedSchema[table] = make(warehouseutils.TableSchema)
				}

				for column, columnType := range columnsMap {
					if _, ok := consolidatedSchema[table][column]; !ok {
						consolidatedSchema[table][column] = columnType
					}

					// if column type is text, we don't want to override it with any other type
					if columnType == "text" {
						consolidatedSchema[table][column] = columnType
					}
				}
			}
		}
	}
	return consolidatedSchema, nil
}

func (sh *Handler) ConsolidateStagingFilesSchemaUsingWarehouseSchema() (warehouseutils.Schema, error) {
	schemaInLocalDB := sh.LocalSchema

	consolidatedSchema := warehouseutils.Schema{}
	count := 0
	for {
		lastIndex := count + sh.StagingFilesSchemaPaginationSize
		if lastIndex >= len(sh.StagingFiles) {
			lastIndex = len(sh.StagingFiles)
		}

		ids := repo.StagingFileIDs(sh.StagingFiles[count:lastIndex])
		schemaJsons, err := sh.StagingRepo.GetSchemaByIDs(context.TODO(), ids)
		if err != nil {
			return nil, fmt.Errorf("get schema by ids: %w", err)
		}

		var schemas []warehouseutils.Schema
		for _, schemaJson := range schemaJsons {
			var schema warehouseutils.Schema
			err = json.Unmarshal(schemaJson, &schema)
			if err != nil {
				return nil, fmt.Errorf("unmarshalling schema: %w", err)
			}

			schemas = append(schemas, schema)
		}

		consolidatedSchema = MergeSchema(schemaInLocalDB, schemas, consolidatedSchema, sh.Warehouse.Type)

		count += sh.StagingFilesSchemaPaginationSize
		if count >= len(sh.StagingFiles) {
			break
		}
	}

	// add rudder_discards Schema
	consolidatedSchema[sh.SafeName(warehouseutils.DiscardsTable)] = sh.DiscardsSchema()

	// add rudder_identity_mappings Schema
	if sh.isIDResolutionEnabled() {
		if _, ok := consolidatedSchema[sh.SafeName(warehouseutils.IdentityMergeRulesTable)]; ok {
			consolidatedSchema[sh.SafeName(warehouseutils.IdentityMergeRulesTable)] = sh.MergeRulesSchema()
			consolidatedSchema[sh.SafeName(warehouseutils.IdentityMappingsTable)] = sh.IdentitiesMappingsSchema()
		}
	}

	return consolidatedSchema, nil
}

// HasSchemaChanged checks if the local schema is different from the schema in warehouse
// 1. Default behaviour is to do the deep equals.
// 2. If we are skipping deep equals, then we are validating local schemas against warehouse schemas only. Not the other way around.
func (sh *Handler) HasSchemaChanged(localSchema, schemaInWarehouse warehouseutils.Schema) bool {
	if !sh.SkipDeepEqualSchemas {
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

func GetTableSchemaDiff(tableName string, currentSchema, uploadSchema warehouseutils.Schema) (diff warehouseutils.TableSchemaDiffT) {
	diff = warehouseutils.TableSchemaDiffT{
		ColumnMap:        make(warehouseutils.TableSchema),
		UpdatedSchema:    make(warehouseutils.TableSchema),
		AlteredColumnMap: make(warehouseutils.TableSchema),
	}

	var currentTableSchema warehouseutils.TableSchema
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

	diff.ColumnMap = make(warehouseutils.TableSchema)
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
func MergeUploadAndLocalSchemas(uploadSchema, schemaInWarehousePreUpload warehouseutils.Schema) warehouseutils.Schema {
	mergedSchema := warehouseutils.Schema{}
	// iterate over all tables in uploadSchema
	for uploadTableName, uploadTableSchema := range uploadSchema {
		if _, ok := mergedSchema[uploadTableName]; !ok {
			// init map if it does not exist
			mergedSchema[uploadTableName] = warehouseutils.TableSchema{}
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

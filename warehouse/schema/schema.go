package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"reflect"

	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	ErrIncompatibleSchemaConversion = errors.New("incompatible schema conversion")
	ErrSchemaConversionNotSupported = errors.New("schema conversion not supported")
)

type Handler struct {
	StagingFiles                  []*model.StagingFile
	Warehouse                     warehouseutils.Warehouse
	LocalSchema                   warehouseutils.Schema
	SchemaInWarehouse             warehouseutils.Schema
	UnrecognizedSchemaInWarehouse warehouseutils.Schema
	UploadSchema                  warehouseutils.Schema
	WhSchemaRepo                  *repo.WHSchema
	StagingRepo                   *repo.StagingFiles
	Logger                        logger.Logger

	StagingFilesSchemaPaginationSize int
	SkipDeepEqualSchemas             bool
	IDResolutionEnabled              bool
}

func NewHandler(
	db *sql.DB,
	warehouse warehouseutils.Warehouse,
	stagingFiles []*model.StagingFile,
	conf *config.Config,
) *Handler {
	return &Handler{
		Warehouse:                        warehouse,
		StagingFiles:                     stagingFiles,
		WhSchemaRepo:                     repo.NewWHSchemas(db),
		StagingRepo:                      repo.NewStagingFiles(db),
		Logger:                           logger.NewLogger().Child("warehouse").Child("schema"),
		StagingFilesSchemaPaginationSize: conf.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100),
		SkipDeepEqualSchemas:             conf.GetBool("Warehouse.skipDeepEqualSchemas", false),
		IDResolutionEnabled:              warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, warehouse.Type),
	}
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

func (sh *Handler) GetLocalSchema() (warehouseutils.Schema, error) {
	whSchema, err := sh.WhSchemaRepo.GetForNamespace(
		context.TODO(),
		sh.Warehouse.Source.ID,
		sh.Warehouse.Destination.ID,
		sh.Warehouse.Namespace,
	)
	if err != nil {
		return warehouseutils.Schema{}, fmt.Errorf("get schema for namespace: %w", err)
	}
	return whSchema.Schema, nil
}

func (sh *Handler) UpdateLocalSchema(uploadId int64, updatedSchema warehouseutils.Schema) error {
	_, err := sh.WhSchemaRepo.Insert(context.TODO(), &model.WHSchema{
		UploadID:        uploadId,
		SourceID:        sh.Warehouse.Source.ID,
		Namespace:       sh.Warehouse.Namespace,
		DestinationID:   sh.Warehouse.Destination.ID,
		DestinationType: sh.Warehouse.Type,
		Schema:          updatedSchema,
	})
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

func (sh *Handler) GetDiscardsSchema() warehouseutils.TableSchema {
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

func (sh *Handler) GetMergeRulesSchema() warehouseutils.TableSchema {
	return warehouseutils.TableSchema{
		sh.SafeName("merge_property_1_type"):  "string",
		sh.SafeName("merge_property_1_value"): "string",
		sh.SafeName("merge_property_2_type"):  "string",
		sh.SafeName("merge_property_2_value"): "string",
	}
}

func (sh *Handler) GetIdentitiesMappingsSchema() warehouseutils.TableSchema {
	return warehouseutils.TableSchema{
		sh.SafeName("merge_property_type"):  "string",
		sh.SafeName("merge_property_value"): "string",
		sh.SafeName("rudder_id"):            "string",
		sh.SafeName("updated_at"):           "datetime",
	}
}

func (sh *Handler) ConsolidateStagingFilesSchemaUsingWarehouseSchema() (warehouseutils.Schema, error) {
	consolidatedSchema := warehouseutils.Schema{}
	count := 0
	for {
		lastIndex := count + sh.StagingFilesSchemaPaginationSize
		if lastIndex >= len(sh.StagingFiles) {
			lastIndex = len(sh.StagingFiles)
		}

		rawSchemas, err := sh.StagingRepo.GetSchemasByIDs(
			context.TODO(),
			repo.StagingFileIDs(sh.StagingFiles[count:lastIndex]),
		)
		if err != nil {
			return warehouseutils.Schema{}, fmt.Errorf("getting staging files schema: %v", err)
		}

		var schemas []warehouseutils.Schema
		for _, rawSchema := range rawSchemas {
			var schema warehouseutils.Schema
			err = json.Unmarshal(rawSchema, &schema)
			if err != nil {
				return warehouseutils.Schema{}, fmt.Errorf("unmarshalling staging files schema: %v", err)
			}
			schemas = append(schemas, schema)
		}

		consolidatedSchema = MergeSchema(sh.LocalSchema, schemas, consolidatedSchema, sh.Warehouse.Type)

		count += sh.StagingFilesSchemaPaginationSize
		if count >= len(sh.StagingFiles) {
			break
		}
	}

	// add rudder_discards Schema
	consolidatedSchema[sh.SafeName(warehouseutils.DiscardsTable)] = sh.GetDiscardsSchema()

	// add rudder_identity_mappings Schema
	if sh.IDResolutionEnabled {
		if _, ok := consolidatedSchema[sh.SafeName(warehouseutils.IdentityMergeRulesTable)]; ok {
			consolidatedSchema[sh.SafeName(warehouseutils.IdentityMergeRulesTable)] = sh.GetMergeRulesSchema()
			consolidatedSchema[sh.SafeName(warehouseutils.IdentityMappingsTable)] = sh.GetIdentitiesMappingsSchema()
		}
	}

	return consolidatedSchema, nil
}

// HasSchemaChanged compares the localSchema with the schemaInWarehouse and returns true if they are not equal
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

// GetTableSchemaDiff returns the diff between the current schema and the upload schema
func GetTableSchemaDiff(tableName string, currentSchema, uploadSchema warehouseutils.Schema) (diff warehouseutils.TableSchemaDiff) {
	diff = warehouseutils.TableSchemaDiff{
		ColumnMap:        make(warehouseutils.TableSchema),
		UpdatedSchema:    make(warehouseutils.TableSchema),
		AlteredColumnMap: make(warehouseutils.TableSchema),
	}

	var (
		currentTableSchema warehouseutils.TableSchema
		ok                 bool
	)

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

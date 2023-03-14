package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	ErrIncompatibleSchemaConversion = errors.New("incompatible schema conversion")
	ErrSchemaConversionNotSupported = errors.New("schema conversion not supported")
)

type Handler struct {
	Warehouse                     model.Warehouse
	LocalSchema                   model.Schema
	SchemaInWarehouse             model.Schema
	UnrecognizedSchemaInWarehouse model.Schema
	UploadSchema                  model.Schema
	WhSchemaRepo                  schemaRepo
	StagingRepo                   stagingFileRepo
	Logger                        logger.Logger

	StagingFilesSchemaPaginationSize int
	SkipDeepEqualSchemas             bool
	IDResolutionEnabled              bool
}

type schemaRepo interface {
	GetForNamespace(ctx context.Context, sourceID, destID, namespace string) (model.WHSchema, error)
	Insert(ctx context.Context, whSchema *model.WHSchema) (int64, error)
}

type stagingFileRepo interface {
	GetSchemasByIDs(ctx context.Context, ids []int64) ([]model.Schema, error)
}

type fetchSchemaRepo interface {
	FetchSchema(warehouse model.Warehouse) (model.Schema, model.Schema, error)
}

func NewHandler(
	db *sql.DB,
	warehouse model.Warehouse,
) *Handler {
	return &Handler{
		Warehouse:           warehouse,
		WhSchemaRepo:        repo.NewWHSchemas(db),
		StagingRepo:         repo.NewStagingFiles(db),
		Logger:              logger.NewLogger().Child("warehouse").Child("schema"),
		IDResolutionEnabled: warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, warehouse.Type),
	}
}

func WithConfig(h *Handler, conf *config.Config) {
	h.StagingFilesSchemaPaginationSize = conf.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100)
	h.SkipDeepEqualSchemas = conf.GetBool("Warehouse.skipDeepEqualSchemas", false)
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

func (sh *Handler) GetLocalSchema() (model.Schema, error) {
	whSchema, err := sh.WhSchemaRepo.GetForNamespace(
		context.TODO(),
		sh.Warehouse.Source.ID,
		sh.Warehouse.Destination.ID,
		sh.Warehouse.Namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("getting schema for namespace: %w", err)
	}
	if whSchema.Schema == nil {
		return model.Schema{}, nil
	}
	return whSchema.Schema, nil
}

func (sh *Handler) UpdateLocalSchema(uploadId int64, updatedSchema model.Schema) error {
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

func (sh *Handler) FetchSchemaFromWarehouse(repo fetchSchemaRepo) (model.Schema, model.Schema, error) {
	schemaInWarehouse, unrecognizedSchemaInWarehouse, err := repo.FetchSchema(sh.Warehouse)
	if err != nil {
		return model.Schema{}, model.Schema{}, fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	sh.skipDeprecatedColumns(schemaInWarehouse)
	sh.skipDeprecatedColumns(unrecognizedSchemaInWarehouse)

	return schemaInWarehouse, unrecognizedSchemaInWarehouse, nil
}

// skipDeprecatedColumns skips deprecated columns from the schema
func (sh *Handler) skipDeprecatedColumns(schema model.Schema) {
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

// ConsolidateStagingFilesSchemaUsingWarehouseSchema consolidates staging files schema with warehouse schema
func (sh *Handler) ConsolidateStagingFilesSchemaUsingWarehouseSchema(stagingFiles []*model.StagingFile) (model.Schema, error) {
	consolidatedSchema := model.Schema{}
	batches := lo.Chunk(stagingFiles, sh.StagingFilesSchemaPaginationSize)
	for _, batch := range batches {
		schemas, err := sh.StagingRepo.GetSchemasByIDs(
			context.TODO(),
			repo.StagingFileIDs(batch),
		)
		if err != nil {
			return model.Schema{}, fmt.Errorf("getting staging files schema: %v", err)
		}

		consolidatedSchema = consolidateStagingSchemas(consolidatedSchema, schemas)
	}
	consolidatedSchema = consolidateWarehouseSchema(consolidatedSchema, sh.LocalSchema)
	consolidatedSchema = overrideUsersWithIdentifiesSchema(consolidatedSchema, sh.Warehouse.Type)
	consolidatedSchema = enhanceDiscardsSchema(consolidatedSchema, sh.Warehouse.Type)
	consolidatedSchema = enhanceSchemaWithIDResolution(consolidatedSchema, sh.IDResolutionEnabled, sh.Warehouse.Type)
	return consolidatedSchema, nil
}

// consolidateStagingSchemas merges multiple schemas into one
// Prefer the type of the first schema, If the type is text, prefer text
func consolidateStagingSchemas(consolidatedSchema model.Schema, schemas []model.Schema) model.Schema {
	for _, schema := range schemas {
		for tableName, columnMap := range schema {
			if _, ok := consolidatedSchema[tableName]; !ok {
				consolidatedSchema[tableName] = model.TableSchema{}
			}
			for columnName, columnType := range columnMap {
				if model.SchemaType(columnType) == model.TextDataType {
					consolidatedSchema[tableName][columnName] = string(model.TextDataType)
					continue
				}

				if _, ok := consolidatedSchema[tableName][columnName]; !ok {
					consolidatedSchema[tableName][columnName] = columnType
				}
			}
		}
	}
	return consolidatedSchema
}

// consolidateWarehouseSchema overwrites the consolidatedSchema with the warehouseSchema
// Prefer the type of the warehouseSchema, If the type is text, prefer text
func consolidateWarehouseSchema(consolidatedSchema, warehouseSchema model.Schema) model.Schema {
	for tableName, columnMap := range warehouseSchema {
		if _, ok := consolidatedSchema[tableName]; !ok {
			continue
		}

		for columnName, columnType := range columnMap {
			if _, ok := consolidatedSchema[tableName][columnName]; !ok {
				continue
			}

			var (
				consolidatedSchemaType = model.SchemaType(consolidatedSchema[tableName][columnName])
				warehouseSchemaType    = model.SchemaType(columnType)
			)

			if consolidatedSchemaType == model.TextDataType && warehouseSchemaType == model.StringDataType {
				continue
			}

			consolidatedSchema[tableName][columnName] = columnType
		}
	}
	return consolidatedSchema
}

// overrideUsersWithIdentifiesSchema overrides the users table with the identifies table
// users(id) <-> identifies(user_id)
// Removes the user_id column from the users table
func overrideUsersWithIdentifiesSchema(consolidatedSchema model.Schema, warehouseType string) model.Schema {
	var (
		usersTable      = warehouseutils.ToProviderCase(warehouseType, warehouseutils.UsersTable)
		identifiesTable = warehouseutils.ToProviderCase(warehouseType, warehouseutils.IdentifiesTable)
		userIDColumn    = warehouseutils.ToProviderCase(warehouseType, "user_id")
		IDColumn        = warehouseutils.ToProviderCase(warehouseType, "id")
	)

	if _, ok := consolidatedSchema[usersTable]; !ok {
		return consolidatedSchema
	}
	if _, ok := consolidatedSchema[identifiesTable]; !ok {
		return consolidatedSchema
	}

	for k, v := range consolidatedSchema[identifiesTable] {
		consolidatedSchema[usersTable][k] = v
	}

	consolidatedSchema[usersTable][IDColumn] = consolidatedSchema[identifiesTable][userIDColumn]
	delete(consolidatedSchema[usersTable], userIDColumn)

	return consolidatedSchema
}

// enhanceDiscardsSchema adds the discards table to the schema
// For bq, adds the loaded_at column to be segment compatible
func enhanceDiscardsSchema(consolidatedSchema model.Schema, warehouseType string) model.Schema {
	discards := model.TableSchema{}

	for colName, colType := range warehouseutils.DiscardsSchema {
		discards[warehouseutils.ToProviderCase(warehouseType, colName)] = colType
	}

	if warehouseType == warehouseutils.BQ {
		discards[warehouseutils.ToProviderCase(warehouseType, "loaded_at")] = "datetime"
	}

	consolidatedSchema[warehouseutils.ToProviderCase(warehouseType, warehouseutils.DiscardsTable)] = discards
	return consolidatedSchema
}

// enhanceSchemaWithIDResolution adds the merge rules and mappings table to the schema if IDResolution is enabled
func enhanceSchemaWithIDResolution(consolidatedSchema model.Schema, isIDResolutionEnabled bool, warehouseType string) model.Schema {
	if !isIDResolutionEnabled {
		return consolidatedSchema
	}
	var (
		mergeRulesTable = warehouseutils.ToProviderCase(warehouseType, warehouseutils.IdentityMergeRulesTable)
		mappingsTable   = warehouseutils.ToProviderCase(warehouseType, warehouseutils.IdentityMappingsTable)
	)
	if _, ok := consolidatedSchema[mergeRulesTable]; ok {
		consolidatedSchema[mergeRulesTable] = model.TableSchema{
			warehouseutils.ToProviderCase(warehouseType, "merge_property_1_type"):  "string",
			warehouseutils.ToProviderCase(warehouseType, "merge_property_1_value"): "string",
			warehouseutils.ToProviderCase(warehouseType, "merge_property_2_type"):  "string",
			warehouseutils.ToProviderCase(warehouseType, "merge_property_2_value"): "string",
		}
		consolidatedSchema[mappingsTable] = model.TableSchema{
			warehouseutils.ToProviderCase(warehouseType, "merge_property_type"):  "string",
			warehouseutils.ToProviderCase(warehouseType, "merge_property_value"): "string",
			warehouseutils.ToProviderCase(warehouseType, "rudder_id"):            "string",
			warehouseutils.ToProviderCase(warehouseType, "updated_at"):           "datetime",
		}
	}
	return consolidatedSchema
}

// HasSchemaChanged compares the localSchema with the schemaInWarehouse and returns true if they are not equal
func (sh *Handler) HasSchemaChanged(localSchema, schemaInWarehouse model.Schema) bool {
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
func GetTableSchemaDiff(tableName string, currentSchema, uploadSchema model.Schema) (diff warehouseutils.TableSchemaDiff) {
	diff = warehouseutils.TableSchemaDiff{
		ColumnMap:        make(model.TableSchema),
		UpdatedSchema:    make(model.TableSchema),
		AlteredColumnMap: make(model.TableSchema),
	}

	var (
		currentTableSchema model.TableSchema
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

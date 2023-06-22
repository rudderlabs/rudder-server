package warehouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errIncompatibleSchemaConversion = errors.New("incompatible schema conversion")
	errSchemaConversionNotSupported = errors.New("schema conversion not supported")
)

// deprecatedColumnsRegex
// This regex is used to identify deprecated columns in the warehouse
// Example: abc-deprecated-dba626a7-406a-4757-b3e0-3875559c5840
var deprecatedColumnsRegex = regexp.MustCompile(`.*-deprecated-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type schemaRepo interface {
	GetForNamespace(ctx context.Context, sourceID, destID, namespace string) (model.WHSchema, error)
	Insert(ctx context.Context, whSchema *model.WHSchema) (int64, error)
}

type stagingFileRepo interface {
	GetSchemasByIDs(ctx context.Context, ids []int64) ([]model.Schema, error)
}

type fetchSchemaRepo interface {
	FetchSchema(ctx context.Context) (model.Schema, model.Schema, error)
}

type Schema struct {
	warehouse                        model.Warehouse
	localSchema                      model.Schema
	schemaInWarehouse                model.Schema
	unrecognizedSchemaInWarehouse    model.Schema
	uploadSchema                     model.Schema
	schemaRepo                       schemaRepo
	stagingFileRepo                  stagingFileRepo
	log                              logger.Logger
	stagingFilesSchemaPaginationSize int
	skipDeepEqualSchemas             bool
	enableIDResolution               bool
}

func NewSchema(
	db *sql.DB,
	warehouse model.Warehouse,
	conf *config.Config,
) *Schema {
	return &Schema{
		warehouse:                        warehouse,
		schemaRepo:                       repo.NewWHSchemas(db),
		stagingFileRepo:                  repo.NewStagingFiles(db),
		log:                              logger.NewLogger().Child("warehouse").Child("schema"),
		stagingFilesSchemaPaginationSize: conf.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100),
		skipDeepEqualSchemas:             conf.GetBool("Warehouse.skipDeepEqualSchemas", false),
		enableIDResolution:               conf.GetBool("Warehouse.enableIDResolution", false),
	}
}

func (sh *Schema) updateLocalSchema(ctx context.Context, uploadId int64, updatedSchema model.Schema) error {
	ctx, cancel := context.WithTimeout(
		ctx,
		config.GetDuration(
			"Warehouse.schemaUpdateTimeout",
			30,
			time.Second,
		),
	)
	defer cancel()
	_, err := sh.schemaRepo.Insert(ctx, &model.WHSchema{
		UploadID:        uploadId,
		SourceID:        sh.warehouse.Source.ID,
		Namespace:       sh.warehouse.Namespace,
		DestinationID:   sh.warehouse.Destination.ID,
		DestinationType: sh.warehouse.Type,
		Schema:          updatedSchema,
	})
	if err != nil {
		return fmt.Errorf("updating local schema: %w", err)
	}

	sh.localSchema = updatedSchema

	return nil
}

// fetchSchemaFromLocal fetches schema from local
func (sh *Schema) fetchSchemaFromLocal(ctx context.Context) error {
	localSchema, err := sh.getLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema from local: %w", err)
	}

	sh.localSchema = localSchema

	return nil
}

func (sh *Schema) getLocalSchema(ctx context.Context) (model.Schema, error) {
	whSchema, err := sh.schemaRepo.GetForNamespace(
		ctx,
		sh.warehouse.Source.ID,
		sh.warehouse.Destination.ID,
		sh.warehouse.Namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("getting schema for namespace: %w", err)
	}
	if whSchema.Schema == nil {
		return model.Schema{}, nil
	}
	return whSchema.Schema, nil
}

// fetchSchemaFromWarehouse fetches schema from warehouse
func (sh *Schema) fetchSchemaFromWarehouse(ctx context.Context, repo fetchSchemaRepo) error {
	warehouseSchema, unrecognizedWarehouseSchema, err := repo.FetchSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	sh.skipDeprecatedColumns(warehouseSchema)
	sh.skipDeprecatedColumns(unrecognizedWarehouseSchema)

	sh.schemaInWarehouse = warehouseSchema
	sh.unrecognizedSchemaInWarehouse = unrecognizedWarehouseSchema

	return nil
}

// skipDeprecatedColumns skips deprecated columns from the schema
func (sh *Schema) skipDeprecatedColumns(schema model.Schema) {
	for tableName, columnMap := range schema {
		for columnName := range columnMap {
			if deprecatedColumnsRegex.MatchString(columnName) {
				sh.log.Debugw("skipping deprecated column",
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

func (sh *Schema) prepareUploadSchema(ctx context.Context, stagingFiles []*model.StagingFile) error {
	ctx, cancel := context.WithTimeout(
		ctx,
		config.GetDuration(
			"Warehouse.schemaGenerationTimeout",
			30,
			time.Second,
		),
	)
	defer cancel()
	consolidatedSchema, err := sh.consolidateStagingFilesSchemaUsingWarehouseSchema(ctx, stagingFiles)
	if err != nil {
		return fmt.Errorf("consolidating staging files schema: %w", err)
	}

	sh.uploadSchema = consolidatedSchema
	return nil
}

// consolidateStagingFilesSchemaUsingWarehouseSchema consolidates staging files schema with warehouse schema
func (sh *Schema) consolidateStagingFilesSchemaUsingWarehouseSchema(ctx context.Context, stagingFiles []*model.StagingFile) (model.Schema, error) {
	consolidatedSchema := model.Schema{}
	batches := lo.Chunk(stagingFiles, sh.stagingFilesSchemaPaginationSize)
	for _, batch := range batches {
		schemas, err := sh.stagingFileRepo.GetSchemasByIDs(
			ctx,
			repo.StagingFileIDs(batch),
		)
		if err != nil {
			return model.Schema{}, fmt.Errorf("getting staging files schema: %v", err)
		}

		consolidatedSchema = consolidateStagingSchemas(consolidatedSchema, schemas)
	}

	consolidatedSchema = consolidateWarehouseSchema(consolidatedSchema, sh.localSchema)
	consolidatedSchema = overrideUsersWithIdentifiesSchema(consolidatedSchema, sh.warehouse.Type, sh.localSchema)
	consolidatedSchema = enhanceDiscardsSchema(consolidatedSchema, sh.warehouse.Type)
	consolidatedSchema = enhanceSchemaWithIDResolution(consolidatedSchema, sh.isIDResolutionEnabled(), sh.warehouse.Type)
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

// consolidateWarehouseSchema overwrites the consolidatedSchema with the schemaInWarehouse
// Prefer the type of the schemaInWarehouse, If the type is text, prefer text
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
func overrideUsersWithIdentifiesSchema(consolidatedSchema model.Schema, warehouseType string, warehouseSchema model.Schema) model.Schema {
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
	for k, v := range warehouseSchema[usersTable] {
		if _, ok := warehouseSchema[identifiesTable][k]; !ok {
			consolidatedSchema[usersTable][k] = v
			consolidatedSchema[identifiesTable][k] = v
		}
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

func (sh *Schema) isIDResolutionEnabled() bool {
	return sh.enableIDResolution && slices.Contains(warehouseutils.IdentityEnabledWarehouses, sh.warehouse.Type)
}

// hasSchemaChanged compares the localSchema with the schemaInWarehouse
func (sh *Schema) hasSchemaChanged() bool {
	if !sh.skipDeepEqualSchemas {
		eq := reflect.DeepEqual(sh.localSchema, sh.schemaInWarehouse)
		return !eq
	}
	// Iterating through all tableName in the localSchema
	for tableName := range sh.localSchema {
		localColumns := sh.localSchema[tableName]
		warehouseColumns, whColumnsExist := sh.schemaInWarehouse[tableName]

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

// generateTableSchemaDiff returns the diff between the warehouse schema and the upload schema
func (sh *Schema) generateTableSchemaDiff(tableName string) warehouseutils.TableSchemaDiff {
	diff := warehouseutils.TableSchemaDiff{
		ColumnMap:        make(model.TableSchema),
		UpdatedSchema:    make(model.TableSchema),
		AlteredColumnMap: make(model.TableSchema),
	}

	currentTableSchema, ok := sh.schemaInWarehouse[tableName]
	if !ok {
		if _, ok := sh.uploadSchema[tableName]; !ok {
			return diff
		}
		diff.Exists = true
		diff.TableToBeCreated = true
		diff.ColumnMap = sh.uploadSchema[tableName]
		diff.UpdatedSchema = sh.uploadSchema[tableName]
		return diff
	}

	for columnName, columnType := range currentTableSchema {
		diff.UpdatedSchema[columnName] = columnType
	}

	diff.ColumnMap = make(model.TableSchema)
	for columnName, columnType := range sh.uploadSchema[tableName] {
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

// handleSchemaChange checks if the existing column type is compatible with the new column type
func handleSchemaChange(existingDataType, currentDataType model.SchemaType, value any) (any, error) {
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
			err = errIncompatibleSchemaConversion
		} else {
			newColumnVal = float64(intVal)
		}
	} else if currentDataType == model.FloatDataType && (existingDataType == model.IntDataType || existingDataType == model.BigIntDataType) {
		floatVal, ok := value.(float64)
		if !ok {
			err = errIncompatibleSchemaConversion
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
		err = errSchemaConversionNotSupported
	}

	return newColumnVal, err
}

package schema

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sync"

	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// deprecatedColumnsRegex
// This regex is used to identify deprecated columns in the warehouse
// Example: abc-deprecated-dba626a7-406a-4757-b3e0-3875559c5840
var deprecatedColumnsRegex = regexp.MustCompile(
	`.*-deprecated-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

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
	schemaRepo                       schemaRepo
	stagingFileRepo                  stagingFileRepo
	log                              logger.Logger
	stagingFilesSchemaPaginationSize int
	skipDeepEqualSchemas             bool
	enableIDResolution               bool

	localSchema                     model.Schema
	localSchemaMu                   sync.RWMutex
	schemaInWarehouse               model.Schema
	schemaInWarehouseMu             sync.RWMutex
	unrecognizedSchemaInWarehouse   model.Schema
	unrecognizedSchemaInWarehouseMu sync.RWMutex
}

func New(
	db *sqlquerywrapper.DB,
	warehouse model.Warehouse,
	conf *config.Config,
	logger logger.Logger,
) *Schema {
	return &Schema{
		warehouse:                        warehouse,
		schemaRepo:                       repo.NewWHSchemas(db),
		stagingFileRepo:                  repo.NewStagingFiles(db),
		log:                              logger.Child("schema"),
		stagingFilesSchemaPaginationSize: conf.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100),
		skipDeepEqualSchemas:             conf.GetBool("Warehouse.skipDeepEqualSchemas", false),
		enableIDResolution:               conf.GetBool("Warehouse.enableIDResolution", false),
	}
}

// ConsolidateStagingFilesUsingLocalSchema consolidates staging files schema with warehouse schema
func (sh *Schema) ConsolidateStagingFilesUsingLocalSchema(ctx context.Context, stagingFiles []*model.StagingFile) (model.Schema, error) {
	consolidatedSchema := model.Schema{}
	batches := lo.Chunk(stagingFiles, sh.stagingFilesSchemaPaginationSize)
	for _, batch := range batches {
		schemas, err := sh.stagingFileRepo.GetSchemasByIDs(ctx, repo.StagingFileIDs(batch))
		if err != nil {
			return nil, fmt.Errorf("getting staging files schema: %v", err)
		}

		consolidatedSchema = consolidateStagingSchemas(consolidatedSchema, schemas)
	}

	sh.localSchemaMu.RLock()
	consolidatedSchema = consolidateWarehouseSchema(consolidatedSchema, sh.localSchema)
	consolidatedSchema = overrideUsersWithIdentifiesSchema(consolidatedSchema, sh.warehouse.Type, sh.localSchema)
	sh.localSchemaMu.RUnlock()

	consolidatedSchema = enhanceDiscardsSchema(consolidatedSchema, sh.warehouse.Type)
	consolidatedSchema = enhanceSchemaWithIDResolution(consolidatedSchema, sh.isIDResolutionEnabled(), sh.warehouse.Type)

	return consolidatedSchema, nil
}

func (sh *Schema) UpdateLocalSchemaWithWarehouse(ctx context.Context, uploadID int64) error {
	sh.schemaInWarehouseMu.RLock()
	defer sh.schemaInWarehouseMu.RUnlock()
	return sh.updateLocalSchema(ctx, uploadID, sh.schemaInWarehouse)
}

func (sh *Schema) UpdateLocalSchema(ctx context.Context, uploadID int64, updatedSchema model.Schema) error {
	return sh.updateLocalSchema(ctx, uploadID, updatedSchema)
}

func (sh *Schema) GetLocalSchema(ctx context.Context) (model.Schema, error) {
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

// FetchSchemaFromWarehouse fetches schema from warehouse
func (sh *Schema) FetchSchemaFromWarehouse(ctx context.Context, repo fetchSchemaRepo) error {
	warehouseSchema, unrecognizedWarehouseSchema, err := repo.FetchSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	sh.removeDeprecatedColumns(warehouseSchema)
	sh.removeDeprecatedColumns(unrecognizedWarehouseSchema)

	sh.schemaInWarehouseMu.Lock()
	defer sh.schemaInWarehouseMu.Unlock()
	sh.unrecognizedSchemaInWarehouseMu.Lock()
	defer sh.unrecognizedSchemaInWarehouseMu.Unlock()

	sh.schemaInWarehouse = warehouseSchema
	sh.unrecognizedSchemaInWarehouse = unrecognizedWarehouseSchema

	return nil
}

func (sh *Schema) SyncRemoteSchema(ctx context.Context, fetchSchemaRepo fetchSchemaRepo, uploadID int64) (bool, error) {
	localSchema, err := sh.GetLocalSchema(ctx)
	if err != nil {
		return false, fmt.Errorf("fetching schema from local: %w", err)
	}
	if err := sh.FetchSchemaFromWarehouse(ctx, fetchSchemaRepo); err != nil {
		return false, fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	sh.schemaInWarehouseMu.RLock()
	defer sh.schemaInWarehouseMu.RUnlock()
	schemaChanged := sh.hasSchemaChanged(localSchema)
	if schemaChanged {
		err := sh.updateLocalSchema(ctx, uploadID, sh.schemaInWarehouse)
		if err != nil {
			return false, fmt.Errorf("updating local schema: %w", err)
		}
	}

	return schemaChanged, nil
}

// TableSchemaDiff returns the diff between the warehouse schema and the upload schema
func (sh *Schema) TableSchemaDiff(tableName string, tableSchema model.TableSchema) whutils.TableSchemaDiff {
	diff := whutils.TableSchemaDiff{
		ColumnMap:        make(model.TableSchema),
		UpdatedSchema:    make(model.TableSchema),
		AlteredColumnMap: make(model.TableSchema),
	}

	sh.schemaInWarehouseMu.RLock()
	currentTableSchema, ok := sh.schemaInWarehouse[tableName]
	sh.schemaInWarehouseMu.RUnlock()

	if !ok {
		if len(tableSchema) == 0 {
			return diff
		}
		diff.Exists = true
		diff.TableToBeCreated = true
		diff.ColumnMap = tableSchema
		diff.UpdatedSchema = tableSchema
		return diff
	}

	for columnName, columnType := range currentTableSchema {
		diff.UpdatedSchema[columnName] = columnType
	}

	diff.ColumnMap = make(model.TableSchema)
	for columnName, columnType := range tableSchema {
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

func (sh *Schema) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	sh.schemaInWarehouseMu.RLock()
	defer sh.schemaInWarehouseMu.RUnlock()
	return sh.schemaInWarehouse[tableName]
}

func (sh *Schema) UpdateWarehouseTableSchema(tableName string, tableSchema model.TableSchema) {
	sh.schemaInWarehouseMu.Lock()
	defer sh.schemaInWarehouseMu.Unlock()
	if sh.schemaInWarehouse == nil {
		sh.schemaInWarehouse = make(model.Schema)
	}
	sh.schemaInWarehouse[tableName] = tableSchema
}

func (sh *Schema) IsWarehouseSchemaEmpty() bool {
	sh.schemaInWarehouseMu.RLock()
	defer sh.schemaInWarehouseMu.RUnlock()
	return len(sh.schemaInWarehouse) == 0
}

func (sh *Schema) GetColumnsCountInWarehouseSchema(tableName string) int {
	sh.schemaInWarehouseMu.RLock()
	defer sh.schemaInWarehouseMu.RUnlock()
	return len(sh.schemaInWarehouse[tableName])
}

func (sh *Schema) IsColumnInUnrecognizedSchema(t, c string) bool {
	sh.unrecognizedSchemaInWarehouseMu.RLock()
	defer sh.unrecognizedSchemaInWarehouseMu.RUnlock()
	s, ok := sh.unrecognizedSchemaInWarehouse[t]
	if ok {
		_, ok = s[c]
	}
	return ok
}

func (sh *Schema) updateLocalSchema(ctx context.Context, uploadId int64, updatedSchema model.Schema) error {
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

	sh.localSchemaMu.Lock()
	sh.localSchema = updatedSchema
	sh.localSchemaMu.Unlock()

	return nil
}

// removeDeprecatedColumns skips deprecated columns from the schema map
func (sh *Schema) removeDeprecatedColumns(schema model.Schema) {
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
			}
		}
	}
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
		usersTable      = whutils.ToProviderCase(warehouseType, whutils.UsersTable)
		identifiesTable = whutils.ToProviderCase(warehouseType, whutils.IdentifiesTable)
		userIDColumn    = whutils.ToProviderCase(warehouseType, "user_id")
		IDColumn        = whutils.ToProviderCase(warehouseType, "id")
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

	for colName, colType := range whutils.DiscardsSchema {
		discards[whutils.ToProviderCase(warehouseType, colName)] = colType
	}

	if warehouseType == whutils.BQ {
		discards[whutils.ToProviderCase(warehouseType, "loaded_at")] = "datetime"
	}

	consolidatedSchema[whutils.ToProviderCase(warehouseType, whutils.DiscardsTable)] = discards
	return consolidatedSchema
}

// enhanceSchemaWithIDResolution adds the merge rules and mappings table to the schema if IDResolution is enabled
func enhanceSchemaWithIDResolution(consolidatedSchema model.Schema, isIDResolutionEnabled bool, warehouseType string) model.Schema {
	if !isIDResolutionEnabled {
		return consolidatedSchema
	}
	var (
		mergeRulesTable = whutils.ToProviderCase(warehouseType, whutils.IdentityMergeRulesTable)
		mappingsTable   = whutils.ToProviderCase(warehouseType, whutils.IdentityMappingsTable)
	)
	if _, ok := consolidatedSchema[mergeRulesTable]; ok {
		consolidatedSchema[mergeRulesTable] = model.TableSchema{
			whutils.ToProviderCase(warehouseType, "merge_property_1_type"):  "string",
			whutils.ToProviderCase(warehouseType, "merge_property_1_value"): "string",
			whutils.ToProviderCase(warehouseType, "merge_property_2_type"):  "string",
			whutils.ToProviderCase(warehouseType, "merge_property_2_value"): "string",
		}
		consolidatedSchema[mappingsTable] = model.TableSchema{
			whutils.ToProviderCase(warehouseType, "merge_property_type"):  "string",
			whutils.ToProviderCase(warehouseType, "merge_property_value"): "string",
			whutils.ToProviderCase(warehouseType, "rudder_id"):            "string",
			whutils.ToProviderCase(warehouseType, "updated_at"):           "datetime",
		}
	}
	return consolidatedSchema
}

func (sh *Schema) isIDResolutionEnabled() bool {
	return sh.enableIDResolution && slices.Contains(whutils.IdentityEnabledWarehouses, sh.warehouse.Type)
}

// hasSchemaChanged compares the localSchema with the schemaInWarehouse
func (sh *Schema) hasSchemaChanged(localSchema model.Schema) bool {
	if !sh.skipDeepEqualSchemas {
		eq := reflect.DeepEqual(localSchema, sh.schemaInWarehouse)
		return !eq
	}
	// Iterating through all tableName in the localSchema
	for tableName := range localSchema {
		localColumns := localSchema[tableName]
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

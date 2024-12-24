package schema

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	FetchSchema(ctx context.Context) (model.Schema, error)
}

type Schema struct {
	warehouse                        model.Warehouse
	schemaRepo                       schemaRepo
	stagingFileRepo                  stagingFileRepo
	log                              logger.Logger
	stagingFilesSchemaPaginationSize int
	enableIDResolution               bool

	localSchema   model.Schema
	localSchemaMu sync.RWMutex

	stats struct {
		schemaSize stats.Histogram
	}
}

func New(
	db *sqlquerywrapper.DB,
	warehouse model.Warehouse,
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
) *Schema {
	s := &Schema{
		warehouse:                        warehouse,
		schemaRepo:                       repo.NewWHSchemas(db),
		stagingFileRepo:                  repo.NewStagingFiles(db),
		log:                              logger.Child("schema"),
		stagingFilesSchemaPaginationSize: conf.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100),
		enableIDResolution:               conf.GetBool("Warehouse.enableIDResolution", false),
	}
	s.stats.schemaSize = statsFactory.NewTaggedStat("warehouse_schema_size", stats.HistogramType, stats.Tags{
		"module":        "warehouse",
		"workspaceId":   warehouse.WorkspaceID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
		"sourceId":      warehouse.Source.ID,
		"destinationId": warehouse.Destination.ID,
	})
	return s
}

// ConsolidateStagingFilesUsingLocalSchema
// 1. Fetches the schemas for the staging files
// 2. Consolidates the staging files schemas
// 3. Consolidates the consolidated schema with the warehouse schema
// 4. Enhances the consolidated schema with discards schema
// 5. Enhances the consolidated schema with ID resolution schema
// 6. Returns the consolidated schema
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

// consolidateStagingSchemas merges multiple schemas into one
// Prefer the type of the first schema, If the type is text, prefer text
func consolidateStagingSchemas(consolidatedSchema model.Schema, schemas []model.Schema) model.Schema {
	for _, schema := range schemas {
		for tableName, columnMap := range schema {
			if _, ok := consolidatedSchema[tableName]; !ok {
				consolidatedSchema[tableName] = model.TableSchema{}
			}
			for columnName, columnType := range columnMap {
				if columnType == model.TextDataType {
					consolidatedSchema[tableName][columnName] = model.TextDataType
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
				consolidatedSchemaType = consolidatedSchema[tableName][columnName]
				warehouseSchemaType    = columnType
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

func (sh *Schema) UpdateLocalSchemaWithWarehouse(ctx context.Context, warehouseSchema model.Schema) error {
	return sh.updateSchema(ctx, warehouseSchema)
}

func (sh *Schema) UpdateLocalSchema(ctx context.Context) error {
	return sh.updateSchema(ctx, sh.localSchema)
}

func (sh *Schema) UpdateSchema(ctx context.Context, updatedSchema model.Schema) error {
	return sh.updateSchema(ctx, updatedSchema)
}

// updateLocalSchema
// 1. Inserts the updated schema into the local schema table
// 2. Updates the local schema instance
func (sh *Schema) updateSchema(ctx context.Context, updatedSchema model.Schema) error {
	updatedSchemaInBytes, err := json.Marshal(updatedSchema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	sh.stats.schemaSize.Observe(float64(len(updatedSchemaInBytes)))

	_, err = sh.schemaRepo.Insert(ctx, &model.WHSchema{
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

// GetLocalSchema returns the local schema from wh_schemas table
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
	sh.localSchemaMu.Lock()
	sh.localSchema = whSchema.Schema
	sh.localSchemaMu.Unlock()
	return whSchema.Schema, nil
}

// FetchSchemaFromWarehouse
// 1. Fetches schema from warehouse
// 2. Removes deprecated columns from schema
// 3. Updates local warehouse schema and unrecognized schema instance
func (sh *Schema) FetchSchemaFromWarehouse(ctx context.Context, repo fetchSchemaRepo) (model.Schema, error) {
	warehouseSchema, err := repo.FetchSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}

	sh.removeDeprecatedColumns(warehouseSchema)
	return warehouseSchema, nil
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

// HasSchemaChanged compares the localSchema with the schemaInWarehouse
func (sh *Schema) HasSchemaChanged(schema model.Schema) bool {
	return !reflect.DeepEqual(schema, sh.localSchema)
}

// TableSchemaDiff returns the diff between the warehouse schema and the upload schema
func (sh *Schema) TableSchemaDiff(tableName string, tableSchema model.TableSchema) whutils.TableSchemaDiff {
	diff := whutils.TableSchemaDiff{
		ColumnMap:        make(model.TableSchema),
		UpdatedSchema:    make(model.TableSchema),
		AlteredColumnMap: make(model.TableSchema),
	}

	sh.localSchemaMu.RLock()
	currentTableSchema, ok := sh.localSchema[tableName]
	sh.localSchemaMu.RUnlock()

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
		} else if columnType == model.TextDataType && currentTableSchema[columnName] == model.StringDataType {
			diff.AlteredColumnMap[columnName] = columnType
			diff.UpdatedSchema[columnName] = columnType
			diff.Exists = true
		}
	}
	return diff
}

func (sh *Schema) GetTableSchema(tableName string) model.TableSchema {
	sh.localSchemaMu.RLock()
	defer sh.localSchemaMu.RUnlock()
	return sh.localSchema[tableName]
}

func (sh *Schema) UpdateTableSchema(tableName string, tableSchema model.TableSchema) {
	sh.localSchemaMu.RLock()
	defer sh.localSchemaMu.RUnlock()
	if sh.localSchema == nil {
		sh.localSchema = make(model.Schema)
	}
	sh.localSchema[tableName] = tableSchema
}

func (sh *Schema) IsSchemaEmpty() bool {
	sh.localSchemaMu.RLock()
	defer sh.localSchemaMu.RUnlock()
	return len(sh.localSchema) == 0
}

func (sh *Schema) GetColumnsCountInSchema(tableName string) int {
	sh.localSchemaMu.RLock()
	defer sh.localSchemaMu.RUnlock()
	return len(sh.localSchema[tableName])
}

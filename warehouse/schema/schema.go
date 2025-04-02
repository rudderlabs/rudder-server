package schema

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
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
	FetchSchema(ctx context.Context) (model.Schema, error)
}

type Handler interface {
	// Synchronizes the schema with the schema from the warehouse
	SyncRemoteSchema(ctx context.Context, fetchSchemaRepo fetchSchemaRepo, uploadID int64) (bool, error)
	// Check if schema exists for the namespace
	IsWarehouseSchemaEmpty(ctx context.Context) bool
	// Retrieves the schema for a specific table
	GetTableSchemaInWarehouse(ctx context.Context, tableName string) model.TableSchema
	// Updates the schema with the provided schema definition
	UpdateLocalSchema(ctx context.Context, updatedSchema model.Schema) error
	// Updates the schema for a specific table
	UpdateWarehouseTableSchema(ctx context.Context, tableName string, tableSchema model.TableSchema) error
	// Returns the number of columns present in the schema for a given table
	GetColumnsCountInWarehouseSchema(ctx context.Context, tableName string) (int, error)
	// Merges schemas from staging files with the schema to produce a consolidated schema.
	ConsolidateStagingFilesUsingLocalSchema(ctx context.Context, stagingFiles []*model.StagingFile) (model.Schema, error)
	// TODO: Remove this method as it's no longer needed.
	UpdateLocalSchemaWithWarehouse(ctx context.Context) error
	// Computes the difference between the existing schema of a table and a newly provided schema.
	// Returns details of added and modified columns
	TableSchemaDiff(ctx context.Context, tableName string, tableSchema model.TableSchema) (whutils.TableSchemaDiff, error)
	// Synchronizes the schema with the schema from the warehouse
	FetchSchemaFromWarehouse(ctx context.Context, repo fetchSchemaRepo) error
}

type schema struct {
	stats struct {
		schemaSize stats.Histogram
	}
	warehouse                        model.Warehouse
	log                              logger.Logger
	ttlInMinutes                     time.Duration
	schemaRepo                       schemaRepo
	stagingFilesSchemaPaginationSize int
	stagingFileRepo                  stagingFileRepo
	enableIDResolution               bool
	fetchSchemaRepo                  fetchSchemaRepo
	now                              func() time.Time
	cachedSchema                     model.Schema
	cacheExpiry                      time.Time
	cachedSchemaMu                   sync.RWMutex
}

func New(
	db *sqlquerywrapper.DB,
	warehouse model.Warehouse,
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	fetchSchemaRepo fetchSchemaRepo,
) Handler {
	ttlInMinutes := conf.GetDurationVar(720, time.Minute, "Warehouse.schemaTTLInMinutes")
	schema := &schema{
		warehouse:                        warehouse,
		log:                              logger.Child("schema"),
		ttlInMinutes:                     ttlInMinutes,
		schemaRepo:                       repo.NewWHSchemas(db),
		stagingFilesSchemaPaginationSize: conf.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100),
		stagingFileRepo:                  repo.NewStagingFiles(db),
		fetchSchemaRepo:                  fetchSchemaRepo,
		enableIDResolution:               conf.GetBool("Warehouse.enableIDResolution", false),
		now:                              timeutil.Now,
	}
	schema.stats.schemaSize = statsFactory.NewTaggedStat("warehouse_schema_size", stats.HistogramType, stats.Tags{
		"module":        "warehouse",
		"workspaceId":   warehouse.WorkspaceID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
		"sourceId":      warehouse.Source.ID,
		"destinationId": warehouse.Destination.ID,
	})
	return schema
}

func (sh *schema) SyncRemoteSchema(ctx context.Context, _ fetchSchemaRepo, uploadID int64) (bool, error) {
	return false, sh.FetchSchemaFromWarehouse(ctx, sh.fetchSchemaRepo)
}

func (sh *schema) IsWarehouseSchemaEmpty(ctx context.Context) bool {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		sh.log.Warnn("error getting schema", obskit.Error(err))
		return true
	}
	return len(schema) == 0
}

func (sh *schema) GetTableSchemaInWarehouse(ctx context.Context, tableName string) model.TableSchema {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		sh.log.Warnn("error getting schema", obskit.Error(err))
		return model.TableSchema{}
	}
	return schema[tableName]
}

func (sh *schema) UpdateLocalSchema(ctx context.Context, updatedSchema model.Schema) error {
	updatedSchemaInBytes, err := jsonrs.Marshal(updatedSchema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	sh.stats.schemaSize.Observe(float64(len(updatedSchemaInBytes)))
	return sh.saveSchema(ctx, updatedSchema)
}

func (sh *schema) UpdateWarehouseTableSchema(ctx context.Context, tableName string, tableSchema model.TableSchema) error {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}
	schemaCopy := make(model.Schema)
	for k, v := range schema {
		schemaCopy[k] = v
	}
	schemaCopy[tableName] = tableSchema
	err = sh.saveSchema(ctx, schemaCopy)
	if err != nil {
		return fmt.Errorf("saving schema: %w", err)
	}
	return nil
}

func (sh *schema) GetColumnsCountInWarehouseSchema(ctx context.Context, tableName string) (int, error) {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting schema: %w", err)
	}
	return len(schema[tableName]), nil
}

func (sh *schema) ConsolidateStagingFilesUsingLocalSchema(ctx context.Context, stagingFiles []*model.StagingFile) (model.Schema, error) {
	consolidatedSchema := model.Schema{}
	batches := lo.Chunk(stagingFiles, sh.stagingFilesSchemaPaginationSize)
	for _, batch := range batches {
		schemas, err := sh.stagingFileRepo.GetSchemasByIDs(ctx, repo.StagingFileIDs(batch))
		if err != nil {
			return nil, fmt.Errorf("getting staging files schema: %v", err)
		}

		consolidatedSchema = consolidateStagingSchemas(consolidatedSchema, schemas)
	}
	schema, err := sh.getSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %v", err)
	}
	consolidatedSchema = consolidateWarehouseSchema(consolidatedSchema, schema)
	consolidatedSchema = overrideUsersWithIdentifiesSchema(consolidatedSchema, sh.warehouse.Type, schema)
	consolidatedSchema = enhanceDiscardsSchema(consolidatedSchema, sh.warehouse.Type)
	consolidatedSchema = enhanceSchemaWithIDResolution(consolidatedSchema, sh.isIDResolutionEnabled(), sh.warehouse.Type)

	return consolidatedSchema, nil
}

func (sh *schema) isIDResolutionEnabled() bool {
	return sh.enableIDResolution && slices.Contains(whutils.IdentityEnabledWarehouses, sh.warehouse.Type)
}

func (sh *schema) UpdateLocalSchemaWithWarehouse(ctx context.Context) error {
	// no-op
	return nil
}

func (sh *schema) TableSchemaDiff(ctx context.Context, tableName string, tableSchema model.TableSchema) (whutils.TableSchemaDiff, error) {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		return whutils.TableSchemaDiff{}, fmt.Errorf("getting schema: %w", err)
	}
	return tableSchemaDiff(tableName, schema, tableSchema), nil
}

func (sh *schema) FetchSchemaFromWarehouse(ctx context.Context, _ fetchSchemaRepo) error {
	_, err := sh.fetchSchemaFromWarehouse(ctx)
	return err
}

func (sh *schema) fetchSchemaFromWarehouse(ctx context.Context) (model.Schema, error) {
	warehouseSchema, err := sh.fetchSchemaRepo.FetchSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	removeDeprecatedColumns(warehouseSchema, sh.warehouse, sh.log)
	return warehouseSchema, sh.saveSchema(ctx, warehouseSchema)
}

func (sh *schema) saveSchema(ctx context.Context, newSchema model.Schema) error {
	expiresAt := sh.now().Add(sh.ttlInMinutes)
	_, err := sh.schemaRepo.Insert(ctx, &model.WHSchema{
		SourceID:        sh.warehouse.Source.ID,
		Namespace:       sh.warehouse.Namespace,
		DestinationID:   sh.warehouse.Destination.ID,
		DestinationType: sh.warehouse.Type,
		Schema:          newSchema,
		ExpiresAt:       expiresAt,
	})
	if err != nil {
		return fmt.Errorf("inserting schema: %w", err)
	}
	sh.cachedSchemaMu.Lock()
	sh.cachedSchema = newSchema
	sh.cachedSchemaMu.Unlock()
	sh.cacheExpiry = expiresAt
	return nil
}

func (sh *schema) getSchema(ctx context.Context) (model.Schema, error) {
	sh.cachedSchemaMu.RLock()
	if sh.cachedSchema != nil && sh.cacheExpiry.After(sh.now()) {
		defer sh.cachedSchemaMu.RUnlock()
		return sh.cachedSchema, nil
	}
	sh.cachedSchemaMu.RUnlock()
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
		sh.cachedSchemaMu.Lock()
		defer sh.cachedSchemaMu.Unlock()
		sh.cachedSchema = model.Schema{}
		sh.cacheExpiry = sh.now().Add(sh.ttlInMinutes)
		return sh.cachedSchema, nil
	}
	if whSchema.ExpiresAt.Before(sh.now()) {
		sh.log.Infon("Schema expired", obskit.DestinationID(sh.warehouse.Destination.ID), obskit.Namespace(sh.warehouse.Namespace), logger.NewTimeField("expiresAt", whSchema.ExpiresAt))
		return sh.fetchSchemaFromWarehouse(ctx)
	}
	sh.cachedSchemaMu.Lock()
	defer sh.cachedSchemaMu.Unlock()
	sh.cachedSchema = whSchema.Schema
	sh.cacheExpiry = whSchema.ExpiresAt
	return sh.cachedSchema, nil
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

func removeDeprecatedColumns(schema model.Schema, warehouse model.Warehouse, log logger.Logger) {
	for tableName, columnMap := range schema {
		for columnName := range columnMap {
			if deprecatedColumnsRegex.MatchString(columnName) {
				log.Debugw("skipping deprecated column",
					logfield.SourceID, warehouse.Source.ID,
					logfield.DestinationID, warehouse.Destination.ID,
					logfield.DestinationType, warehouse.Destination.DestinationDefinition.Name,
					logfield.WorkspaceID, warehouse.WorkspaceID,
					logfield.Namespace, warehouse.Namespace,
					logfield.TableName, tableName,
					logfield.ColumnName, columnName,
				)
				delete(schema[tableName], columnName)
			}
		}
	}
}

func tableSchemaDiff(tableName string, schemaMap model.Schema, tableSchema model.TableSchema) whutils.TableSchemaDiff {
	diff := whutils.TableSchemaDiff{
		ColumnMap:        make(model.TableSchema),
		UpdatedSchema:    make(model.TableSchema),
		AlteredColumnMap: make(model.TableSchema),
	}

	currentTableSchema, ok := schemaMap[tableName]

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

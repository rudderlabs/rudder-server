package schema

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type schemaV2 struct {
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

func newSchemaV2(v1 *schema, warehouse model.Warehouse, log logger.Logger, ttlInMinutes time.Duration, fetchSchemaRepo fetchSchemaRepo) *schemaV2 {
	v2 := &schemaV2{
		warehouse:                        warehouse,
		log:                              log,
		ttlInMinutes:                     ttlInMinutes,
		schemaRepo:                       v1.schemaRepo,
		stagingFilesSchemaPaginationSize: v1.stagingFilesSchemaPaginationSize,
		stagingFileRepo:                  v1.stagingFileRepo,
		fetchSchemaRepo:                  fetchSchemaRepo,
		enableIDResolution:               v1.enableIDResolution,
		now:                              timeutil.Now,
	}
	v2.stats.schemaSize = v1.stats.schemaSize
	return v2
}

func (sh *schemaV2) SyncRemoteSchema(ctx context.Context, _ fetchSchemaRepo, uploadID int64) (bool, error) {
	return false, sh.FetchSchemaFromWarehouse(ctx, sh.fetchSchemaRepo)
}

func (sh *schemaV2) IsWarehouseSchemaEmpty(ctx context.Context) bool {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		sh.log.Warnn("error getting schema", obskit.Error(err))
		return true
	}
	return len(schema) == 0
}

func (sh *schemaV2) GetTableSchemaInWarehouse(ctx context.Context, tableName string) model.TableSchema {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		sh.log.Warnn("error getting schema", obskit.Error(err))
		return model.TableSchema{}
	}
	return schema[tableName]
}

func (sh *schemaV2) GetLocalSchema(ctx context.Context) (model.Schema, error) {
	return sh.getSchema(ctx)
}

func (sh *schemaV2) UpdateLocalSchema(ctx context.Context, updatedSchema model.Schema) error {
	updatedSchemaInBytes, err := json.Marshal(updatedSchema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	sh.stats.schemaSize.Observe(float64(len(updatedSchemaInBytes)))
	return sh.saveSchema(ctx, updatedSchema)
}

func (sh *schemaV2) UpdateWarehouseTableSchema(ctx context.Context, tableName string, tableSchema model.TableSchema) error {
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

func (sh *schemaV2) GetColumnsCountInWarehouseSchema(ctx context.Context, tableName string) (int, error) {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting schema: %w", err)
	}
	return len(schema[tableName]), nil
}

func (sh *schemaV2) ConsolidateStagingFilesUsingLocalSchema(ctx context.Context, stagingFiles []*model.StagingFile) (model.Schema, error) {
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

func (sh *schemaV2) isIDResolutionEnabled() bool {
	return sh.enableIDResolution && slices.Contains(whutils.IdentityEnabledWarehouses, sh.warehouse.Type)
}

func (sh *schemaV2) UpdateLocalSchemaWithWarehouse(ctx context.Context) error {
	// no-op
	return nil
}

func (sh *schemaV2) TableSchemaDiff(ctx context.Context, tableName string, tableSchema model.TableSchema) (whutils.TableSchemaDiff, error) {
	schema, err := sh.getSchema(ctx)
	if err != nil {
		return whutils.TableSchemaDiff{}, fmt.Errorf("getting schema: %w", err)
	}
	return tableSchemaDiff(tableName, schema, tableSchema), nil
}

func (sh *schemaV2) FetchSchemaFromWarehouse(ctx context.Context, _ fetchSchemaRepo) error {
	_, err := sh.fetchSchemaFromWarehouse(ctx)
	return err
}

func (sh *schemaV2) fetchSchemaFromWarehouse(ctx context.Context) (model.Schema, error) {
	warehouseSchema, err := sh.fetchSchemaRepo.FetchSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching schema: %w", err)
	}
	removeDeprecatedColumns(warehouseSchema, sh.warehouse, sh.log)
	return warehouseSchema, sh.saveSchema(ctx, warehouseSchema)
}

func (sh *schemaV2) saveSchema(ctx context.Context, newSchema model.Schema) error {
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

func (sh *schemaV2) getSchema(ctx context.Context) (model.Schema, error) {
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

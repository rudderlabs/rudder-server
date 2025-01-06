package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type schemaV2 struct {
	stats struct {
		schemaSize stats.Histogram
	}
	// caches the schema present in the repository
	cachedSchema model.Schema
	warehouse    model.Warehouse
	v1           *schema
	log          logger.Logger
	schemaMu     sync.RWMutex
}

func FetchAndSaveSchema(ctx context.Context, fetchSchemaRepo fetchSchemaRepo, warehouse model.Warehouse, schemaRepo schemaRepo, log logger.Logger) error {
	warehouseSchema, err := fetchSchemaRepo.FetchSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema: %w", err)
	}
	removeDeprecatedColumns(warehouseSchema, warehouse, log)
	return writeSchema(ctx, schemaRepo, warehouse, warehouseSchema)
}

func newSchemaV2(ctx context.Context, v1 *schema, warehouse model.Warehouse, log logger.Logger, schemaSize stats.Histogram) *schemaV2 {
	sh := &schemaV2{
		v1:           v1,
		warehouse:    warehouse,
		log:          log,
		cachedSchema: model.Schema{},
		stats: struct {
			schemaSize stats.Histogram
		}{
			schemaSize: schemaSize,
		},
	}
	_, err := sh.SyncRemoteSchema(ctx, nil, 0)
	if err != nil {
		sh.log.Errorf("error syncing remote schema: %w", err)
	}
	return sh
}

func (sh *schemaV2) SyncRemoteSchema(ctx context.Context, fetchSchemaRepo fetchSchemaRepo, uploadID int64) (bool, error) {
	whSchema, err := sh.v1.schemaRepo.GetForNamespace(
		ctx,
		sh.warehouse.Source.ID,
		sh.warehouse.Destination.ID,
		sh.warehouse.Namespace,
	)
	if err != nil {
		return false, fmt.Errorf("getting schema for namespace: %w", err)
	}
	if whSchema.Schema == nil {
		return false, nil
	}
	sh.schemaMu.Lock()
	defer sh.schemaMu.Unlock()
	sh.cachedSchema = whSchema.Schema
	return false, nil
}

func (sh *schemaV2) IsWarehouseSchemaEmpty() bool {
	sh.schemaMu.RLock()
	defer sh.schemaMu.RUnlock()
	return len(sh.cachedSchema) == 0
}

func (sh *schemaV2) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	sh.schemaMu.RLock()
	defer sh.schemaMu.RUnlock()
	return sh.cachedSchema[tableName]
}

func (sh *schemaV2) GetLocalSchema(ctx context.Context) (model.Schema, error) {
	return sh.cachedSchema, nil
}

func (sh *schemaV2) UpdateLocalSchema(ctx context.Context, updatedSchema model.Schema) error {
	updatedSchemaInBytes, err := json.Marshal(updatedSchema)
	if err != nil {
		return fmt.Errorf("marshaling schema: %w", err)
	}
	sh.stats.schemaSize.Observe(float64(len(updatedSchemaInBytes)))
	sh.schemaMu.Lock()
	defer sh.schemaMu.Unlock()
	err = writeSchema(ctx, sh.v1.schemaRepo, sh.warehouse, updatedSchema)
	if err != nil {
		return fmt.Errorf("updating local schema: %w", err)
	}
	sh.cachedSchema = updatedSchema
	return nil
}

func (sh *schemaV2) UpdateWarehouseTableSchema(tableName string, tableSchema model.TableSchema) {
	sh.schemaMu.Lock()
	defer sh.schemaMu.Unlock()
	sh.cachedSchema[tableName] = tableSchema
	err := writeSchema(context.TODO(), sh.v1.schemaRepo, sh.warehouse, sh.cachedSchema)
	if err != nil {
		// TODO - Return error to the caller
		sh.log.Errorf("error updating warehouse schema: %v", err)
	}
}

func (sh *schemaV2) GetColumnsCountInWarehouseSchema(tableName string) int {
	sh.schemaMu.RLock()
	defer sh.schemaMu.RUnlock()
	return len(sh.cachedSchema[tableName])
}

func (sh *schemaV2) ConsolidateStagingFilesUsingLocalSchema(ctx context.Context, stagingFiles []*model.StagingFile) (model.Schema, error) {
	consolidatedSchema := model.Schema{}
	batches := lo.Chunk(stagingFiles, sh.v1.stagingFilesSchemaPaginationSize)
	for _, batch := range batches {
		schemas, err := sh.v1.stagingFileRepo.GetSchemasByIDs(ctx, repo.StagingFileIDs(batch))
		if err != nil {
			return nil, fmt.Errorf("getting staging files schema: %v", err)
		}

		consolidatedSchema = consolidateStagingSchemas(consolidatedSchema, schemas)
	}
	sh.schemaMu.RLock()
	consolidatedSchema = consolidateWarehouseSchema(consolidatedSchema, sh.cachedSchema)
	consolidatedSchema = overrideUsersWithIdentifiesSchema(consolidatedSchema, sh.warehouse.Type, sh.cachedSchema)
	sh.schemaMu.RUnlock()
	consolidatedSchema = enhanceDiscardsSchema(consolidatedSchema, sh.warehouse.Type)
	consolidatedSchema = enhanceSchemaWithIDResolution(consolidatedSchema, sh.v1.isIDResolutionEnabled(), sh.warehouse.Type)

	return consolidatedSchema, nil
}

func (sh *schemaV2) UpdateLocalSchemaWithWarehouse(ctx context.Context) error {
	// no-op
	return nil
}

func (sh *schemaV2) TableSchemaDiff(tableName string, tableSchema model.TableSchema) whutils.TableSchemaDiff {
	sh.schemaMu.RLock()
	defer sh.schemaMu.RUnlock()
	return tableSchemaDiff(tableName, sh.cachedSchema, tableSchema)
}

func (sh *schemaV2) FetchSchemaFromWarehouse(ctx context.Context, repo fetchSchemaRepo) error {
	// no-op since local schema and warehouse schema are supposed to be in sync
	return nil
}

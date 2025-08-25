package schemarepository

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Uploader interface {
	GetLocalSchema(ctx context.Context) (model.Schema, error)
	UpdateLocalSchema(ctx context.Context, schema model.Schema) error
}
type LocalSchemaRepository struct {
	warehouse model.Warehouse
	uploader  Uploader

	// mu protects the read-modify-write pattern for schema operations
	// Operations like CreateTable, AddColumns, AlterColumn:
	// 1. Read schema from PostgreSQL via GetLocalSchema
	// 2. Modify the schema in memory
	// 3. Write back to PostgreSQL via UpdateLocalSchema
	// This prevents race conditions when multiple goroutines modify the schema concurrently
	mu sync.RWMutex
}

func NewLocalSchemaRepository(warehouse model.Warehouse, uploader Uploader) (*LocalSchemaRepository, error) {
	ls := LocalSchemaRepository{
		warehouse: warehouse,
		uploader:  uploader,
	}
	return &ls, nil
}

func (ls *LocalSchemaRepository) FetchSchema(ctx context.Context, _ model.Warehouse) (model.Schema, error) {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	schema, err := ls.uploader.GetLocalSchema(ctx)
	if err != nil {
		return model.Schema{}, fmt.Errorf("fetching local schema: %w", err)
	}
	return schema, nil
}

func (*LocalSchemaRepository) CreateSchema(context.Context) error {
	return nil
}

func (ls *LocalSchemaRepository) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching local schema: %w", err)
	}

	// check if table already exists
	if _, ok := schema[tableName]; ok {
		return fmt.Errorf("failed to create table: table %s already exists", tableName)
	}

	// add table to schema
	schema[tableName] = columnMap

	// update schema
	return ls.uploader.UpdateLocalSchema(ctx, schema)
}

func (ls *LocalSchemaRepository) AddColumns(ctx context.Context, tableName string, columnsInfo []whutils.ColumnInfo) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching local schema: %w", err)
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return fmt.Errorf("failed to add column: table %s does not exist", tableName)
	}

	for _, columnInfo := range columnsInfo {
		schema[tableName][columnInfo.Name] = columnInfo.Type
	}

	// update schema
	return ls.uploader.UpdateLocalSchema(ctx, schema)
}

func (ls *LocalSchemaRepository) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema(ctx)
	if err != nil {
		return model.AlterTableResponse{}, fmt.Errorf("fetching local schema: %w", err)
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return model.AlterTableResponse{}, fmt.Errorf("failed to add column: table %s does not exist", tableName)
	}

	// check if column exists
	if _, ok := schema[tableName][columnName]; !ok {
		return model.AlterTableResponse{}, fmt.Errorf("failed to alter column: column %s does not exist in table %s", columnName, tableName)
	}

	// update column type
	schema[tableName][columnName] = columnType

	// update schema
	return model.AlterTableResponse{}, ls.uploader.UpdateLocalSchema(ctx, schema)
}

func (*LocalSchemaRepository) RefreshPartitions(context.Context, string, []whutils.LoadFile) error {
	return nil
}

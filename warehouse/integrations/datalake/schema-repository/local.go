package schemarepository

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type LocalSchemaRepository struct {
	warehouse model.Warehouse
	uploader  warehouseutils.Uploader
}

func NewLocalSchemaRepository(wh model.Warehouse, uploader warehouseutils.Uploader) (*LocalSchemaRepository, error) {
	ls := LocalSchemaRepository{
		warehouse: wh,
		uploader:  uploader,
	}

	return &ls, nil
}

func (ls *LocalSchemaRepository) FetchSchema(ctx context.Context, _ model.Warehouse) (model.Schema, error) {
	schema, err := ls.uploader.GetLocalSchema(ctx)
	if err != nil {
		return model.Schema{}, fmt.Errorf("fetching local schema: %w", err)
	}

	return schema, nil
}

func (*LocalSchemaRepository) CreateSchema(context.Context) (err error) {
	return nil
}

func (ls *LocalSchemaRepository) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching local schema: %w", err)
	}

	if _, ok := schema[tableName]; ok {
		return fmt.Errorf("failed to create table: table %s already exists", tableName)
	}

	// add table to schema
	schema[tableName] = columnMap

	// update schema
	return ls.uploader.UpdateLocalSchema(ctx, schema)
}

func (ls *LocalSchemaRepository) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
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

	schema[tableName][columnName] = columnType

	// update schema
	return model.AlterTableResponse{}, ls.uploader.UpdateLocalSchema(ctx, schema)
}

func (*LocalSchemaRepository) RefreshPartitions(context.Context, string, []warehouseutils.LoadFile) error {
	return nil
}

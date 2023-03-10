package schemarepository

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/uploader"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type LocalSchemaRepository struct {
	warehouse warehouseutils.Warehouse
	uploader  uploader.Uploader
}

func NewLocalSchemaRepository(wh warehouseutils.Warehouse, uploader uploader.Uploader) (*LocalSchemaRepository, error) {
	ls := LocalSchemaRepository{
		warehouse: wh,
		uploader:  uploader,
	}

	return &ls, nil
}

func (ls *LocalSchemaRepository) FetchSchema(_ warehouseutils.Warehouse) (warehouseutils.Schema, warehouseutils.Schema, error) {
	schema, err := ls.uploader.GetLocalSchema()
	if err != nil {
		return warehouseutils.Schema{}, warehouseutils.Schema{}, fmt.Errorf("fetching local schema: %w", err)
	}

	return schema, warehouseutils.Schema{}, nil
}

func (*LocalSchemaRepository) CreateSchema() (err error) {
	return nil
}

func (ls *LocalSchemaRepository) CreateTable(tableName string, columnMap warehouseutils.TableSchema) (err error) {
	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema()
	if err != nil {
		return fmt.Errorf("fetching local schema: %w", err)
	}

	if _, ok := schema[tableName]; ok {
		return fmt.Errorf("failed to create table: table %s already exists", tableName)
	}

	// add table to schema
	schema[tableName] = columnMap

	// update schema
	return ls.uploader.UpdateLocalSchema(schema)
}

func (ls *LocalSchemaRepository) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema()
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
	return ls.uploader.UpdateLocalSchema(schema)
}

func (ls *LocalSchemaRepository) AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	// fetch schema from local db
	schema, err := ls.uploader.GetLocalSchema()
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
	return model.AlterTableResponse{}, ls.uploader.UpdateLocalSchema(schema)
}

func (*LocalSchemaRepository) RefreshPartitions(_ string, _ []warehouseutils.LoadFile) error {
	return nil
}

package schemarepository

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type LocalSchemaRepository struct {
	warehouse warehouseutils.Warehouse
	uploader  warehouseutils.UploaderI
}

func NewLocalSchemaRepository(wh warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (*LocalSchemaRepository, error) {
	ls := LocalSchemaRepository{
		warehouse: wh,
		uploader:  uploader,
	}

	return &ls, nil
}

func (ls *LocalSchemaRepository) FetchSchema(_ warehouseutils.Warehouse) (warehouseutils.SchemaT, error) {
	schema := ls.uploader.GetLocalSchema()
	if schema == nil {
		schema = warehouseutils.SchemaT{}
	}
	return schema, nil
}

func (*LocalSchemaRepository) CreateSchema() (err error) {
	return nil
}

func (ls *LocalSchemaRepository) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// fetch schema from local db
	schema, err := ls.FetchSchema(ls.warehouse)
	if err != nil {
		return err
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
	schema, err := ls.FetchSchema(ls.warehouse)
	if err != nil {
		return err
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

func (ls *LocalSchemaRepository) AlterColumn(tableName, columnName, columnType string) (err error) {
	// fetch schema from local db
	schema, err := ls.FetchSchema(ls.warehouse)
	if err != nil {
		return err
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return fmt.Errorf("failed to add column: table %s does not exist", tableName)
	}

	// check if column exists
	if _, ok := schema[tableName][columnName]; !ok {
		return fmt.Errorf("failed to alter column: column %s does not exist in table %s", columnName, tableName)
	}

	schema[tableName][columnName] = columnType

	// update schema
	return ls.uploader.UpdateLocalSchema(schema)
}

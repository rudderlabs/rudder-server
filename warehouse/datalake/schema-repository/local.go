package schemarepository

import (
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type LocalSchemaRepository struct {
	warehouse warehouseutils.WarehouseT
	uploader  warehouseutils.UploaderI
}

func NewLocalSchemaRepository(wh warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (*LocalSchemaRepository, error) {
	ls := LocalSchemaRepository{
		warehouse: wh,
		uploader:  uploader,
	}

	return &ls, nil
}

func (ls *LocalSchemaRepository) FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error) {
	schema := ls.uploader.GetLocalSchema()
	if schema == nil {
		schema = warehouseutils.SchemaT{}
	}
	return schema, nil
}

func (ls *LocalSchemaRepository) CreateSchema() (err error) {
	return nil
}

func (ls *LocalSchemaRepository) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// fetch schema from local db
	schema, err := ls.FetchSchema(ls.warehouse)
	if err != nil {
		return err
	}

	if _, ok := schema[tableName]; ok {
		return fmt.Errorf("Failed to create table: table %s already exists", tableName)
	}

	// add table to schema
	schema[tableName] = columnMap

	// update schema
	return ls.uploader.UpdateLocalSchema(schema)
}

func (ls *LocalSchemaRepository) AddColumn(tableName string, columnName string, columnType string) (err error) {
	// fetch schema from local db
	schema, err := ls.FetchSchema(ls.warehouse)
	if err != nil {
		return err
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return fmt.Errorf("Failed to add column: table %s does not exist", tableName)
	}

	schema[tableName][columnName] = columnType

	// update schema
	return ls.uploader.UpdateLocalSchema(schema)
}

func (ls *LocalSchemaRepository) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	// fetch schema from local db
	schema, err := ls.FetchSchema(ls.warehouse)
	if err != nil {
		return err
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return fmt.Errorf("Failed to add column: table %s does not exist", tableName)
	}

	// check if column exists
	if _, ok := schema[tableName][columnName]; !ok {
		return fmt.Errorf("Failed to alter column: column %s does not exist in table %s", columnName, tableName)
	}

	schema[tableName][columnName] = columnType

	// update schema
	return ls.uploader.UpdateLocalSchema(schema)
}

func (ls *LocalSchemaRepository) RefreshPartitions(tableName string, loadFiles []warehouseutils.LoadFileT) error {
	return nil
}

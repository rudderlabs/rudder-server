package uploader

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Noop struct{}

func (n *Noop) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (n *Noop) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (n *Noop) UpdateLocalSchema(schema warehouseutils.SchemaT) error {
	return nil
}

func (n *Noop) GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (n *Noop) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (n *Noop) GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (n *Noop) GetSampleLoadFileLocation(tableName string) (string, error) {
	return "", nil
}

func (n *Noop) GetSingleLoadFile(tableName string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (n *Noop) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (n *Noop) UseRudderStorage() bool {
	return false
}

func (n *Noop) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (n *Noop) GetLoadFileType() string {
	return ""
}

func (n *Noop) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Time{}, time.Time{}
}

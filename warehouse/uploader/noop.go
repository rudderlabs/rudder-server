package uploader

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Noop struct{}

func (*Noop) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*Noop) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*Noop) UpdateLocalSchema(warehouseutils.SchemaT) error {
	return nil
}

func (*Noop) GetTableSchemaInWarehouse(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*Noop) GetTableSchemaInUpload(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*Noop) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (*Noop) GetSampleLoadFileLocation(string) (string, error) {
	return "", nil
}

func (*Noop) GetSingleLoadFile(string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (*Noop) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (*Noop) UseRudderStorage() bool {
	return false
}

func (*Noop) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (*Noop) GetLoadFileType() string {
	return ""
}

func (*Noop) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Time{}, time.Time{}
}

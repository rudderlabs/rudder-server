package encoding

import (
	"encoding/json"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

const LOADED_AT_COLUMN = "loaded_at"

// JsonLoader is only for BQ now. Treat this is as custom BQ loader.
// If more warehouses are added in the future, change this accordingly.
type JsonLoader struct {
	destType   string
	columnData map[string]interface{}
	fileWriter warehouseutils.LoadFileWriterI
}

func NewJSONLoader(destType string, writer warehouseutils.LoadFileWriterI) *JsonLoader {
	loader := &JsonLoader{destType: destType, fileWriter: writer}
	loader.columnData = make(map[string]interface{})
	return loader
}

func (loader *JsonLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUID_TS_COLUMN) || columnName == warehouseutils.ToProviderCase(loader.destType, LOADED_AT_COLUMN)
}

func (loader *JsonLoader) GetLoadTimeFormat(columnName string) string {
	switch columnName {
	case warehouseutils.ToProviderCase(loader.destType, UUID_TS_COLUMN):
		return warehouseutils.BQUuidTSFormat
	case warehouseutils.ToProviderCase(loader.destType, LOADED_AT_COLUMN):
		return warehouseutils.BQLoadedAtFormat
	}
	return ""
}

func (loader *JsonLoader) AddColumn(columnName, _ string, val interface{}) {
	providerColumnName := warehouseutils.ToProviderCase(loader.destType, columnName)
	loader.columnData[providerColumnName] = val
}

func (loader *JsonLoader) AddRow(columnNames, row []string) {
	for i, columnName := range columnNames {
		providerColumnName := warehouseutils.ToProviderCase(loader.destType, columnName)
		loader.columnData[providerColumnName] = row[i]
	}
}

func (loader *JsonLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", nil)
}

func (loader *JsonLoader) WriteToString() (string, error) {
	jsonData, err := json.Marshal(loader.columnData)
	if err != nil {
		pkgLogger.Errorf(`[JSONWriter]: Error writing discardRow to buffer: %v`, err)
		return "", err
	}
	return string(jsonData) + "\n", nil
}

func (loader *JsonLoader) Write() error {
	eventData, err := loader.WriteToString()
	if err != nil {
		return err
	}

	return loader.fileWriter.WriteGZ(eventData)
}

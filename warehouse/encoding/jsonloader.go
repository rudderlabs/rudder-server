package encoding

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type JsonLoader struct {
	destType   string
	columnData map[string]interface{}
	fileWriter LoadFileWriter
}

// NewJSONLoader returns a new JsonLoader
// JsonLoader is only for BQ now. Treat this is as custom BQ loader.
// If more warehouses are added in the future, change this accordingly.
func NewJSONLoader(destType string, writer LoadFileWriter) *JsonLoader {
	loader := &JsonLoader{destType: destType, fileWriter: writer}
	loader.columnData = make(map[string]interface{})
	return loader
}

func (loader *JsonLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn) || columnName == warehouseutils.ToProviderCase(loader.destType, LoadedAtColumn)
}

func (loader *JsonLoader) GetLoadTimeFormat(columnName string) string {
	switch columnName {
	case warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn):
		return BQUuidTSFormat
	case warehouseutils.ToProviderCase(loader.destType, LoadedAtColumn):
		return BQLoadedAtFormat
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
	var (
		jsonData []byte
		err      error
	)

	if jsonData, err = json.Marshal(loader.columnData); err != nil {
		return "", fmt.Errorf("json.Marshal: %w", err)
	}
	return string(jsonData) + "\n", nil
}

func (loader *JsonLoader) Write() error {
	var (
		eventData string
		err       error
	)

	if eventData, err = loader.WriteToString(); err != nil {
		return fmt.Errorf("writeToString: %w", err)
	}
	return loader.fileWriter.WriteGZ(eventData)
}

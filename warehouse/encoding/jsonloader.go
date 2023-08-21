package encoding

import (
	"encoding/json"
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type jsonLoader struct {
	destType   string
	columnData map[string]interface{}
	fileWriter LoadFileWriter
}

// newJSONLoader returns a new jsonLoader is only for BQ now. Treat this is as custom BQ loader.
// If more warehouses are added in the future, change this accordingly.
func newJSONLoader(writer LoadFileWriter, destType string) *jsonLoader {
	loader := &jsonLoader{destType: destType, fileWriter: writer}
	loader.columnData = make(map[string]interface{})

	return loader
}

func (loader *jsonLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn) ||
		columnName == warehouseutils.ToProviderCase(loader.destType, LoadedAtColumn)
}

func (loader *jsonLoader) GetLoadTimeFormat(columnName string) string {
	switch columnName {
	case warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn):
		return BQUuidTSFormat
	case warehouseutils.ToProviderCase(loader.destType, LoadedAtColumn):
		return BQLoadedAtFormat
	default:
		return ""
	}
}

func (loader *jsonLoader) AddColumn(columnName, _ string, val interface{}) {
	providerColumnName := warehouseutils.ToProviderCase(loader.destType, columnName)
	loader.columnData[providerColumnName] = val
}

func (loader *jsonLoader) AddRow(columnNames, row []string) {
	for i, columnName := range columnNames {
		providerColumnName := warehouseutils.ToProviderCase(loader.destType, columnName)
		loader.columnData[providerColumnName] = row[i]
	}
}

func (loader *jsonLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", nil)
}

func (loader *jsonLoader) WriteToString() (string, error) {
	jsonData, err := json.Marshal(loader.columnData)
	if err != nil {
		return "", fmt.Errorf("json.Marshal: %w", err)
	}
	return string(jsonData) + "\n", nil
}

func (loader *jsonLoader) Write() error {
	eventData, err := loader.WriteToString()
	if err != nil {
		return fmt.Errorf("writeToString: %w", err)
	}
	return loader.fileWriter.WriteGZ(eventData)
}

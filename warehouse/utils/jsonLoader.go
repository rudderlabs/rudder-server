package warehouseutils

import (
	"encoding/json"
)

const LOADED_AT_COLUMN = "loaded_at"

// jsonLoader is only for BQ now. Treat this is as custom BQ loader.
// If more warehouses are added in future, change this accordingly.
type jsonLoader struct {
	destType   string
	columnData map[string]interface{}
}

func NewJSONLoader(destType string) *jsonLoader {
	loader := &jsonLoader{destType: destType}
	loader.columnData = make(map[string]interface{})
	return loader
}

func (loader *jsonLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == ToProviderCase(loader.destType, UUID_TS_COLUMN) || columnName == ToProviderCase(loader.destType, LOADED_AT_COLUMN)
}
func (loader *jsonLoader) GetLoadTimeFomat(columnName string) string {

	switch columnName {
	case ToProviderCase(loader.destType, UUID_TS_COLUMN):
		return BQUuidTSFormat
	case ToProviderCase(loader.destType, LOADED_AT_COLUMN):
		return BQLoadedAtFormat
	}
	return ""
}

func (loader *jsonLoader) AddColumn(columnName string, val interface{}) {
	providerColumnName := ToProviderCase(loader.destType, columnName)
	loader.columnData[providerColumnName] = val
}

func (loader *jsonLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, nil)
}

func (loader *jsonLoader) WriteToString() (string, error) {
	jsonData, err := json.Marshal(loader.columnData)
	if err != nil {
		pkgLogger.Errorf(`[JSONWriter]: Error writing discardRow to buffer: %w`, err)
		return "", err
	}
	return string(jsonData) + "\n", nil
}

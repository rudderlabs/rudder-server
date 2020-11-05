package warehouseutils

import (
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// csvLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type csvLoader struct {
	destType  string
	csvRow    []string
	buff      bytes.Buffer
	csvWriter *csv.Writer
}

func NewCSVLoader(destType string) *csvLoader {
	loader := &csvLoader{destType: destType}
	loader.csvRow = []string{}
	loader.buff = bytes.Buffer{}
	loader.csvWriter = csv.NewWriter(&loader.buff)
	return loader
}

func (loader *csvLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == ToProviderCase(loader.destType, UUID_TS_COLUMN)
}

func (loader *csvLoader) GetLoadTimeFomat(columnName string) string {
	return misc.RFC3339Milli
}

func (loader *csvLoader) AddColumn(columnName string, val interface{}) {
	valString := fmt.Sprintf("%v", val)
	loader.csvRow = append(loader.csvRow, valString)
}

func (loader *csvLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "")
}
func (loader *csvLoader) WriteToString() (string, error) {
	err := loader.csvWriter.Write(loader.csvRow)
	if err != nil {
		logger.Errorf(`[CSVWriter]: Error writing discardRow to buffer: %w`, err)
		return "", err
	}
	loader.csvWriter.Flush()
	return loader.buff.String(), nil
}

package warehouseutils

import (
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// CsvLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type CsvLoader struct {
	destType  string
	csvRow    []string
	buff      bytes.Buffer
	csvWriter *csv.Writer
}

func NewCSVLoader(destType string) *CsvLoader {
	loader := &CsvLoader{destType: destType}
	loader.csvRow = []string{}
	loader.buff = bytes.Buffer{}
	loader.csvWriter = csv.NewWriter(&loader.buff)
	return loader
}

func (loader *CsvLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == ToProviderCase(loader.destType, UUID_TS_COLUMN)
}

func (loader *CsvLoader) GetLoadTimeFomat(columnName string) string {
	return misc.RFC3339Milli
}

func (loader *CsvLoader) AddColumn(columnName string, val interface{}) {
	valString := fmt.Sprintf("%v", val)
	loader.csvRow = append(loader.csvRow, valString)
}

func (loader *CsvLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "")
}
func (loader *CsvLoader) WriteToString() (string, error) {
	err := loader.csvWriter.Write(loader.csvRow)
	if err != nil {
		pkgLogger.Errorf(`[CSVWriter]: Error writing discardRow to buffer: %v`, err)
		return "", err
	}
	loader.csvWriter.Flush()
	return loader.buff.String(), nil
}

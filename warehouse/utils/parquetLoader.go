package warehouseutils

import (
	"bytes"
	"encoding/csv"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// ParquetLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type ParquetLoader struct {
	destType  string
	csvRow    []string
	buff      bytes.Buffer
	csvWriter *csv.Writer
}

func NewParquetLoader(destType string) *ParquetLoader {
	loader := &ParquetLoader{destType: destType}
	loader.csvRow = []string{}
	loader.buff = bytes.Buffer{}
	loader.csvWriter = csv.NewWriter(&loader.buff)
	return loader
}

func (loader *ParquetLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == ToProviderCase(loader.destType, UUID_TS_COLUMN)
}

func (loader *ParquetLoader) GetLoadTimeFomat(columnName string) string {
	return misc.RFC3339Milli
}

func (loader *ParquetLoader) AddColumn(columnName string, val interface{}) {

}

func (loader *ParquetLoader) AddRow(columnNames []string, row []string) {

}

func (loader *ParquetLoader) AddEmptyColumn(columnName string) {

}
func (loader *ParquetLoader) WriteToString() (string, error) {
	err := loader.csvWriter.Write(loader.csvRow)
	if err != nil {
		pkgLogger.Errorf(`[CSVWriter]: Error writing discardRow to buffer: %v`, err)
		return "", err
	}
	loader.csvWriter.Flush()
	return loader.buff.String(), nil
}

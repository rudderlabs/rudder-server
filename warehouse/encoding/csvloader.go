package encoding

import (
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// CsvLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type CsvLoader struct {
	destType   string
	csvRow     []string
	buff       bytes.Buffer
	csvWriter  *csv.Writer
	fileWriter LoadFileWriter
}

func NewCSVLoader(destType string, writer LoadFileWriter) *CsvLoader {
	loader := &CsvLoader{destType: destType, fileWriter: writer}
	loader.csvRow = []string{}
	loader.buff = bytes.Buffer{}
	loader.csvWriter = csv.NewWriter(&loader.buff)
	return loader
}

func (loader *CsvLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn)
}

func (*CsvLoader) GetLoadTimeFormat(string) string {
	return misc.RFC3339Milli
}

func (loader *CsvLoader) AddColumn(_, _ string, val interface{}) {
	valString := fmt.Sprintf("%v", val)
	loader.csvRow = append(loader.csvRow, valString)
}

func (loader *CsvLoader) AddRow(_, row []string) {
	loader.csvRow = append(loader.csvRow, row...)
}

func (loader *CsvLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", "")
}

func (loader *CsvLoader) WriteToString() (string, error) {
	err := loader.csvWriter.Write(loader.csvRow)
	if err != nil {
		return "", fmt.Errorf("csvWriter write: %w", err)
	}

	loader.csvWriter.Flush()
	return loader.buff.String(), nil
}

func (loader *CsvLoader) Write() error {
	eventData, err := loader.WriteToString()
	if err != nil {
		return fmt.Errorf("writing to string: %w", err)
	}

	return loader.fileWriter.WriteGZ(eventData)
}

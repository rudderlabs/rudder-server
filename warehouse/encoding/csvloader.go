package encoding

import (
	"bytes"
	"encoding/csv"
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// csvLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type csvLoader struct {
	destType   string
	csvRow     []string
	buff       bytes.Buffer
	csvWriter  *csv.Writer
	fileWriter LoadFileWriter
}

func newCSVLoader(writer LoadFileWriter, destType string) *csvLoader {
	loader := &csvLoader{destType: destType, fileWriter: writer}
	loader.csvRow = []string{}
	loader.buff = bytes.Buffer{}
	loader.csvWriter = csv.NewWriter(&loader.buff)
	return loader
}

func (loader *csvLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn)
}

func (*csvLoader) GetLoadTimeFormat(string) string {
	return misc.RFC3339Milli
}

func (loader *csvLoader) AddColumn(_, _ string, val interface{}) {
	valString := fmt.Sprintf("%v", val)
	loader.csvRow = append(loader.csvRow, valString)
}

func (loader *csvLoader) AddRow(_, row []string) {
	loader.csvRow = append(loader.csvRow, row...)
}

func (loader *csvLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", "")
}

func (loader *csvLoader) WriteToString() (string, error) {
	err := loader.csvWriter.Write(loader.csvRow)
	if err != nil {
		return "", fmt.Errorf("csvWriter write: %w", err)
	}

	loader.csvWriter.Flush()

	return loader.buff.String(), nil
}

func (loader *csvLoader) Write() error {
	eventData, err := loader.WriteToString()
	if err != nil {
		return fmt.Errorf("writing to string: %w", err)
	}

	return loader.fileWriter.WriteGZ(eventData)
}

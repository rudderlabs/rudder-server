package encoding

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type jsonLoader struct {
	destType      string
	partitionMode string
	columnData    map[string]any
	fileWriter    LoadFileWriter
}

// newJSONLoader writes one JSON object per row for BQ and MaxCompute.
func newJSONLoader(writer LoadFileWriter, destType, partitionMode string) *jsonLoader {
	loader := &jsonLoader{destType: destType, partitionMode: partitionMode, fileWriter: writer}
	loader.columnData = make(map[string]any)

	return loader
}

func (loader *jsonLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn) ||
		columnName == warehouseutils.ToProviderCase(loader.destType, LoadedAtColumn) ||
		(loader.destType == MaxCompute && loader.partitionMode == FixedPartitionMode && columnName == PartitionTimeColumn)
}

func (loader *jsonLoader) GetLoadTimeFormat(columnName string) string {
	if loader.destType == MaxCompute {
		switch columnName {
		case UUIDTsColumn, LoadedAtColumn, PartitionTimeColumn:
			return MaxComputeTimeFormat
		}
	}
	switch columnName {
	case warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn):
		return BQUuidTSFormat
	case warehouseutils.ToProviderCase(loader.destType, LoadedAtColumn):
		return BQLoadedAtFormat
	default:
		return ""
	}
}

func (loader *jsonLoader) AddColumn(columnName, _ string, val any) {
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
	jsonData, err := jsonrs.Marshal(loader.columnData)
	if err != nil {
		return "", fmt.Errorf("jsonrs.Marshal: %w", err)
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

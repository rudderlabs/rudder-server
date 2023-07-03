package encoding

import warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFormat(columnName string) string
	AddColumn(columnName, columnType string, val interface{})
	AddRow(columnNames, values []string)
	AddEmptyColumn(columnName string)
	WriteToString() (string, error)
	Write() error
}

func GetNewEventLoader(destinationType, loadFileType string, w LoadFileWriter) EventLoader {
	switch loadFileType {
	case warehouseutils.LoadFileTypeJson:
		return NewJSONLoader(destinationType, w)
	case warehouseutils.LoadFileTypeParquet:
		return NewParquetLoader(destinationType, w)
	default:
		return NewCSVLoader(destinationType, w)
	}
}

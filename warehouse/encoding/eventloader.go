package encoding

import "github.com/rudderlabs/rudder-server/warehouse/utils"

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
	case warehouseutils.LOAD_FILE_TYPE_JSON:
		return NewJSONLoader(destinationType, w)
	case warehouseutils.LOAD_FILE_TYPE_PARQUET:
		return NewParquetLoader(destinationType, w)
	default:
		return NewCSVLoader(destinationType, w)
	}
}

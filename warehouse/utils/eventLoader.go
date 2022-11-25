package warehouseutils

const UUID_TS_COLUMN = "uuid_ts"

type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFormat(columnName string) string
	AddColumn(columnName, columnType string, val interface{})
	AddRow(columnNames, values []string)
	AddEmptyColumn(columnName string)
	WriteToString() (string, error)
	Write() error
}

func GetNewEventLoader(destinationType, loadFileType string, w LoadFileWriterI) EventLoader {
	switch loadFileType {
	case LOAD_FILE_TYPE_JSON:
		return NewJSONLoader(destinationType, w)
	case LOAD_FILE_TYPE_PARQUET:
		return NewParquetLoader(destinationType, w)
	default:
		return NewCSVLoader(destinationType, w)
	}
}

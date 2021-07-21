package warehouseutils

const UUID_TS_COLUMN = "uuid_ts"

type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFomat(columnName string) string
	AddColumn(columnName string, columnType string, val interface{})
	AddRow(columnNames []string, values []string)
	AddEmptyColumn(columnName string)
	WriteToString() (string, error)
	Write() error
}

// TODO : use the load file type from payloadT here - identity resolution does not have access to payloadT
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

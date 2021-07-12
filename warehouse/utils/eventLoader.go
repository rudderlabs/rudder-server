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

// type Writer interface {
// 	Write(p []byte) (n int, err error)
// 	WriteRow(p []interface{}) error
// 	WriteString(p string) error
// 	Close() error
// 	GetLoadFile() *os.File
// }

func GetNewEventLoader(destinationType string, w LoadFileWriterI) EventLoader {
	// if destinationType == "BQ" {
	// 	return NewJSONLoader(destinationType)
	// }
	// return NewCSVLoader(destinationType)
	return NewParquetLoader(destinationType, w)
}

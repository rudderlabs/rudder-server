package warehouseutils

const UUID_TS_COLUMN = "uuid_ts"

type EventLoader interface {
	IsLoadTimeColumn(columnName string) bool
	GetLoadTimeFomat(columnName string) string
	AddColumn(columnName string, val interface{})
	AddRow(columnNames []string, values []string)
	AddEmptyColumn(columnName string)
	WriteToString() (string, error)
}

func GetNewEventLoader(destinationType string) EventLoader {
	if destinationType == "BQ" {
		return NewJSONLoader(destinationType)
	}
	return NewCSVLoader(destinationType)
}

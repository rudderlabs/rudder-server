package warehouseutils

import "io"

type eventReader interface {
	Read(columnNames []string) (record []string, err error)
}

func NewEventReader(r io.Reader, provider string) eventReader {
	if provider == "BQ" {
		return NewJSONReader(r)
	}
	return NewCsvReader(r)
}

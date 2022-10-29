package warehouseutils

import "io"

type EventReader interface {
	Read(columnNames []string) (record []string, err error)
}

func NewEventReader(r io.Reader, provider string) EventReader {
	if provider == BQ {
		return NewJSONReader(r)
	}
	return NewCsvReader(r)
}

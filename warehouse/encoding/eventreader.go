package encoding

import (
	"io"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type EventReader interface {
	Read(columnNames []string) (record []string, err error)
}

func NewEventReader(r io.Reader, provider string) EventReader {
	if provider == warehouseutils.BQ {
		return NewJSONReader(r)
	}
	return NewCsvReader(r)
}

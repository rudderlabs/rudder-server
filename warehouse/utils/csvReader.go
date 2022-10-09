package warehouseutils

import (
	"encoding/csv"
	"io"
)

type CsvReader struct {
	reader *csv.Reader
}

func (csv *CsvReader) Read(_ []string) (record []string, err error) {
	record, err = csv.reader.Read()
	return
}

func NewCsvReader(r io.Reader) *CsvReader {
	return &CsvReader{reader: csv.NewReader(r)}
}

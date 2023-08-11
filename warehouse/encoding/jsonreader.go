package encoding

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

type jsonReader struct {
	scanner *bufio.Scanner
}

func (js *jsonReader) Read(columnNames []string) ([]string, error) {
	ok := js.scanner.Scan()
	if !ok {
		if err := js.scanner.Err(); err != nil {
			return []string{}, fmt.Errorf("scanner scan: %w", err)
		}
		return []string{}, io.EOF
	}

	lineBytes := js.scanner.Bytes()
	jsonData := make(map[string]interface{})

	err := json.Unmarshal(lineBytes, &jsonData)
	if err != nil {
		return []string{}, fmt.Errorf("json unmarshal: %w", err)
	}

	var record []string
	for _, columnName := range columnNames {
		record = append(record, fmt.Sprintf("%v", jsonData[columnName]))
	}

	return record, nil
}

// newJSONReader returns a new JSON reader
// default scanner buffer maxCapacity is 64K
// set it to higher value to avoid read stop on read size error
func newJSONReader(r io.Reader, bufferCapacityInK int) *jsonReader {
	maxCapacity := bufferCapacityInK * 1024

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, maxCapacity), maxCapacity)

	return &jsonReader{scanner: scanner}
}

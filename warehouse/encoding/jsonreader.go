package encoding

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

type JsonReader struct {
	scanner *bufio.Scanner
}

func (js *JsonReader) Read(columnNames []string) ([]string, error) {
	var (
		record []string
		err    error
	)

	ok := js.scanner.Scan()
	if !ok {
		err = js.scanner.Err()
		if err != nil {
			return []string{}, fmt.Errorf("scanner scan: %w", err)
		}
		return []string{}, io.EOF
	}

	lineBytes := js.scanner.Bytes()
	jsonData := make(map[string]string)

	err = json.Unmarshal(lineBytes, &jsonData)
	if err != nil {
		return []string{}, fmt.Errorf("json unmarshal: %w", err)
	}

	for _, columnName := range columnNames {
		record = append(record, jsonData[columnName])
	}
	return record, nil
}

// NewJSONReader returns a new JSON reader
// default scanner buffer maxCapacity is 64K
// set it to higher value to avoid read stop on read size error
func NewJSONReader(r io.Reader) *JsonReader {
	scanner := bufio.NewScanner(r)
	maxCapacity := maxStagingFileReadBufferCapacityInK * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	return &JsonReader{scanner: scanner}
}

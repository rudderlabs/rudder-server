package warehouseutils

import (
	"bufio"
	"encoding/json"
	"io"

	// "github.com/rudderlabs/rudder-server/config"
)

type jsonReader struct {
	scanner *bufio.Scanner
}

var maxStagingFileReadBufferCapacityInK int

func (js *jsonReader) Read(columnNames []string) (record []string, err error) {
	ok := js.scanner.Scan()
	if !ok {
		err = js.scanner.Err()
		if err != nil {
			pkgLogger.Errorf("WH: Error in scanner reading line from staging file: %v", err) //TODO: change log
			return
		}
		return []string{}, io.EOF

	}

	lineBytes := js.scanner.Bytes()
	jsonData := make(map[string]string)
	err = json.Unmarshal(lineBytes, &jsonData)
	if err != nil {
		return
	}
	for _, columnName := range columnNames {
		record = append(record, jsonData[columnName])
	}
	return
}

func NewJSONReader(r io.Reader) *jsonReader {
	scanner := bufio.NewScanner(r)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := maxStagingFileReadBufferCapacityInK * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	return &jsonReader{scanner: scanner}
}

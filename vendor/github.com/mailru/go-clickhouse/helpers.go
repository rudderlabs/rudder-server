package clickhouse

import (
	"bytes"
	"net/http"
	"strings"
	"time"
)

var (
	escaper    = strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	dateFormat = "2006-01-02"
	timeFormat = "2006-01-02 15:04:05"
)

func escape(s string) string {
	return escaper.Replace(s)
}

func quote(s string) string {
	return "'" + s + "'"
}

func formatTime(value time.Time) string {
	return quote(value.Format(timeFormat))
}

func formatDate(value time.Time) string {
	return quote(value.Format(dateFormat))
}

func readResponse(response *http.Response) (result []byte, err error) {
	if response.ContentLength > 0 {
		result = make([]byte, 0, response.ContentLength)
	}
	buf := bytes.NewBuffer(result)
	defer response.Body.Close()
	_, err = buf.ReadFrom(response.Body)
	result = buf.Bytes()
	return
}

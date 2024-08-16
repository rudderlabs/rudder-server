package lyticsBulkUpload

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type LyticsBulkUploader struct {
	destName      string
	logger        logger.Logger
	authorization string
	baseEndpoint  string
	fileSizeLimit int64
	jobToCSVMap   map[int64]int64
}

type DestinationConfig struct {
	LyticsAccountId          string                   `json:"lyticsAccountId"`
	LyticsApiKey             string                   `json:"lyticsApiKey"`
	LyticsStreamName         string                   `json:"lyticsStreamName"`
	TimestampField           string                   `json:"timestampField"`
	OneTrustCookieCategories []OneTrustCookieCategory `json:"oneTrustCookieCategories"`
	StreamTraitsMapping      []StreamTraitMapping     `json:"streamTraitsMapping"`
}

type OneTrustCookieCategory struct {
	OneTrustCookieCategory string `json:"oneTrustCookieCategory"`
}

type StreamTraitMapping struct {
	RudderProperty string `json:"rudderProperty"`
	LyticsProperty string `json:"lyticsProperty"`
}

type HttpRequestData struct {
	Body          io.Reader
	Authorization string
	Endpoint      string
	ContentType   string
}

type ActionFileInfo struct {
	Action           string
	CSVWriter        *csv.Writer
	CSVFilePath      string
	SuccessfulJobIDs []int64
	FailedJobIDs     []int64
	FileSize         int64
	EventCount       int64
	File             *os.File
}

type Message struct {
	Action   string          `json:"action"`
	Type     string          `json:"type"`
	Channel  string          `json:"channel"`
	Fields   json.RawMessage `json:"fields"`
	RecordId string          `json:"recordId"`
	Context  json.RawMessage `json:"context"`
}
type Metadata struct {
	JobID int64 `json:"job_id"`
}

type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

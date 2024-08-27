package lyticsBulkUpload

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
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
	Method        string
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
	UserID      string          `json:"userId"`
	AnonymousID string          `json:"anonymousId"`
	Event       string          `json:"event"`
	Properties  json.RawMessage `json:"properties"`
	Traits      json.RawMessage `json:"traits"`
	Context     json.RawMessage `json:"context"`
	Timestamp   time.Time       `json:"timestamp"`
}

type Metadata struct {
	JobID int64 `json:"job_id"`
}

type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

type Uploader interface {
	Upload(*common.AsyncDestinationStruct) common.AsyncUploadOutput
}

type Poller interface {
	Poll(input common.AsyncPoll) common.PollStatusResponse
}

type UploadStats interface {
	GetUploadStats(common.GetUploadStatsInput) common.GetUploadStatsResponse
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

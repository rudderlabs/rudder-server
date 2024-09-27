package lyticsBulkUpload

import (
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type LyticsBulkUploader struct {
	destName      string
	logger        logger.Logger
	statsFactory  stats.Stats
	authorization string
	baseEndpoint  string
	fileSizeLimit int64
	jobToCSVMap   map[int64]int64
	service       LyticsService
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
	UserID      string              `json:"userId"`
	AnonymousID string              `json:"anonymousId"`
	Event       string              `json:"event"`
	Properties  jsoniter.RawMessage `json:"properties"`
	Traits      jsoniter.RawMessage `json:"traits"`
	Context     jsoniter.RawMessage `json:"context"`
	Timestamp   time.Time           `json:"timestamp"`
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
	UploadBulkFile(ctx context.Context, filePath string) (bool, error)
	PopulateCsvFile(actionFile *ActionFileInfo, streamTraitsMapping []StreamTraitMapping, line string, data Data) error
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type LyticsService interface {
	UploadBulkFile(data *HttpRequestData, filePath string) error
	MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error)
}

package clevertapSegment

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type ClevertapBulkUploader struct {
	destName                  string
	logger                    logger.Logger
	statsFactory              stats.Stats
	appKey                    string
	accessToken               string
	presignedURLEndpoint      string
	notifyEndpoint            string
	fileSizeLimit             int64
	jobToCSVMap               map[int64]int64
	service                   ClevertapService
	clevertapConnectionConfig *ConnectionConfig
}

type DestinationConfig struct {
	AppKey                   string                   `json:"appKey"`
	AccessToken              string                   `json:"accessToken"`
	Region                   string                   `json:"region"`
	OneTrustCookieCategories []OneTrustCookieCategory `json:"oneTrustCookieCategories"`
}

type OneTrustCookieCategory struct {
	OneTrustCookieCategory string `json:"oneTrustCookieCategory"`
}

type HttpRequestData struct {
	Body        io.Reader
	appKey      string
	accessToken string
	Endpoint    string
	ContentType string
	Method      string
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

const DEFAULT_SENDER_NAME = "Rudderstack"

type Destination struct {
	SchemaVersion string `json:"schemaVersion"`
	SegmentName   string `json:"segmentName"`
	AdminEmail    string `json:"adminEmail"`
	SenderName    string `json:"senderName"`
}

type ConnConfig struct {
	Destination Destination `json:"destination"`
}

type ConnectionConfig struct {
	SourceId      string     `json:"sourceId"`
	DestinationId string     `json:"destinationId"`
	Enabled       bool       `json:"enabled"`
	Config        ConnConfig `json:"config"`
}

type Uploader interface {
	Upload(*common.AsyncDestinationStruct) common.AsyncUploadOutput
	PopulateCsvFile(actionFile *ActionFileInfo, line string, data Data) error
	convertToConnectionConfig(conn *backendconfig.Connection) (*ConnectionConfig, error)
	getPresignedS3URL(string, string, ClevertapService) (string, error)
	namingSegment(presignedURL, csvFilePath, appKey, accessToken string) error
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type ClevertapService interface {
	UploadBulkFile(filePath, presignedURL string) error
	MakeHTTPRequest(data *HttpRequestData) ([]byte, int, error)
}

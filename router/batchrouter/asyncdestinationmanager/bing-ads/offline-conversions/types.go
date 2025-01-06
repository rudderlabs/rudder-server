package offline_conversions

import (
	"encoding/csv"
	"encoding/json"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type BingAdsBulkUploader struct {
	destName       string
	service        bingads.BulkServiceI
	logger         logger.Logger
	statsFactory   stats.Stats
	fileSizeLimit  int64
	eventsLimit    int64
	isHashRequired bool
}
type Message struct {
	Fields json.RawMessage `json:"fields"`
	Action string          `json:"action"`
}
type Metadata struct {
	JobID int64 `json:"jobId"`
}

// This struct represent each line of the text file created by the batchrouter
type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

type DestinationConfig struct {
	CustomerAccountID string `json:"customerAccountId"`
	CustomerID        string `json:"customerId"`
	RudderAccountID   string `json:"rudderAccountId"`
	IsHashRequired    bool   `json:"isHashRequired"`
}

type ActionFileInfo struct {
	Action           string
	CSVWriter        *csv.Writer
	CSVFilePath      string
	ZipFilePath      string
	SuccessfulJobIDs []int64
	FailedJobIDs     []int64
	FileSize         int64
	EventCount       int64
}

var actionTypes = [3]string{"update", "insert", "delete"}

const commaSeparator = ","

type Record struct {
	Action   string          `json:"action"`
	Type     string          `json:"type"`
	Channel  string          `json:"channel"`
	Fields   json.RawMessage `json:"fields"`
	RecordId string          `json:"recordId"`
	Context  json.RawMessage `json:"context"`
}

type RecordFields struct {
	ConversionCurrencyCode    string `json:"conversionCurrencyCode"`
	ConversionValue           string `json:"conversionValue"`
	ConversionName            string `json:"conversionName"`
	ConversionTime            string `json:"conversionTime"`
	Email                     string `json:"email"`
	Phone                     string `json:"phone"`
	MicrosoftClickId          string `json:"microsoftClickId"`
	ConversionAdjustedTime    string `json:"conversionAdjustedTime"`
	ExternalAttributionCredit string `json:"externalAttributionCredit"`
	ExternalAttributionModel  string `json:"externalAttributionModel"`
}

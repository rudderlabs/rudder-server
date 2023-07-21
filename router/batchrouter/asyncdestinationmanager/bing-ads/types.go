package bingads

import (
	"encoding/csv"
	"net/http"

	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type Client struct {
	URL    string
	client *http.Client
}
type BingAdsBulkUploader struct {
	destName      string
	service       bingads.BulkServiceI
	logger        logger.Logger
	client        Client
	fileSizeLimit int64
	eventsLimit   int64
}
type User struct {
	Email       string `json:"email"`
	HashedEmail string `json:"hashedEmail"`
}
type Message struct {
	List   []User `json:"List"`
	Action string `json:"Action"`
}
type Metadata struct {
	JobID int64 `json:"job_id"`
}

// This struct represent each line of the text file created by the batchrouter
type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

type DestinationConfig struct {
	AudienceID               string   `json:"audienceId"`
	CustomerAccountID        string   `json:"customerAccountId"`
	CustomerID               string   `json:"customerId"`
	OneTrustCookieCategories []string `json:"oneTrustCookieCategories"`
	RudderAccountID          string   `json:"rudderAccountId"`
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

var actionTypes = [3]string{"Replace", "Remove", "Add"}

const commaSeparator = ","

const clientIDSeparator = "<<>>"

type ClientID struct {
	JobID       int64
	HashedEmail string
}

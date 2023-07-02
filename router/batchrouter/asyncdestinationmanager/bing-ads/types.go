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
	destName string
	service  bingads.BulkServiceI
	logger   logger.Logger
	client   Client
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
	AudienceId               string   `json:"audienceId"`
	CustomerAccountId        string   `json:"customerAccountId"`
	CustomerId               string   `json:"customerId"`
	OneTrustCookieCategories []string `json:"oneTrustCookieCategories"`
	RudderAccountId          string   `json:"rudderAccountId"`
}

type ActionFileInfo struct {
	Action           string
	CSVWriter        *csv.Writer
	CSVFilePath      string
	ZipFilePath      string
	SuccessfulJobIDs []int64
	FailedJobIDs     []int64
	FileSize         int
	EventCount       int
}

var actionTypes = [3]string{"Add", "Remove", "Update"}

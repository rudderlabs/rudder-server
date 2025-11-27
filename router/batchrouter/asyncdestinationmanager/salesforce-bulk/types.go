package salesforcebulk

import (
	"net/http"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

type DestinationConfig struct {
	RudderAccountID string `json:"rudderAccountId"`
	Operation       string `json:"operation"`
	ObjectType      string `json:"objectType"`
	APIVersion      string `json:"apiVersion"`
}

type SalesforceBulkUploader struct {
	destName        string
	logger          logger.Logger
	statsFactory    stats.Stats
	apiService      SalesforceAPIServiceInterface
	dataHashToJobID map[string][]int64
	hashMapMutex    sync.RWMutex
	destinationInfo *oauthv2.DestinationInfo
}

type SalesforceAPIServiceInterface interface {
	CreateJob(objectName, operation, externalIDField string) (string, *APIError)
	UploadData(jobID, csvFilePath string) *APIError
	CloseJob(jobID string) *APIError
	GetJobStatus(jobID string) (*JobResponse, *APIError)
	GetFailedRecords(jobID string) ([]map[string]string, *APIError)
	GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError)
	DeleteJob(jobID string) *APIError
}

type SalesforceAPIService struct {
	logger          logger.Logger
	destinationInfo *oauthv2.DestinationInfo
	apiVersion      string
	client          *http.Client
}

type SalesforceJobInfo struct {
	ID      string   `json:"id"`
	Headers []string `json:"headers"`
}

type JobResponse struct {
	ID                     string  `json:"id"`
	State                  string  `json:"state"`
	Object                 string  `json:"object"`
	Operation              string  `json:"operation"`
	CreatedDate            string  `json:"createdDate"`
	SystemModstamp         string  `json:"systemModstamp"`
	NumberRecordsProcessed int     `json:"numberRecordsProcessed"`
	NumberRecordsFailed    int     `json:"numberRecordsFailed"`
	TotalProcessingTime    int     `json:"totalProcessingTime"`
	APIVersion             float64 `json:"apiVersion"`
	ErrorMessage           string  `json:"errorMessage,omitempty"`
}

type JobCreateRequest struct {
	Object              string `json:"object"`
	ContentType         string `json:"contentType"`
	Operation           string `json:"operation"`
	LineEnding          string `json:"lineEnding"`
	ExternalIDFieldName string `json:"externalIdFieldName,omitempty"`
}

type APIError struct {
	StatusCode int
	Message    string
	Category   string
}

const (
	destName = "SALESFORCE_BULK_UPLOAD"
)

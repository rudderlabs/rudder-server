package salesforcebulk

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

// DestinationConfig holds Salesforce Bulk Upload configuration
// For RETL use case, field mapping comes from VDM, not config
type DestinationConfig struct {
	RudderAccountID string `json:"rudderAccountId"` // OAuth account ID from Control Plane
	Operation       string `json:"operation"`       // insert, update, upsert, delete
	APIVersion      string `json:"apiVersion"`      // Salesforce API version (default: v57.0)
}

// SalesforceBulkUploader implements AsyncDestinationManager interface
type SalesforceBulkUploader struct {
	destName        string
	config          DestinationConfig
	logger          logger.Logger
	statsFactory    stats.Stats
	apiService      SalesforceAPIServiceInterface
	authService     SalesforceAuthServiceInterface
	dataHashToJobID map[string]int64 // Maps CSV row hash to job ID for result tracking
}

// SalesforceAuthServiceInterface handles OAuth token management
type SalesforceAuthServiceInterface interface {
	GetAccessToken() (string, error)
	GetInstanceURL() string
}

// SalesforceAuthService manages OAuth tokens via RudderStack's OAuth v2 service
type SalesforceAuthService struct {
	logger       logger.Logger
	oauthClient  oauthv2.Authorizer
	workspaceID  string
	accountID    string
	destID       string
	apiVersion   string
	accessToken  string
	instanceURL  string
	tokenExpiry  int64 // Unix timestamp
}

// SalesforceAPIServiceInterface defines Salesforce Bulk API operations
type SalesforceAPIServiceInterface interface {
	CreateJob(objectName, operation, externalIDField string) (string, *APIError)
	UploadData(jobID, csvFilePath string) *APIError
	CloseJob(jobID string) *APIError
	GetJobStatus(jobID string) (*JobResponse, *APIError)
	GetFailedRecords(jobID string) ([]map[string]string, *APIError)
	GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError)
	DeleteJob(jobID string) *APIError
}

// SalesforceAPIService handles Salesforce Bulk API 2.0 calls
type SalesforceAPIService struct {
	authService SalesforceAuthServiceInterface
	logger      logger.Logger
	apiVersion  string
}

// JobResponse represents Salesforce Bulk API job status
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

// JobCreateRequest is the request body for creating a Bulk API job
type JobCreateRequest struct {
	Object              string `json:"object"`
	ContentType         string `json:"contentType"`
	Operation           string `json:"operation"`
	LineEnding          string `json:"lineEnding"`
	ExternalIDFieldName string `json:"externalIdFieldName,omitempty"`
}

// APIError represents errors from Salesforce API
type APIError struct {
	StatusCode int
	Message    string
	Category   string // "RefreshToken", "RateLimit", "BadRequest", "ServerError"
}

const (
	destName = "SALESFORCE_BULK_UPLOAD"
)


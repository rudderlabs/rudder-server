package salesforcebulkupload

//go:generate mockgen -destination=../../../../mocks/router/salesforcebulkupload/salesforcebulkupload_mock.go -package=mocks github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk-upload APIServiceInterface

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

const (
	ApiVersion = "v62.0"
	ApiBaseURL = "https://default.salesforce.com/services/data/" + ApiVersion
	CSVDir     = "/tmp/rudder-async-destination-logs"
)

type Uploader struct {
	destName        string
	logger          logger.Logger
	statsFactory    stats.Stats
	apiService      APIServiceInterface
	dataHashToJobID map[string][]int64
	destinationInfo *oauthv2.DestinationInfo
	config          struct {
		maxBufferCapacity config.ValueLoader[int64]
	}
}

type APIServiceInterface interface {
	CreateJob(objectName, operation, externalIDField string) (string, *APIError)
	UploadData(jobID, csvFilePath string) *APIError
	CloseJob(jobID string) *APIError
	GetJobStatus(jobID string) (*JobResponse, *APIError)
	GetFailedRecords(jobID string) ([]map[string]string, *APIError)
	GetSuccessfulRecords(jobID string) ([]map[string]string, *APIError)
	DeleteJob(jobID string) *APIError
}

type apiService struct {
	logger          logger.Logger
	destinationInfo *oauthv2.DestinationInfo
	client          *http.Client
}

type SalesforceJobInfo struct {
	ID      string   `json:"id"`
	Headers []string `json:"headers"`
}

type JobResponse struct {
	ID                  string `json:"id"`
	State               string `json:"state"`
	NumberRecordsFailed int    `json:"numberRecordsFailed"`
	ErrorMessage        string `json:"errorMessage,omitempty"`
}

type jobCreateRequest struct {
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

type ObjectInfo struct {
	ObjectType      string
	ExternalIDField string
	ExternalIDValue string
}

const (
	destName = "SALESFORCE_BULK_UPLOAD"
)

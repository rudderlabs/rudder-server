package salesforcebulkupload

//go:generate mockgen -destination=../../../../mocks/router/salesforcebulkupload/salesforcebulkupload_mock.go -package=mocks github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk-upload APIServiceInterface

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

const (
	ApiVersion  = "v62.0"
	ApiBasePath = "/services/data/" + ApiVersion
	ApiBaseURL  = "https://oauth-placeholder.invalid" + ApiBasePath
	CSVDir      = "/tmp/rudder-async-destination-logs"
)

type Uploader struct {
	destName          string
	logger            logger.Logger
	statsFactory      stats.Stats
	apiService        APIServiceInterface
	externalIDToJobID map[string][]int64
	destination       *backendconfig.DestinationT
	config            struct {
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
	logger      logger.Logger
	destination *backendconfig.DestinationT
	client      *http.Client
}

type SalesforceJobInfo struct {
	ID string `json:"id"`
	// ExternalIDField is the upsert key field name. Job statuses are correlated
	// after polling by hashing this field's value (not the whole row), so that
	// Salesforce's value coercion on other columns (e.g. datetime millisecond
	// truncation, number scale) does not break the match.
	ExternalIDField string `json:"externalIdField"`
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

// SalesforceExternalID is a single VDM externalId entry (the upsert key spec).
type SalesforceExternalID struct {
	ID             string `json:"id"`
	IdentifierType string `json:"identifierType"`
	Type           string `json:"type"`
}

// SalesforceJobMetadata is the metadata this connector's Transform writes for
// each event and reads back during Upload.
type SalesforceJobMetadata struct {
	JobID           int64                  `json:"job_id"`
	RudderOperation string                 `json:"rudderOperation"`
	ExternalID      []SalesforceExternalID `json:"externalId"`
}

// SalesforceAsyncJob is the typed representation of a transformed event line.
// Message holds the arbitrary traits plus the externalId field/value.
type SalesforceAsyncJob struct {
	Message  map[string]any        `json:"message"`
	Metadata SalesforceJobMetadata `json:"metadata"`
}

const (
	destName = "SALESFORCE_BULK_UPLOAD"

	// externalIDFieldEmptyReason is returned for the whole batch when the upsert
	// externalId field name itself could not be resolved.
	externalIDFieldEmptyReason = "externalId field is empty; cannot correlate poll results back to jobs"
	// missingExternalIDReason is the abort reason for individual events that
	// carry no externalId value (no upsert key).
	missingExternalIDReason = "externalId is missing for the event; cannot upsert to Salesforce"
	// emptyCorrelationMapReason keeps jobs retryable when the in-memory map is
	// gone (e.g. a process restart between Upload and GetUploadStats).
	emptyCorrelationMapReason = "correlation map is empty (likely a restart between upload and stats); retrying"
	// resultCorrelationFailedReason aborts jobs whose externalId did not come
	// back in the Salesforce success/failed records (e.g. reformatted on store).
	resultCorrelationFailedReason = "could not correlate Salesforce result back to the job: externalId not found in success/failed records (possibly reformatted by Salesforce on store)"
)

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
	destName            string
	logger              logger.Logger
	statsFactory        stats.Stats
	apiService          APIServiceInterface
	destination         *backendconfig.DestinationT
	payloadSizeStat     stats.Histogram
	eventsPerFileStat   stats.Histogram
	asyncUploadTimeStat stats.Timer
	config              struct {
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
	// after polling on this field's value (not the whole row), so that
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
	ExternalIDs     []SalesforceExternalID `json:"externalId"`
}

// SalesforceAsyncJob is the typed representation of a transformed event line.
// Message holds the arbitrary traits plus the externalId field/value.
type SalesforceAsyncJob struct {
	Message  map[string]any        `json:"message"`
	Metadata SalesforceJobMetadata `json:"metadata"`
}

// importingMetadata is the per-job metadata persisted on the importing job
// status params (JobImportingParameters). It lets us rebuild the externalId →
// jobID correlation at poll time from the DB, without any in-memory state or
// reading the job payload. We store the SHA-256 hash (not the raw externalId)
// because the externalId is often PII (e.g. an email); correlation re-hashes the
// Salesforce-returned externalId to match.
type importingMetadata struct {
	ExternalIDHash string `json:"externalIdHash"`
}

const (
	destName = "SALESFORCE_BULK_UPLOAD"

	// salesforceNullSentinel is Salesforce Bulk API's documented value for
	// clearing a field: an empty CSV cell means "leave the field unchanged",
	// while #N/A sets the field to null.
	// https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_prepare_csv.htm
	salesforceNullSentinel = "#N/A"

	// noCorrelationMetadataReason aborts jobs when no externalId metadata is
	// present on the importing job statuses, so none of the Salesforce results
	// can be correlated back (e.g. pre-change in-flight jobs, or a bug). This is
	// logged at error level so it can be alerted on.
	noCorrelationMetadataReason = "no externalId correlation metadata found on the importing job; cannot correlate Salesforce result"
	// resultCorrelationFailedReason aborts jobs whose externalId did not come
	// back in the Salesforce success/failed records (e.g. reformatted on store).
	resultCorrelationFailedReason = "could not correlate Salesforce result back to the job: externalId not found in success/failed records (possibly reformatted by Salesforce on store)"
)

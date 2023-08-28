package eloqua

import (
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type Eloqua interface {
	GetBaseEndpoint(*HttpRequestData) (string, error)
	FetchFields(*HttpRequestData) (*Fields, error)
	CreateImportDefinition(*HttpRequestData, string) (*ImportDefinition, error)
	UploadData(*HttpRequestData, string) error
	RunSync(*HttpRequestData) (string, error)
	CheckSyncStatus(*HttpRequestData) (string, error)
	DeleteImportDefinition(*HttpRequestData) error
	CheckRejectedData(*HttpRequestData) (*RejectResponse, error)
}
type DestinationConfig struct {
	CompanyName              string   `json:"companyName"`
	Password                 string   `json:"password"`
	UserName                 string   `json:"userName"`
	OneTrustCookieCategories []string `json:"oneTrustCookieCategories"`
	RudderAccountID          string   `json:"rudderAccountId"`
}

type HttpRequestData struct {
	Body          io.Reader
	Authorization string
	BaseEndpoint  string
	Endpoint      string
	DynamicPart   string
	Offset        int
	Method        string
	ContentType   string
}

type URL struct {
	Base string                 `json:"base"`
	Apis map[string]interface{} `json:"apis"`
}

type LoginDetailsResponse struct {
	Site map[string]interface{} `json:"site"`
	User map[string]interface{} `json:"user"`
	Urls URL                    `json:"urls"`
}

type EloquaBulkUploader struct {
	destName      string
	logger        logger.Logger
	authorization string
	baseEndpoint  string
	fileSizeLimit int64
	eventsLimit   int64
	service       Eloqua
}

type HttpClient interface {
	Do(*http.Request) (*http.Response, error)
}
type item struct {
	CustomName              string    `json:"name"`
	InternalName            string    `json:"internalName"`
	DataType                string    `json:"dataType"`
	HasReadOnlyConstraint   bool      `json:"hasReadOnlyConstraint"`
	HasNotNullConstraint    bool      `json:"hasNotNullConstraint"`
	HasUniquenessConstraint bool      `json:"hasUniquenessConstraint"`
	Statement               string    `json:"statement"`
	URI                     string    `json:"uri"`
	CreatedAt               time.Time `json:"createdAt"`
	UpdatedAt               time.Time `json:"updatedAt"`
}

type Fields struct {
	Items        []item `json:"items"`
	TotalResults int    `json:"totalResults"`
	Limit        int    `json:"limit"`
	Offset       int    `json:"offset"`
	Count        int    `json:"count"`
	HasMore      bool   `json:"hasMore"`
}

type ImportDefinition struct {
	Name                             string            `json:"name"`
	Fields                           map[string]string `json:"fields"`
	IdentifierFieldName              string            `json:"identifierFieldName"`
	IsSyncTriggeredOnImport          bool              `json:"isSyncTriggeredOnImport"`
	DataRetentionDuration            string            `json:"dataRetentionDuration"`
	IsUpdatingMultipleMatchedRecords bool              `json:"isUpdatingMultipleMatchedRecords"`
	URI                              string            `json:"uri"`
	CreatedBy                        string            `json:"createdBy"`
	CreatedAt                        time.Time         `json:"createdAt"`
	UpdatedBy                        string            `json:"updatedBy"`
	UpdatedAt                        time.Time         `json:"updatedAt"`
}

// {
//     "syncedInstanceUri": "/contacts/imports/66",
//     "status": "pending",
//     "createdAt": "2023-08-17T15:39:41.0030000Z",
//     "createdBy": "Marketing.Analytics",
//     "uri": "/syncs/78"
// }

type SyncResponse struct {
	SyncedInstanceUri string    `json:"syncedInstanceUri"`
	Status            string    `json:"status"`
	CreatedAt         time.Time `json:createdAt`
	Uri               string    `json:uri`
}

type SyncStatusResponse struct {
	SyncedInstanceUri string    `json:"syncedInstanceUri"`
	SyncStartedAt     time.Time `json:"syncStartedAt"`
	SyncEndedAt       time.Time `json:"syncEndedAt"`
	Status            string    `json:"status"`
	CreatedAt         time.Time `json:"createdAt"`
	CreatedBy         string    `json:"CreatedBy"`
	Uri               string    `json:uri`
}

//	{
//	    "items": [
//	        {
//	            "fieldValues": {
//	                "C_FirstName": "Weldon_Wintheiser60",
//	                "C_LastName": "Vanuatu",
//	                "C_EmailAddress": "Polar bear"
//	            },
//	            "message": "Invalid email address.",
//	            "statusCode": "ELQ-00002",
//	            "recordIndex": 1,
//	            "invalidFields": [
//	                "C_EmailAddress"
//	            ]
//	        }
//	    ],
//	    "totalResults": 871598,
//	    "limit": 1,
//	    "offset": 0,
//	    "count": 1,
//	    "hasMore": true
//	}
type Item struct {
	FieldValues   map[string]string `json:"fieldValues"`
	Message       string            `json:"message"`
	StatusCode    string            `json:"statusCode"`
	RecordIndex   int64             `json:"recordIndex"`
	InvalidFields []string          `json:"invalidFields"`
}

type RejectResponse struct {
	Items        []Item `json:"items"`
	TotalResults int    `json:"totalResults"`
	Limit        int    `json:"limit"`
	Offset       int    `json:"offset"`
	Count        int    `json:"count"`
	HasMore      bool   `json:"hasMore"`
}

type Message struct {
	Data                map[string]interface{} `json:"data"`
	IdentifierFieldName string                 `json:"identifierFieldName"`
	CustomObjectId      string                 `json:"customObjectId"`
	Type                string                 `json:"type"`
}
type Metadata struct {
	JobID int64 `json:"job_id"`
}

// This struct represent each line of the text file created by the batchrouter
type TransformedData struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}
type JobInfo struct {
	succeededJobs []int64
	failedJobs    []int64
	fileSizeLimit int64
	importingJobs []int64
}

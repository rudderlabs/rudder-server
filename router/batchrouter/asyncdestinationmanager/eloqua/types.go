package eloqua

//go:generate mockgen -destination=../../../../mocks/router/eloqua/mock_eloqua.go -package=mock_bulkservice github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua EloquaService

import (
	"io"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type EloquaService interface {
	GetBaseEndpoint(*HttpRequestData) (string, error)
	FetchFields(*HttpRequestData) (*Fields, error)
	CreateImportDefinition(*HttpRequestData, string) (*ImportDefinition, error)
	UploadData(*HttpRequestData, string) error
	RunSync(*HttpRequestData) (string, error)
	CheckSyncStatus(*HttpRequestData) (string, error)
	DeleteImportDefinition(*HttpRequestData) error
	CheckRejectedData(*HttpRequestData) (*RejectResponse, error)
}

type EloquaBulkUploader struct {
	destName          string
	logger            logger.Logger
	statsFactory      stats.Stats
	authorization     string
	baseEndpoint      string
	fileSizeLimit     int64
	eventsLimit       int64
	service           EloquaService
	jobToCSVMap       map[int64]int64
	uniqueKeys        []string
	successStatusCode []string
}
type DestinationConfig struct {
	CompanyName string `json:"companyName"`
	Password    string `json:"password"`
	UserName    string `json:"userName"`
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

type EventDetails struct {
	Type                string
	CustomObjectId      string
	Fields              []string
	IdentifierFieldName string
}

/*
*
struct to unmarshal response of loginDetails

	{
	    "site": {
	        .....
	    },
	    "user": {
	        .....
	    },
	    "urls": {
	        "base": "https://secure.p04.eloqua.com",
	        "apis": {
	        ....
	        }
	    }
	}
*/
type LoginDetailsResponse struct {
	Site map[string]interface{} `json:"site"`
	User map[string]interface{} `json:"user"`
	Urls URL                    `json:"urls"`
}
type URL struct {
	Base string                 `json:"base"`
	Apis map[string]interface{} `json:"apis"`
}

/*
*
struct to unmarshal response fields fetched for object like contact traits

	{
	  "items": [
	    {
	      "name": "key1",
	      "internalName": "key11",
	      "dataType": "string",
	      "defaultValue": "value1",
	      "hasReadOnlyConstraint": false,
	      "hasNotNullConstraint": false,
	      "hasUniquenessConstraint": false,
	      "statement": "{{CustomObject[172].Field[976]}}",
	      "uri": "/customObjects/172/fields/976"
	    },
	    .....
	  ],
	  "totalResults": 8,
	  "limit": 1000,
	  "offset": 0,
	  "count": 8,
	  "hasMore": false
	}
*/
type Fields struct {
	Items        []Item `json:"items"`
	TotalResults int    `json:"totalResults"`
	Limit        int    `json:"limit"`
	Offset       int    `json:"offset"`
	Count        int    `json:"count"`
	HasMore      bool   `json:"hasMore"`
}
type Item struct {
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

/*
*
struct to unmarshal importdefinition. Import definition is like the schema using which we will upload the data

	{
	    "name": "Test Import",
	    "fields": {
	        "C_FirstName": "{{Contact.Field(C_FirstName)}}",
	        "C_LastName": "{{Contact.Field(C_LastName)}}",
	        "C_EmailAddress": "{{Contact.Field(C_EmailAddress)}}"
	    },
	    "identifierFieldName": "C_EmailAddress",
	    "isSyncTriggeredOnImport": false,
	    "dataRetentionDuration": "P7D",
	    "isUpdatingMultipleMatchedRecords": false,
	    "uri": "/contacts/imports/384",
	    "createdBy": "Marketing.Analytics",
	    "createdAt": "2023-08-28T16:07:49.9730000Z",
	    "updatedBy": "Marketing.Analytics",
	    "updatedAt": "2023-08-28T16:07:49.9730000Z"
	}
*/
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

/*
*
struct to unmarshal sync response. We need to run the sync after uploading the data to staging area.

	{
	    "syncedInstanceUri": "/contacts/imports/384",
	    "status": "pending",
	    "createdAt": "2023-08-28T16:11:16.0100000Z",
	    "createdBy": "Marketing.Analytics",
	    "uri": "/syncs/384"
	}
*/
type SyncResponse struct {
	SyncedInstanceUri string    `json:"syncedInstanceUri"`
	Status            string    `json:"status"`
	CreatedAt         time.Time `json:"createdAt"`
	Uri               string    `json:"uri"`
}

/*
*
struct to verify the syncStatusResponse. After running the sync we need to verify if the sync completed or pending

	{
	    "syncedInstanceUri": "/contacts/imports/384",
	    "syncStartedAt": "2023-08-28T16:11:16.5500000Z",
	    "syncEndedAt": "2023-08-28T16:11:18.9330000Z",
	    "status": "success",
	    "createdAt": "2023-08-28T16:11:16.0100000Z",
	    "createdBy": "Marketing.Analytics",
	    "uri": "/syncs/384"
	}
*/
type SyncStatusResponse struct {
	SyncedInstanceUri string    `json:"syncedInstanceUri"`
	SyncStartedAt     time.Time `json:"syncStartedAt"`
	SyncEndedAt       time.Time `json:"syncEndedAt"`
	Status            string    `json:"status"`
	CreatedAt         time.Time `json:"createdAt"`
	CreatedBy         string    `json:"CreatedBy"`
	Uri               string    `json:"uri"`
}

/*
*
struct to unmarshal RejectResponse. if we get any warning after running the sync we need to check the failed events.

	{
	    "items": [
	        {
	            "fieldValues": {
	                "C_FirstName": "Weldon_Wintheiser60",
	                "C_LastName": "Vanuatu",
	                "C_EmailAddress": "Polar bear"
	            },
	            "message": "Invalid email address.",
	            "statusCode": "ELQ-00002",
	            "recordIndex": 1,
	            "invalidFields": [
	                "C_EmailAddress"
	            ]
	        }
	    ],
	    "totalResults": 871598,
	    "limit": 1,
	    "offset": 0,
	    "count": 1,
	    "hasMore": true
	}
*/
type RejectResponse struct {
	Items        []RejectedItem `json:"items"`
	TotalResults int            `json:"totalResults"`
	Limit        int            `json:"limit"`
	Offset       int            `json:"offset"`
	Count        int            `json:"count"`
	HasMore      bool           `json:"hasMore"`
}
type RejectedItem struct {
	FieldValues   map[string]string `json:"fieldValues"`
	Message       string            `json:"message"`
	StatusCode    string            `json:"statusCode"`
	RecordIndex   int64             `json:"recordIndex"`
	InvalidFields []string          `json:"invalidFields"`
}

/*
*
This struct represent each line of the text file created by the batchrouter
{"message":{"data":{"C_Address1":"xx/y3, abcd3 efgh3","C_EmailAddress":"test3@mail.com","C_FirstName":"Test3","C_LastName":"Name3"},"identifierFieldName":"C_EmailAddress","customObjectId": "contacts","type":"identify"},"metadata":{"job_id":1014}}
*/
type TransformedData struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
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

/*
*
struct to propagate jobs among functions
*/
type JobInfo struct {
	succeededJobs []int64
	failedJobs    []int64
	fileSizeLimit int64
	importingJobs []int64
}

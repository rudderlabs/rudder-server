package klaviyobulkupload

//go:generate mockgen -destination=../../../../mocks/router/klaviyobulkupload/klaviyobulkupload_mock.go -package=mocks github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload KlaviyoAPIService

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type KlaviyoAPIService interface {
	UploadProfiles(profiles Payload) (*UploadResp, error)
	GetUploadStatus(importId string) (*PollResp, error)
	GetUploadErrors(importId string) (*UploadStatusResp, error)
}

type KlaviyoBulkUploader struct {
	DestName             string
	DestinationConfig    map[string]interface{}
	Logger               logger.Logger
	StatsFactory         stats.Stats
	KlaviyoAPIService    KlaviyoAPIService
	JobIdToIdentifierMap map[string]int64
}

type ErrorDetail struct {
	ID     string      `json:"id"`
	Code   string      `json:"code"`
	Title  string      `json:"title"`
	Detail string      `json:"detail"`
	Source ErrorSource `json:"source"`
}

type ErrorSource struct {
	Pointer   string `json:"pointer"`
	Parameter string `json:"parameter"`
}

type UploadResp struct {
	Data struct {
		Id string `json:"id"`
	} `json:"data"`
	Errors []ErrorDetail `json:"errors"`
}

type PollResp struct {
	Data struct {
		Id         string `json:"id"`
		Attributes struct {
			Total_count     int    `json:"total_count"`
			Completed_count int    `json:"completed_count"`
			Failed_count    int    `json:"failed_count"`
			Status          string `json:"status"`
		} `json:"attributes"`
	} `json:"data"`
	Errors []ErrorDetail `json:"errors"`
}

type UploadStatusResp struct {
	Data []struct {
		Type       string `json:"type"`
		ID         string `json:"id"`
		Attributes struct {
			Code   string `json:"code"`
			Title  string `json:"title"`
			Detail string `json:"detail"`
			Source struct {
				Pointer string `json:"pointer"`
			} `json:"source"`
			OriginalPayload struct {
				Id          string `json:"id"`
				AnonymousId string `json:"anonymous_id"`
			} `json:"original_payload"`
		} `json:"attributes"`
		Links struct {
			Self string `json:"self"`
		} `json:"links"`
	} `json:"data"`
	Links struct {
		Self  string `json:"self"`
		First string `json:"first"`
		Last  string `json:"last"`
		Prev  string `json:"prev"`
		Next  string `json:"next"`
	} `json:"links"`
	Errors []ErrorDetail `json:"errors"`
}

type Payload struct {
	Data Data `json:"data"`
}

type Data struct {
	Type          string            `json:"type"`
	Attributes    PayloadAttributes `json:"attributes"`
	Relationships *Relationships    `json:"relationships,omitempty"`
}

type PayloadAttributes struct {
	Profiles Profiles `json:"profiles"`
}

type ProfileAttributes struct {
	Email         string `json:"email,omitempty"`
	Phone         string `json:"phone_number,omitempty"`
	ExternalId    string `json:"external_id,omitempty"`
	FirstName     string `json:"first_name,omitempty"`
	JobIdentifier string `json:"jobIdentifier,omitempty"`
	LastName      string `json:"last_name,omitempty"`
	Organization  string `json:"organization,omitempty"`
	Title         string `json:"title,omitempty"`
	Image         string `json:"image,omitempty"`
	Location      struct {
		Address1  string `json:"address1,omitempty"`
		Address2  string `json:"address2,omitempty"`
		City      string `json:"city,omitempty"`
		Country   string `json:"country,omitempty"`
		Latitude  string `json:"latitude,omitempty"`
		Longitude string `json:"longitude,omitempty"`
		Region    string `json:"region,omitempty"`
		Zip       string `json:"zip,omitempty"`
		Timezone  string `json:"timezone,omitempty"`
		IP        string `json:"ip,omitempty"`
	} `json:"location,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

type Profiles struct {
	Data []Profile `json:"data"`
}

type Relationships struct {
	Lists Lists `json:"lists"`
}

type Lists struct {
	Data []List `json:"data"`
}

type List struct {
	Type string `json:"type,omitempty"`
	ID   string `json:"id,omitempty"`
}
type Metadata struct {
	JobID int `json:"jobId,omitempty"`
}

type Profile struct {
	Attributes ProfileAttributes `json:"attributes,omitempty"`
	ID         string            `json:"id,omitempty"`
	Type       string            `json:"type,omitempty"`
}

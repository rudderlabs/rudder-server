package klaviyobulkupload

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

type Uploader interface {
	Upload(*common.AsyncDestinationStruct) common.AsyncUploadOutput
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Poller interface {
	Poll(input common.AsyncPoll) common.PollStatusResponse
}

type UploadStats interface {
	GetUploadStats(common.GetUploadStatsInput) common.GetUploadStatsResponse
}

type KlaviyoBulkUploader struct {
	destName             string
	destinationConfig    map[string]interface{}
	logger               logger.Logger
	Client               *http.Client
	jobIdToIdentifierMap map[string]int64
}

type UploadResp struct {
	Data struct {
		Id string `json:"id"`
	} `json:"data"`
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
}

type Payload struct {
	Data Data `json:"data"`
}

type Data struct {
	Type          string         `json:"type"`
	Attributes    Attributes     `json:"attributes"`
	Relationships *Relationships `json:"relationships,omitempty"`
}

type Attributes struct {
	Profiles Profiles `json:"profiles"`
}

type Profiles struct {
	Data []map[string]interface{} `json:"data"`
}

type Relationships struct {
	Lists Lists `json:"lists"`
}

type Lists struct {
	Data []List `json:"data"`
}

type List struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

package common

import (
	stdjson "encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

var AsyncDestinations = []string{"MARKETO_BULK_UPLOAD", "BING_ADS"}

type PollStatusResponse struct {
	Complete       bool
	InProgress     bool
	StatusCode     int
	HasFailed      bool
	HasWarning     bool
	FailedJobURLs  string
	WarningJobsURL string
}
type AsyncUploadOutput struct {
	Key                 string
	ImportingJobIDs     []int64
	ImportingParameters stdjson.RawMessage
	FailedJobIDs        []int64
	SucceededJobIDs     []int64
	SuccessResponse     string
	FailedReason        string
	AbortJobIDs         []int64
	AbortReason         string
	ImportingCount      int
	FailedCount         int
	AbortCount          int
	DestinationID       string
}

type AsyncPoll struct {
	Config   map[string]interface{} `json:"config"`
	ImportId string                 `json:"importId"`
	DestType string                 `json:"destType"`
}

type ErrorResponse struct {
	Error string
}

type Connection struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}
type BatchedJobs struct {
	Jobs       []*jobsdb.JobT
	Connection *Connection
	TimeWindow time.Time
}

type AsyncJob struct {
	Message  map[string]interface{} `json:"message"`
	Metadata map[string]interface{} `json:"metadata"`
}

type AsyncUploadT struct {
	Config   map[string]interface{} `json:"config"`
	Input    []AsyncJob             `json:"input"`
	DestType string                 `json:"destType"`
}

type UploadStruct struct {
	ImportId string                 `json:"importId"`
	PollUrl  string                 `json:"pollURL"`
	Metadata map[string]interface{} `json:"metadata"`
}

type MetaDataT struct {
	CSVHeaders string `json:"csvHeader"`
}
type ImportParameters struct {
	ImportId string    `json:"importId"`
	PollUrl  *string   `json:"pollURL"`
	MetaData MetaDataT `json:"metadata"`
}

type AsyncDestinationStruct struct {
	ImportingJobIDs []int64
	FailedJobIDs    []int64
	Exists          bool
	Size            int
	CreatedAt       time.Time
	FileName        string
	Count           int
	CanUpload       bool
	UploadMutex     sync.RWMutex
	URL             string
	RsourcesStats   rsources.StatsCollector
}

type AsyncFailedPayload struct {
	Config   map[string]interface{}   `json:"config"`
	Input    []map[string]interface{} `json:"input"`
	DestType string                   `json:"destType"`
	ImportId string                   `json:"importId"`
	MetaData MetaDataT                `json:"metadata"`
}

type FetchUploadJobStatus struct {
	FailedJobURLs      string
	Parameters         stdjson.RawMessage
	ImportingList      []*jobsdb.JobT
	PollResultFileURLs string
}

type EventStatMeta struct {
	FailedKeys    []int64
	ErrFailed     error
	WarningKeys   []int64
	ErrWarning    error
	SucceededKeys []int64
	ErrSuccess    error
	FailedReasons map[int64]string
}

type GetUploadStatsResponse struct {
	Status   string        `json:"status"`
	Metadata EventStatMeta `json:"metadata"`
}

func GetTransformedData(payload stdjson.RawMessage) string {
	return gjson.Get(string(payload), "body.JSON").String()
}

func GetBatchRouterConfigInt64(key, destType string, defaultValue int64) int64 {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt64("BatchRouter."+destType+"."+key, defaultValue)
	} else {
		return config.GetInt64("BatchRouter."+key, defaultValue)
	}
}

/*
Generates array of strings for comma separated string
Also removes "" elements from the array of strings if any.
*/
func GenerateArrayOfStrings(value string) []string {
	result := []string{}
	requestIdsArray := strings.Split(value, ",")
	for _, requestId := range requestIdsArray {
		if requestId != "" {
			result = append(result, requestId)
		}
	}
	return result
}

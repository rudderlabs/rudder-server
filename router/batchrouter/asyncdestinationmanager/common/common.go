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
)

type AsyncDestinationManager interface {
	Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput
	Poll(pollInput AsyncPoll) PollStatusResponse
	GetUploadStats(UploadStatsInput GetUploadStatsInput) GetUploadStatsResponse
}

var AsyncDestinations = []string{"MARKETO_BULK_UPLOAD", "BING_ADS", "ELOQUA"}

type PollStatusResponse struct {
	Complete       bool
	InProgress     bool
	StatusCode     int
	HasFailed      bool
	HasWarning     bool
	FailedJobURLs  string
	WarningJobURLs string
	Error          string `json:"error"`
}
type AsyncUploadOutput struct {
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
	ImportId string `json:"importId"`
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
	Metadata map[string]interface{} `json:"metadata"`
}

type MetaDataT struct {
	CSVHeaders string `json:"csvHeader"`
}
type ImportParameters struct {
	ImportId string    `json:"importId"`
	MetaData MetaDataT `json:"metadata"`
}

type AsyncDestinationStruct struct {
	ImportingJobIDs       []int64
	FailedJobIDs          []int64
	Exists                bool
	Size                  int
	CreatedAt             time.Time
	FileName              string
	Count                 int
	CanUpload             bool
	UploadInProgress      bool
	UploadMutex           sync.RWMutex
	DestinationUploadURL  string
	Destination           *backendconfig.DestinationT
	Manager               AsyncDestinationManager
	AttemptNums           map[int64]int
	FirstAttemptedAts     map[int64]time.Time
	OriginalJobParameters map[int64]stdjson.RawMessage
}

type AsyncFailedPayload struct {
	Config   map[string]interface{}   `json:"config"`
	Input    []map[string]interface{} `json:"input"`
	DestType string                   `json:"destType"`
	ImportId string                   `json:"importId"`
	MetaData MetaDataT                `json:"metadata"`
}

type GetUploadStatsInput struct {
	FailedJobURLs      string
	Parameters         stdjson.RawMessage
	ImportingList      []*jobsdb.JobT
	PollResultFileURLs string
	WarningJobURLs     string
}

type EventStatMeta struct {
	FailedKeys     []int64
	WarningKeys    []int64
	SucceededKeys  []int64
	FailedReasons  map[int64]string
	WarningReasons map[int64]string
}

type GetUploadStatsResponse struct {
	StatusCode int           `json:"statusCode"`
	Metadata   EventStatMeta `json:"metadata"`
	Error      string        `json:"error"`
}

func GetTransformedData(payload stdjson.RawMessage) string {
	return gjson.Get(string(payload), "body.JSON").String()
}

func GetBatchRouterConfigInt64(key, destType string, defaultValue int64) int64 {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt64("BatchRouter."+destType+"."+key, defaultValue)
	}
	return config.GetInt64("BatchRouter."+key, defaultValue)
}

func GetBatchRouterConfigBool(key, destType string, defaultValue bool) bool {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetBool("BatchRouter."+destType+"."+key, defaultValue)
	}
	return config.GetBool("BatchRouter."+key, defaultValue)
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

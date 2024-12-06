package common

import (
	stdjson "encoding/json"
	"net/http"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type AsyncUploadAndTransformManager interface {
	Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput
	Transform(job *jobsdb.JobT) (string, error)
}

type AsyncDestinationManager interface {
	AsyncUploadAndTransformManager
	Poll(pollInput AsyncPoll) PollStatusResponse
	GetUploadStats(UploadStatsInput GetUploadStatsInput) GetUploadStatsResponse
}

type SimpleAsyncDestinationManager struct {
	UploaderAndTransformer AsyncUploadAndTransformManager
}

func (m SimpleAsyncDestinationManager) Upload(asyncDestStruct *AsyncDestinationStruct) AsyncUploadOutput {
	return m.UploaderAndTransformer.Upload(asyncDestStruct)
}

func (m SimpleAsyncDestinationManager) Poll(AsyncPoll) PollStatusResponse {
	return PollStatusResponse{
		StatusCode: http.StatusOK,
		Complete:   true,
	}
}

func (m SimpleAsyncDestinationManager) GetUploadStats(GetUploadStatsInput) GetUploadStatsResponse {
	return GetUploadStatsResponse{
		StatusCode: http.StatusOK,
	}
}

func (m SimpleAsyncDestinationManager) Transform(job *jobsdb.JobT) (string, error) {
	return m.UploaderAndTransformer.Transform(job)
}

type PollStatusResponse struct {
	Complete             bool
	InProgress           bool
	StatusCode           int
	HasFailed            bool
	HasWarning           bool
	FailedJobParameters  string
	WarningJobParameters string
	Error                string `json:"error"`
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

type AsyncJob struct {
	Message  map[string]interface{} `json:"message"`
	Metadata map[string]interface{} `json:"metadata"`
}

type AsyncUploadT struct {
	Config   map[string]interface{} `json:"config"`
	Input    []AsyncJob             `json:"input"`
	DestType string                 `json:"destType"`
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
	PartFileNumber        int
	SourceJobRunID        string
}

type GetUploadStatsInput struct {
	FailedJobParameters  string
	Parameters           stdjson.RawMessage
	ImportingList        []*jobsdb.JobT
	WarningJobParameters string
}

type EventStatMeta struct {
	FailedKeys     []int64
	AbortedKeys    []int64
	WarningKeys    []int64
	SucceededKeys  []int64
	FailedReasons  map[int64]string
	AbortedReasons map[int64]string
	WarningReasons map[int64]string
}

type GetUploadStatsResponse struct {
	StatusCode int           `json:"statusCode"`
	Metadata   EventStatMeta `json:"metadata"`
	Error      string        `json:"error"`
}

func GetMarshalledData(payload string, jobID int64) (string, error) {
	var asyncJob AsyncJob
	err := json.Unmarshal([]byte(payload), &asyncJob.Message)
	if err != nil {
		return "", err
	}
	asyncJob.Metadata = make(map[string]interface{})
	asyncJob.Metadata["job_id"] = jobID
	responsePayload, err := json.Marshal(asyncJob)
	if err != nil {
		return "", err
	}
	return string(responsePayload), nil
}

func GetBatchRouterConfigInt64(key, destType string, defaultValue int64) int64 {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt64("BatchRouter."+destType+"."+key, defaultValue)
	}
	return config.GetInt64("BatchRouter."+key, defaultValue)
}

func GetBatchRouterConfigStringMap(key, destType string, defaultValue []string) []string {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetStringSlice("BatchRouter."+destType+"."+key, defaultValue)
	}
	return config.GetStringSlice("BatchRouter."+key, defaultValue)
}

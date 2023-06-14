package common

import (
	stdjson "encoding/json"
	"sync"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/tidwall/gjson"
)

var (
	AsyncDestinations = []string{"MARKETO_BULK_UPLOAD", "BING_ADS"}
)

// we need to add bingAds specific fields if needs to be handy.
type PollStatusResponse struct {
	Success        bool
	StatusCode     int
	HasFailed      bool
	HasWarning     bool
	FailedJobsURL  string
	WarningJobsURL string
	OutputFilePath string
}
type AsyncUploadOutput struct {
	Key                 string
	ImportingJobIDs     []int64
	ImportingParameters stdjson.RawMessage
	SuccessJobIDs       []int64
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
	FailedJobsURL  string
	Parameters     stdjson.RawMessage
	ImportingList  []*jobsdb.JobT
	OutputFilePath string
}

type EventStatMeta struct {
	FailedKeys    []int64
	ErrFailed     error
	WarningKeys   []int64
	ErrWarning    error
	SucceededKeys []int64
	ErrSuccess    error
	FailedReasons map[string]string
}

type GetUploadStatsResponse struct {
	Status   string        `json:"status"`
	Metadata EventStatMeta `json:"metadata"`
}

func GetTransformedData(payload stdjson.RawMessage) string {
	return gjson.Get(string(payload), "body.JSON").String()
}

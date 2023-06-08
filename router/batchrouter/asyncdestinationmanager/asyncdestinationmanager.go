package asyncdestinationmanager

import (
	stdjson "encoding/json"
	"sync"
 "time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
    "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bingads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
)

type Asyncdestinationmanager interface {
	Upload(destination *backendconfig.DestinationT, asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput
	Poll(pollStruct common.AsyncPoll) (common.AsyncStatusResponse, int)
	FetchFailedEvents(failedJobsStatus common.FetchFailedStatus) ([]byte, int)
	RetrieveImportantKeys(metadata map[string]interface{}, retrieveKeys string) ([]int64, error)
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

type AsyncUploadT struct {
	Config   map[string]interface{} `json:"config"`
	Input    []AsyncJob             `json:"input"`
	DestType string                 `json:"destType"`
}

type AsyncJob struct {
	Message  map[string]interface{} `json:"message"`
	Metadata map[string]interface{} `json:"metadata"`
}

type MetaDataT struct {
	CSVHeaders string `json:"csvHeader"`
}

type UploadStruct struct {
	ImportId string                 `json:"importId"`
	PollUrl  string                 `json:"pollURL"`
	Metadata map[string]interface{} `json:"metadata"`
}

type Parameters struct {
	ImportId string    `json:"importId"`
	PollUrl  string    `json:"pollURL"`
	MetaData MetaDataT `json:"metadata"`
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	HTTPTimeout time.Duration
	pkgLogger   logger.Logger
)

func loadConfig() {
	config.RegisterDurationConfigVariable(600, &HTTPTimeout, true, time.Second, "AsyncDestination.HTTPTimeout")
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("asyncDestinationManager")
}

func CleanUpData(keyMap map[string]interface{}, importingJobIDs []int64) ([]int64, []int64) {
	if keyMap == nil {
		return []int64{}, importingJobIDs
	}

	_, ok := keyMap["successfulJobs"].([]interface{})
	var succesfulJobIDs, failedJobIDsTrans []int64
	var err error
	if ok {
		succesfulJobIDs, err = misc.ConvertStringInterfaceToIntArray(keyMap["successfulJobs"])
		if err != nil {
			failedJobIDsTrans = importingJobIDs
		}
	}
	_, ok = keyMap["unsuccessfulJobs"].([]interface{})
	if ok {
		failedJobIDsTrans, err = misc.ConvertStringInterfaceToIntArray(keyMap["unsuccessfulJobs"])
		if err != nil {
			failedJobIDsTrans = importingJobIDs
		}
	}
	return succesfulJobIDs, failedJobIDsTrans
}

func GetTransformedData(payload stdjson.RawMessage) string {
	return gjson.Get(string(payload), "body.JSON").String()
}

func GetMarshalledData(payload string, jobID int64) string {
	var job AsyncJob
	err := json.Unmarshal([]byte(payload), &job.Message)
	if err != nil {
		panic("Unmarshalling Transformer Response Failed")
	}
	job.Metadata = make(map[string]interface{})
	job.Metadata["job_id"] = jobID
	responsePayload, err := json.Marshal(job)
	if err != nil {
		panic("Marshalling Response Payload Failed")
	}
	return string(responsePayload)
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) Asyncdestinationmanager {
	switch destination.DestinationDefinition.Name {
    case "BINGADS_AUDIENCE":
		return bingads.NewManager(destination, backendConfig, HTTPTimeout)
	case "MARKETO_BULK_UPLOAD": 
		return marketobulkupload.NewManager(destination, HTTPTimeout)
	}
	return nil
}

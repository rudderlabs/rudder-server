package asyncdestinationmanager

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

var (
	DefaultAsyncDestinationFactory AsyncDestinationFactory
)

type AsyncDestinationFactoryT struct {
	destManagers map[string]AsyncFileManager
}

type AsyncUploadOutput struct {
	Key                 string
	ImportingJobIDs     []int64
	ImportingParameters json.RawMessage
	SuccessJobIDs       []int64
	FailedJobIDs        []int64
	SucceededJobIDs     []int64
	SuccessResponse     string
	FailedReason        string
	AbortJobIDs         []int64
	AbortReason         string
	importingCount      int
	FailedCount         int
	AbortCount          int
	DestinationID       string
}

type AsyncDestinationStruct struct {
	ImportingJobIDs []int64
	FailedJobIDs    []int64
	Exists          bool
	Size            int
	CreatedAt       time.Time
	FileName        string
	Count           int
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

type AsyncDestinationFactory interface {
	Get(destType string) (AsyncFileManager, error)
}

// FileManager inplements all upload methods
type AsyncFileManager interface {
	Upload(string, string, map[string]interface{}, string, []int64, []int64, string) AsyncUploadOutput
	GetTransformedData(json.RawMessage) string
	GenerateFailedPayload(map[string]interface{}, []*jobsdb.JobT, string, string, string) []byte
	GetMarshalledData(string, int64) string
}

type AsyncFailedPayload struct {
	Config   map[string]interface{}   `json:"config"`
	Input    []map[string]interface{} `json:"input"`
	DestType string                   `json:"destType"`
	ImportId string                   `json:"importId"`
	MetaData MetaDataT                `json:"metadata"`
}

type MetaDataT struct {
	CSVHeaders string `json:"csvHeader"`
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func init() {
	asyncFileManagerFactory := &AsyncDestinationFactoryT{}
	asyncFileManagerFactory.destManagers = make(map[string]AsyncFileManager)
	DefaultAsyncDestinationFactory = asyncFileManagerFactory
}

// New returns FileManager backed by configured provider
func (factory *AsyncDestinationFactoryT) Get(destType string) (AsyncFileManager, error) {
	switch destType {
	case "MARKETO_BULK_UPLOAD":
		marketoManager, ok := factory.destManagers["MARKETO_BULK_UPLOAD"]
		if !ok {
			marketoManager = &MarketoManager{}
			factory.destManagers["MARKETO_BULK_UPLOAD"] = marketoManager
		}
		return marketoManager, nil
	}
	return nil, errors.New("no provider configured for FileManager")
}

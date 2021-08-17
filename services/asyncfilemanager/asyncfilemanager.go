package asyncfilemanager

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

var (
	DefaultAsyncFileManagerFactory AsyncFileManagerFactory
)

type AsyncFileManagerFactoryT struct{}

type AsyncUploadOutput struct {
	Key                 string
	ImportingJobIDs     []int64
	ImportingParameters json.RawMessage
	FailedJobIDs        []int64
	SucceededJobIDs     []int64
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
	Data     string                 `json:"data"`
	DestType string                 `json:"destType"`
}

type AsyncFileManagerFactory interface {
	New(destType string) (AsyncFileManager, error)
}

// FileManager inplements all upload methods
type AsyncFileManager interface {
	Upload(string, string, map[string]interface{}, string, []int64, []int64, string) AsyncUploadOutput
	GetTransformedData(json.RawMessage) string
	GenerateFailedPayload(map[string]interface{}, []*jobsdb.JobT, string, string) []byte
}

type AsyncFailedPayload struct {
	Config   map[string]interface{} `json:"config"`
	Data     map[string]interface{} `json:"data"`
	DestType string                 `json:"destType"`
	ImportId string                 `json:"importId"`
}

// SettingsT sets configuration for FileManager
type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func init() {
	DefaultAsyncFileManagerFactory = &AsyncFileManagerFactoryT{}
}

// New returns FileManager backed by configured provider
func (factory *AsyncFileManagerFactoryT) New(destType string) (AsyncFileManager, error) {
	switch destType {
	case "MARKETO_BULK_UPLOAD":
		return &MarketoManager{}, nil
	}
	return nil, errors.New("no provider configured for FileManager")
}

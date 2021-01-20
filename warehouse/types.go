package warehouse

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type PayloadT struct {
	BatchID             string
	UploadID            int64
	StagingFileID       int64
	StagingFileLocation string
	Schema              map[string]map[string]string
	SourceID            string
	SourceName          string
	DestinationID       string
	DestinationName     string
	DestinationType     string
	DestinationConfig   interface{}
	UniqueLoadGenID     string
	Output              []loadFileUploadOutputT
}

type ProcessStagingFilesJobT struct {
	Upload    UploadT
	List      []*StagingFileT
	Warehouse warehouseutils.WarehouseT
}

type LoadFileJobT struct {
	Upload                     UploadT
	StagingFile                *StagingFileT
	Schema                     map[string]map[string]string
	Warehouse                  warehouseutils.WarehouseT
	Wg                         *misc.WaitGroup
	LoadFileIDsChan            chan []int64
	TableToBucketFolderMap     map[string]string
	TableToBucketFolderMapLock *sync.RWMutex
}

type StagingFileT struct {
	ID           int64
	Location     string
	SourceID     string
	Schema       json.RawMessage
	Status       string // enum
	CreatedAt    time.Time
	FirstEventAt time.Time
	LastEventAt  time.Time
}

type BatchRouterEventT struct {
	Metadata MetadataT `json:"metadata"`
	Data     DataT     `json:"data"`
}

type MetadataT struct {
	Table        string            `json:"table"`
	Columns      map[string]string `json:"columns"`
	IsMergeRule  bool              `json:"isMergeRule"`
	ReceivedAt   time.Time         `json:"receivedAt"`
	MergePropOne string            `json:"mergePropOne"`
	MergePropTwo string            `json:"mergePropTwo"`
}

type DataT map[string]interface{}

type ColumnInfoT struct {
	ColumnVal  interface{}
	ColumnType string
}

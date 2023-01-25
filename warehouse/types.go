package warehouse

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Payload struct {
	BatchID                      string
	UploadID                     int64
	StagingFileID                int64
	StagingFileLocation          string
	UploadSchema                 map[string]map[string]string
	WorkspaceID                  string
	SourceID                     string
	SourceName                   string
	DestinationID                string
	DestinationName              string
	DestinationType              string
	DestinationNamespace         string
	DestinationRevisionID        string
	StagingDestinationRevisionID string
	DestinationConfig            map[string]interface{}
	StagingDestinationConfig     interface{}
	UseRudderStorage             bool
	StagingUseRudderStorage      bool
	UniqueLoadGenID              string
	RudderStoragePrefix          string
	Output                       []loadFileUploadOutputT
	LoadFilePrefix               string // prefix for the load file name
	LoadFileType                 string
}

type LoadFileJobT struct {
	StagingFile                *model.StagingFile
	Schema                     map[string]map[string]string
	Warehouse                  warehouseutils.Warehouse
	Wg                         *misc.WaitGroup
	LoadFileIDsChan            chan []int64
	TableToBucketFolderMap     map[string]string
	TableToBucketFolderMapLock *sync.RWMutex
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

type FilterClause struct {
	Clause    string
	ClauseArg interface{}
}

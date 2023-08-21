package warehouse

import (
	"sync"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type LoadFileJob struct {
	StagingFile                *model.StagingFile
	Schema                     model.Schema
	Warehouse                  model.Warehouse
	Wg                         *misc.WaitGroup
	LoadFileIDsChan            chan []int64
	TableToBucketFolderMap     map[string]string
	TableToBucketFolderMapLock *sync.RWMutex
}

type BatchRouterEvent struct {
	Metadata Metadata `json:"metadata"`
	Data     Data     `json:"data"`
}

func (event *BatchRouterEvent) GetColumnInfo(columnName string) (columnInfo warehouseutils.ColumnInfo, ok bool) {
	columnVal, ok := event.Data[columnName]
	if !ok {
		return warehouseutils.ColumnInfo{}, false
	}

	columnType, ok := event.Metadata.Columns[columnName]
	if !ok {
		return warehouseutils.ColumnInfo{}, false
	}

	return warehouseutils.ColumnInfo{Value: columnVal, Type: columnType}, true
}

type Metadata struct {
	Table        string            `json:"table"`
	Columns      map[string]string `json:"columns"`
	IsMergeRule  bool              `json:"isMergeRule"`
	ReceivedAt   time.Time         `json:"receivedAt"`
	MergePropOne string            `json:"mergePropOne"`
	MergePropTwo string            `json:"mergePropTwo"`
}

type Data map[string]interface{}

type FilterClause struct {
	Clause    string
	ClauseArg interface{}
}

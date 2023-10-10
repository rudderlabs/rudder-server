package types

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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

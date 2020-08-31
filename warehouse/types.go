package warehouse

import (
	"time"
)

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

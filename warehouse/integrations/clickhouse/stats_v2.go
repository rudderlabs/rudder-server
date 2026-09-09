package clickhouse

import (
	"github.com/rudderlabs/rudder-go-kit/stats"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// loadStats are the measurements one table load produces.
//
// The names match v1's wherever the concept is the same, so a destination
// moving between the two implementations stays comparable on one dashboard.
// The block-level ones have no v1 counterpart: v1 commits a table once, while
// v2 commits every commitEvery rows, and how those blocks behave is the thing
// worth watching during a migration.
type loadStats struct {
	numRowsLoadFile       stats.Counter
	downloadLoadFilesTime stats.Timer
	loadTableTime         stats.Timer
	blockReadTime         stats.Timer
	blockPrepareTime      stats.Timer
	blockBindTime         stats.Timer
	commitTime            stats.Timer
	blockSize             stats.Histogram
	blocks                stats.Counter
	blockRetries          stats.Counter
}

func (ch *ClickhouseV2) newLoadStats(tableName string) *loadStats {
	tags := stats.Tags{
		"workspaceId": ch.Warehouse.WorkspaceID,
		"destination": ch.Warehouse.Destination.ID,
		"destType":    ch.Warehouse.Type,
		"source":      ch.Warehouse.Source.ID,
		"identifier":  ch.Warehouse.Identifier,
		"tableName":   warehouseutils.TableNameForStats(tableName),
	}
	return &loadStats{
		numRowsLoadFile:       ch.stats.NewTaggedStat("warehouse.clickhouse.numRowsLoadFile", stats.CountType, tags),
		downloadLoadFilesTime: ch.stats.NewTaggedStat("warehouse.clickhouse.downloadLoadFilesTime", stats.TimerType, tags),
		loadTableTime:         ch.stats.NewTaggedStat("warehouse.clickhouse.loadTableTime", stats.TimerType, tags),
		blockReadTime:         ch.stats.NewTaggedStat("warehouse.clickhouse.blockReadTime", stats.TimerType, tags),
		blockPrepareTime:      ch.stats.NewTaggedStat("warehouse.clickhouse.blockPrepareTime", stats.TimerType, tags),
		blockBindTime:         ch.stats.NewTaggedStat("warehouse.clickhouse.blockBindTime", stats.TimerType, tags),
		commitTime:            ch.stats.NewTaggedStat("warehouse.clickhouse.commitTime", stats.TimerType, tags),
		blockSize:             ch.stats.NewTaggedStat("warehouse.clickhouse.blockSize", stats.HistogramType, tags),
		blocks:                ch.stats.NewTaggedStat("warehouse.clickhouse.blocks", stats.CountType, tags),
		blockRetries:          ch.stats.NewTaggedStat("warehouse.clickhouse.blockRetries", stats.CountType, tags),
	}
}

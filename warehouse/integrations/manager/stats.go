package manager

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type statsManager struct {
	Manager

	statsFactory stats.Stats
	stats        struct {
		addColumnsCount   stats.Counter
		addTablesCount    stats.Counter
		alterColumnsCount stats.Counter
	}
}

func newStatsManager(manager Manager, statsFactory stats.Stats) *statsManager {
	return &statsManager{Manager: manager, statsFactory: statsFactory}
}

func (m *statsManager) Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error {
	tags := stats.Tags{
		"module":      "warehouse",
		"workspaceId": warehouse.WorkspaceID,
		"warehouseID": misc.GetTagName(warehouse.Destination.ID, warehouse.Source.Name, warehouse.Destination.Name, warehouse.Source.ID),
		"destID":      warehouse.Destination.ID,
		"destType":    warehouse.Destination.DestinationDefinition.Name,
		"sourceID":    warehouse.Source.ID,
		"sourceType":  warehouse.Source.SourceDefinition.Name,
	}
	m.stats.addColumnsCount = m.statsFactory.NewTaggedStat("columns_added", stats.CountType, tags)
	m.stats.addTablesCount = m.statsFactory.NewTaggedStat("tables_added", stats.CountType, tags)
	m.stats.alterColumnsCount = m.statsFactory.NewTaggedStat("columns_altered", stats.CountType, tags)
	return m.Manager.Setup(ctx, warehouse, uploader)
}

func (m *statsManager) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	err := m.Manager.AddColumns(ctx, tableName, columnsInfo)
	if err != nil {
		return fmt.Errorf("adding columns: %w", err)
	}
	m.stats.addColumnsCount.Count(len(columnsInfo))
	return nil
}

func (m *statsManager) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	response, err := m.Manager.AlterColumn(ctx, tableName, columnName, columnType)
	if err == nil {
		m.stats.alterColumnsCount.Increment()
	}
	return response, err
}

func (m *statsManager) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	err = m.Manager.CreateTable(ctx, tableName, columnMap)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	m.stats.addTablesCount.Increment()
	return nil
}

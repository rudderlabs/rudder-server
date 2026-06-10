package bqstreamv2

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// fetchSchemaFromWarehouseForTables fetches the namespace schema and filters it down
// to the given tables.
func (m *Manager) fetchSchemaFromWarehouseForTables(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator, tableNames []string) (whutils.ModelSchema, error) {
	m.logger.Infon("Fetching schema from warehouse")

	bigQueryManager, err := integrationManagerCreator(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery manager: %w", err)
	}

	warehouseSchema, err := bigQueryManager.FetchSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	tableNamesSet := lo.SliceToMap(tableNames, func(tableName string) (string, struct{}) {
		return whutils.ToProviderCase(whutils.BQStreamV2, tableName), struct{}{}
	})

	filteredWarehouseSchema := lo.PickBy(warehouseSchema, func(tableName string, schema whutils.ModelTableSchema) bool {
		_, ok := tableNamesSet[tableName]
		return ok
	})

	return filteredWarehouseSchema, nil
}

// createSchemaInWarehouse creates the schema in the warehouse.
func (m *Manager) createSchemaInWarehouse(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator) error {
	m.logger.Infon("Creating schema in warehouse",
		logger.NewStringField(logfield.Namespace, cfg.Namespace),
	)

	bigQueryManager, err := integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}

	if err := bigQueryManager.CreateSchema(ctx); err != nil {
		return fmt.Errorf("creating schema in warehouse: %w", err)
	}
	return nil
}

// createTableSchema creates the table schema in the warehouse.
func (m *Manager) createTableSchema(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator, tableName string, eventsSchema whutils.ModelTableSchema) error {
	m.logger.Infon("Creating table schema",
		logger.NewStringField(logfield.Namespace, cfg.Namespace),
		logger.NewStringField(logfield.TableName, tableName),
	)

	bigQueryManager, err := integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}

	if err := bigQueryManager.CreateTable(ctx, tableName, eventsSchema); err != nil {
		return fmt.Errorf("creating table schema: %w", err)
	}
	return nil
}

// addColumnsToTable adds the columns to the table in the warehouse.
func (m *Manager) addColumnsToTable(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator, tableName string, columns []whutils.ColumnInfo) error {
	for _, column := range columns {
		m.logger.Infon("Adding column",
			logger.NewStringField("name", column.Name),
			logger.NewStringField("type", column.Type),
			logger.NewStringField(logfield.TableName, tableName),
			logger.NewStringField(logfield.Namespace, cfg.Namespace),
		)
	}

	bigQueryManager, err := integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}

	if err := bigQueryManager.AddColumns(ctx, tableName, columns); err != nil {
		return fmt.Errorf("adding columns: %w", err)
	}
	return nil
}

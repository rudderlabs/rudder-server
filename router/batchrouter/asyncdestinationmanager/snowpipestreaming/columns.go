package snowpipestreaming

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// addColumns adds columns to a Snowflake table one at a time, as ALTER TABLE does not support IF NOT EXISTS with multiple columns.
func (m *Manager) addColumns(ctx context.Context, namespace, tableName string, columns []whutils.ColumnInfo) error {
	m.logger.Infon("Adding columns", logger.NewStringField("table", tableName))

	snowflakeManager, err := m.createSnowflakeManager(ctx, namespace)
	if err != nil {
		return fmt.Errorf("creating snowflake manager: %w", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	for _, column := range columns {
		if err = snowflakeManager.AddColumns(ctx, tableName, []whutils.ColumnInfo{column}); err != nil {
			return fmt.Errorf("adding column: %w", err)
		}
	}
	return nil
}

func findNewColumns(eventSchema, snowPipeSchema whutils.ModelTableSchema) []whutils.ColumnInfo {
	var newColumns []whutils.ColumnInfo
	for column, dataType := range eventSchema {
		if _, exists := snowPipeSchema[column]; !exists {
			newColumns = append(newColumns, whutils.ColumnInfo{
				Name: column,
				Type: dataType,
			})
		}
	}
	return newColumns
}

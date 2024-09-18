package snowpipestreaming

import (
	"context"
	"fmt"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (m *Manager) addColumns(ctx context.Context, namespace, tableName string, columns []whutils.ColumnInfo) error {
	snowflakeManager, err := m.createSnowflakeManager(ctx, namespace)
	if err != nil {
		return fmt.Errorf("creating snowflake manager: %v", err)
	}
	defer func() {
		snowflakeManager.Cleanup(ctx)
	}()
	if err = snowflakeManager.AddColumns(ctx, tableName, columns); err != nil {
		return fmt.Errorf("adding columns: %v", err)
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

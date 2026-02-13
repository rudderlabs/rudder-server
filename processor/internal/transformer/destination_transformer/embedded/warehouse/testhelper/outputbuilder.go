package testhelper

import (
	"strings"

	"github.com/samber/lo"
)

type OutputBuilder map[string]any

func (ob OutputBuilder) SetDataField(key string, value any) OutputBuilder {
	if _, ok := ob["data"]; !ok {
		ob["data"] = make(map[string]any)
	}
	if dataMap, ok := ob["data"].(map[string]any); ok {
		dataMap[key] = value
	}
	return ob
}

func (ob OutputBuilder) SetMetadata(key string, value any) OutputBuilder {
	if _, ok := ob["metadata"]; !ok {
		ob["metadata"] = make(map[string]any)
	}
	metadataMap := ob["metadata"].(map[string]any)
	metadataMap[key] = value
	return ob
}

func (ob OutputBuilder) SetColumnField(key string, value any) OutputBuilder {
	if _, ok := ob["metadata"]; !ok {
		ob["metadata"] = make(map[string]any)
	}
	if metadataMap, ok := ob["metadata"].(map[string]any); ok {
		if _, ok := metadataMap["columns"]; !ok {
			metadataMap["columns"] = make(map[string]any)
		}
		if columnsMap, ok := metadataMap["columns"].(map[string]any); ok {
			columnsMap[key] = value
		}
	}
	return ob
}

func (ob OutputBuilder) SetTableName(tableName string) OutputBuilder {
	if _, ok := ob["metadata"]; !ok {
		ob["metadata"] = make(map[string]any)
	}
	if metadataMap, ok := ob["metadata"].(map[string]any); ok {
		metadataMap["table"] = tableName
	}
	return ob
}

func (ob OutputBuilder) RemoveDataFields(fields ...string) OutputBuilder {
	if dataMap, ok := ob["data"].(map[string]any); ok {
		for _, key := range fields {
			delete(dataMap, key)
		}
	}
	return ob
}

func (ob OutputBuilder) RemoveMetadata(fields ...string) OutputBuilder {
	if metadataMap, ok := ob["metadata"].(map[string]any); ok {
		for _, key := range fields {
			delete(metadataMap, key)
		}
	}
	return ob
}

func (ob OutputBuilder) RemoveColumnFields(fields ...string) OutputBuilder {
	if metadataMap, ok := ob["metadata"].(map[string]any); ok {
		if columnsMap, ok := metadataMap["columns"].(map[string]any); ok {
			for _, key := range fields {
				delete(columnsMap, key)
			}
		}
	}
	return ob
}

func (ob OutputBuilder) AddRandomEntries(count int, predicate func(index int) (dataKey, dataValue, columnKey, columnValue string)) OutputBuilder {
	for i := range count {
		dataKey, dataValue, columnKey, columnValue := predicate(i)
		ob.SetDataField(dataKey, dataValue)
		ob.SetColumnField(columnKey, columnValue)
	}
	return ob
}

func (ob OutputBuilder) BuildForSnowflake() OutputBuilder {
	dataMap, ok := ob["data"].(map[string]any)
	if ok {
		ob["data"] = lo.MapEntries(dataMap, func(key string, value any) (string, any) {
			return strings.ToUpper(key), value
		})
	}
	metadataMap, ok := ob["metadata"].(map[string]any)
	if ok {
		columnsMap, ok := metadataMap["columns"].(map[string]any)
		if ok {
			metadataMap["columns"] = lo.MapEntries(columnsMap, func(key string, value any) (string, any) {
				return strings.ToUpper(key), value
			})
		}

		tableName, ok := metadataMap["table"].(string)
		if ok {
			metadataMap["table"] = strings.ToUpper(tableName)
		}
	}
	return ob
}

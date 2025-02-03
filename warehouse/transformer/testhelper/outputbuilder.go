package testhelper

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
	for i := 0; i < count; i++ {
		dataKey, dataValue, columnKey, columnValue := predicate(i)
		ob.SetDataField(dataKey, dataValue)
		ob.SetColumnField(columnKey, columnValue)
	}
	return ob
}

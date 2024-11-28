package transformer

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/datatype"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	violationErrors     = "violationErrors"
	redshiftStringLimit = 512
)

func (t *transformer) getDataType(destType, key string, val any, isJSONKey bool) string {
	if typeName := getPrimitiveType(val); typeName != "" {
		return typeName
	}
	if strVal, ok := val.(string); ok && utils.ValidTimestamp(strVal) {
		return datatype.TypeDateTime
	}
	if override := t.getDataTypeOverride(destType, key, val, isJSONKey); override != "" {
		return override
	}
	return datatype.TypeString
}

func getPrimitiveType(val any) string {
	switch v := val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return datatype.TypeInt
	case float64:
		return getFloatType(v)
	case float32:
		return getFloatType(float64(v))
	case bool:
		return datatype.TypeBoolean
	default:
		return ""
	}
}

func getFloatType(v float64) string {
	if v == float64(int64(v)) {
		return datatype.TypeInt
	}
	return datatype.TypeFloat
}

func (t *transformer) getDataTypeOverride(destType, key string, val any, isJSONKey bool) string {
	switch destType {
	case whutils.POSTGRES, whutils.SNOWFLAKE:
		return overrideForPostgresSnowflake(key, isJSONKey)
	case whutils.RS:
		return overrideForRedshift(val, isJSONKey)
	case whutils.CLICKHOUSE:
		return t.overrideForClickhouse(destType, key, val)
	default:
		return ""
	}
}

func overrideForPostgresSnowflake(key string, isJSONKey bool) string {
	if key == violationErrors || isJSONKey {
		return datatype.TypeJSON
	}
	return datatype.TypeString
}

func overrideForRedshift(val any, isJSONKey bool) string {
	if isJSONKey {
		return datatype.TypeJSON
	}
	if val == nil {
		return datatype.TypeString
	}
	if jsonVal, _ := json.Marshal(val); len(jsonVal) > redshiftStringLimit {
		return datatype.TypeText
	}
	return datatype.TypeString
}

func (t *transformer) overrideForClickhouse(destType, key string, val any) string {
	if !t.config.enableArraySupport.Load() {
		return datatype.TypeString
	}

	arrayVal, ok := val.([]any)
	if !ok || len(arrayVal) == 0 {
		return datatype.TypeString
	}
	return t.determineClickHouseArrayType(destType, key, arrayVal)
}

func (t *transformer) determineClickHouseArrayType(destType, key string, arrayVal []any) string {
	finalDataType := t.getDataType(destType, key, arrayVal[0], false)

	for i := 1; i < len(arrayVal); i++ {
		dataType := t.getDataType(destType, key, arrayVal[i], false)

		if finalDataType == dataType {
			continue
		}
		if finalDataType == datatype.TypeString {
			break
		}
		if dataType == datatype.TypeFloat && finalDataType == datatype.TypeInt {
			finalDataType = datatype.TypeFloat
			continue
		}
		if dataType == datatype.TypeInt && finalDataType == datatype.TypeFloat {
			continue
		}
		finalDataType = datatype.TypeString
	}
	return datatype.TypeArray + "(" + finalDataType + ")"
}

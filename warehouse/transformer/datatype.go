package transformer

import (
	"reflect"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func dataTypeFor(destType, key string, val any, isJSONKey bool) string {
	if typeName := primitiveType(val); typeName != "" {
		return typeName
	}
	if strVal, ok := val.(string); ok && utils.ValidTimestamp(strVal) {
		return model.DateTimeDataType
	}
	if override := dataTypeOverride(destType, key, val, isJSONKey); override != "" {
		return override
	}
	return model.StringDataType
}

func primitiveType(val any) string {
	switch v := val.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return model.IntDataType
	case float64:
		return getFloatType(v)
	case float32:
		return getFloatType(float64(v))
	case bool:
		return model.BooleanDataType
	default:
		return ""
	}
}

func getFloatType(v float64) string {
	// JSON unmarshalling treats all numbers as float64 by default, even if they are whole numbers
	// So, we need to check if the float is actually an integer
	if v == float64(int64(v)) {
		return model.IntDataType
	}
	return model.FloatDataType
}

func dataTypeOverride(destType, key string, val any, isJSONKey bool) string {
	switch destType {
	case whutils.POSTGRES, whutils.SNOWFLAKE, whutils.SnowpipeStreaming:
		return overrideForPostgresSnowflake(key, isJSONKey)
	case whutils.RS:
		return overrideForRedshift(val, isJSONKey)
	default:
		return ""
	}
}

func overrideForPostgresSnowflake(key string, isJSONKey bool) string {
	if isJSONKey || key == violationErrors {
		return model.JSONDataType
	}
	return model.StringDataType
}

func overrideForRedshift(val any, isJSONKey bool) string {
	if isJSONKey {
		return model.JSONDataType
	}
	if val == nil {
		return model.StringDataType
	}
	switch reflect.TypeOf(val).Kind() {
	case reflect.Slice, reflect.Array:
		if jsonVal, _ := jsonrs.Marshal(val); len(jsonVal) > redshiftStringLimit {
			return model.TextDataType
		}
		return model.StringDataType
	case reflect.String:
		if len(val.(string)) > redshiftStringLimit {
			return model.TextDataType
		}
		return model.StringDataType
	default:
		return model.StringDataType
	}
}

func convertValIfDateTime(val any, colType string) any {
	if colType == model.DateTimeDataType {
		return utils.ToTimestamp(val)
	}
	return val
}

func convertToFloat64IfInteger(val any) any {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	}
	return val
}

func convertToSliceIfViolationErrors(val any) any {
	if validationErrors, ok := val.([]types.ValidationError); ok {
		result := make([]any, len(validationErrors))
		for i, e := range validationErrors {
			result[i] = map[string]any{
				"type":     e.Type,
				"message":  e.Message,
				"meta":     lo.MapValues(e.Meta, func(value, _ string) any { return value }),
				"property": e.Property,
			}
		}
		return result
	}
	return val
}

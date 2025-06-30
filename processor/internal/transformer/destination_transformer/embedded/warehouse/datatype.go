package warehouse

import (
	"math/big"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils"
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
	// We are using Number.isInteger(val) for detecting whether datatype is int or float in rudder-transformer
	// which has higher range then what we have in Golang (9223372036854775807), therefore using big package for determining the type
	if big.NewFloat(v).IsInt() {
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
	if shouldUseTextForRedshift(val) {
		return model.TextDataType
	}
	return model.StringDataType
}

func shouldUseTextForRedshift(data any) bool {
	switch v := data.(type) {
	case []any, map[string]any:
		jsonVal, _ := utils.MarshalJSON(v)
		// Javascript strings are UTF-16 encoded, use utf16 instead of utf8 package for determining the length
		if utils.UTF16RuneCountInString(string(jsonVal)) > redshiftStringLimit {
			return true
		}
	case string:
		// Javascript strings are UTF-16 encoded, use utf16 instead of utf8 package for determining the length
		if utils.UTF16RuneCountInString(v) > redshiftStringLimit {
			return true
		}
	}
	return false
}

func convertValIfDateTime(val any, colType string) any {
	if colType == model.DateTimeDataType {
		return utils.ToTimestamp(val)
	}
	return val
}

package warehouse

import (
	"fmt"
	"math/big"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils"
	"github.com/rudderlabs/rudder-server/processor/types"
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
	case []any, []types.ValidationError, map[string]any:
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

func addDataAndMetadataForContextGeoEnrichment(tec *transformEventContext, data map[string]any, metadata map[string]string, key string, val any) error {
	if geoLocation, ok := val.(enricher.Geolocation); ok {
		if len(geoLocation.IP) > 0 {
			ipKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_ip"))
			if err != nil {
				return fmt.Errorf("could not get ip column name: %w", err)
			}
			data[ipKey], metadata[ipKey] = geoLocation.IP, model.StringDataType
		}
		if len(geoLocation.City) > 0 {
			cityKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_city"))
			if err != nil {
				return fmt.Errorf("could not get city column name: %w", err)
			}
			data[cityKey], metadata[cityKey] = geoLocation.City, model.StringDataType
		}
		if len(geoLocation.Country) > 0 {
			countryKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_country"))
			if err != nil {
				return fmt.Errorf("could not get country column name: %w", err)
			}
			data[countryKey], metadata[countryKey] = geoLocation.Country, model.StringDataType
		}
		if len(geoLocation.Region) > 0 {
			regionKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_region"))
			if err != nil {
				return fmt.Errorf("could not get region column name: %w", err)
			}
			data[regionKey], metadata[regionKey] = geoLocation.Region, model.StringDataType
		}
		if len(geoLocation.Postal) > 0 {
			postalKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_postal"))
			if err != nil {
				return fmt.Errorf("could not get postal column name: %w", err)
			}
			data[postalKey], metadata[postalKey] = geoLocation.Postal, model.StringDataType
		}
		if len(geoLocation.Location) > 0 {
			locationKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_location"))
			if err != nil {
				return fmt.Errorf("could not get location column name: %w", err)
			}
			data[locationKey], metadata[locationKey] = geoLocation.Location, model.StringDataType
		}
		if len(geoLocation.Timezone) > 0 {
			timezoneKey, err := safeColumnNameCached(tec, transformColumnNameCached(tec, key+"_timezone"))
			if err != nil {
				return fmt.Errorf("could not get timezone column name: %w", err)
			}
			data[timezoneKey], metadata[timezoneKey] = geoLocation.Timezone, model.StringDataType
		}
		return nil
	}
	data[key] = val
	return nil
}

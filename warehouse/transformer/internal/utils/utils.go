package utils

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	rudderCreatedTables                      = sliceToMap([]string{"tracks", "pages", "screens", "aliases", "groups", "accounts"})
	rudderIsolatedTables                     = sliceToMap([]string{"users", "identifies"})
	sourceCategoriesToUseRecordID            = sliceToMap([]string{"cloud", "singer-protocol"})
	identityEnabledWarehouses                = sliceToMap([]string{whutils.SNOWFLAKE, whutils.BQ})
	destinationSupportJSONPathAsPartOfConfig = sliceToMap([]string{whutils.POSTGRES, whutils.RS, whutils.SNOWFLAKE, whutils.SnowpipeStreaming, whutils.BQ})

	supportedJSONPathPrefixes     = []string{"track.", "identify.", "page.", "screen.", "alias.", "group.", "extract."}
	fullEventColumnTypeByDestType = map[string]string{
		whutils.SNOWFLAKE:         model.JSONDataType,
		whutils.SnowpipeStreaming: model.JSONDataType,
		whutils.RS:                model.TextDataType,
		whutils.BQ:                model.StringDataType,
		whutils.POSTGRES:          model.JSONDataType,
		whutils.MSSQL:             model.JSONDataType,
		whutils.AzureSynapse:      model.JSONDataType,
		whutils.CLICKHOUSE:        model.StringDataType,
		whutils.S3Datalake:        model.StringDataType,
		whutils.DELTALAKE:         model.StringDataType,
		whutils.GCSDatalake:       model.StringDataType,
		whutils.AzureDatalake:     model.StringDataType,
	}

	reDateTime = regexp.MustCompile(
		`^([+-]?\d{4})((-)((0[1-9]|1[0-2])(-([12]\d|0[1-9]|3[01])))([T\s]((([01]\d|2[0-3])((:)[0-5]\d))(:\d+)?)?(:[0-5]\d([.]\d+)?)?([zZ]|([+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)$`,
	)

	minTimeInMs = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTimeInMs = time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC)
)

func sliceToMap(slice []string) map[string]struct{} {
	return lo.SliceToMap(slice, func(item string) (string, struct{}) {
		return item, struct{}{}
	})
}

func IsDataLake(destType string) bool {
	switch destType {
	case whutils.S3Datalake, whutils.GCSDatalake, whutils.AzureDatalake:
		return true
	default:
		return false
	}
}

func IsRudderSources(event map[string]any) bool {
	return event["channel"] == "sources" || event["CHANNEL"] == "sources"
}

func IsRudderCreatedTable(tableName string) bool {
	_, ok := rudderCreatedTables[strings.ToLower(tableName)]
	return ok
}

func IsRudderIsolatedTable(tableName string) bool {
	_, ok := rudderIsolatedTables[strings.ToLower(tableName)]
	return ok
}

func IsObject(val any) bool {
	_, ok := val.(map[string]any)
	return ok
}

func IsIdentityEnabled(destType string) bool {
	_, ok := identityEnabledWarehouses[destType]
	return ok
}

func CanUseRecordID(sourceCategory string) bool {
	_, ok := sourceCategoriesToUseRecordID[strings.ToLower(sourceCategory)]
	return ok
}

func HasJSONPathPrefix(jsonPath string) bool {
	lowerJSONPath := strings.ToLower(jsonPath)
	for _, prefix := range supportedJSONPathPrefixes {
		if strings.HasPrefix(lowerJSONPath, prefix) {
			return true
		}
	}
	return false
}

func GetFullEventColumnTypeByDestType(destType string) string {
	return fullEventColumnTypeByDestType[destType]
}

func ValidTimestamp(input string) bool {
	if !reDateTime.MatchString(input) {
		return false
	}
	t, err := dateparse.ParseAny(input)
	if err != nil {
		return false
	}
	return !t.Before(minTimeInMs) && !t.After(maxTimeInMs)
}

func ToTimestamp(val any) any {
	if strVal, ok := val.(string); ok {
		t, err := dateparse.ParseAny(strVal)
		if err != nil {
			return val
		}
		return t.UTC().Format(misc.RFC3339Milli)
	}
	return val
}

// ToString converts any value to a string representation.
// - If the value is nil, it returns an empty string.
// - If the value implements the fmt.Stringer interface, it returns the result of the String() method.
// - Otherwise, it returns a string representation using fmt.Sprintf.
func ToString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", value)
	}
}

// IsBlank checks if the given value is considered "blank."
// - A value is considered blank if its string representation is an empty string.
// - The function first converts the value to its string representation using ToString and checks if its length is zero.
func IsBlank(value interface{}) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case string:
		return v == ""
	case fmt.Stringer:
		return v.String() == ""
	default:
		return false
	}
}

func IsJSONPathSupportedAsPartOfConfig(destType string) bool {
	_, ok := destinationSupportJSONPathAsPartOfConfig[destType]
	return ok
}

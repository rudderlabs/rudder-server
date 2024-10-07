package utils

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/datatype"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	rudderCreatedTables           = sliceToMap([]string{"tracks", "pages", "screens", "aliases", "groups", "accounts"})
	rudderIsolatedTables          = sliceToMap([]string{"users", "identifies"})
	sourceCategoriesToUseRecordID = sliceToMap([]string{"cloud", "singer-protocol"})
	identityEnabledWarehouses     = sliceToMap([]string{whutils.SNOWFLAKE, whutils.BQ})

	supportedJSONPathPrefixes     = []string{"track.", "identify.", "page.", "screen.", "alias.", "group.", "extract."}
	fullEventColumnTypeByDestType = map[string]string{
		whutils.SNOWFLAKE:     datatype.TypeJSON,
		whutils.RS:            datatype.TypeText,
		whutils.BQ:            datatype.TypeString,
		whutils.POSTGRES:      datatype.TypeJSON,
		whutils.MSSQL:         datatype.TypeJSON,
		whutils.AzureSynapse:  datatype.TypeJSON,
		whutils.CLICKHOUSE:    datatype.TypeString,
		whutils.S3Datalake:    datatype.TypeString,
		whutils.DELTALAKE:     datatype.TypeString,
		whutils.GCSDatalake:   datatype.TypeString,
		whutils.AzureDatalake: datatype.TypeString,
	}

	timestampRegex = regexp.MustCompile(
		`^([+-]?\d{4})((-)((0[1-9]|1[0-2])(-([12]\d|0[1-9]|3[01])))([T\s]((([01]\d|2[0-3])((:)[0-5]\d))(:\d+)?)?(:[0-5]\d([.]\d+)?)?([zZ]|([+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)$`,
	)
	timestampToParse = []string{time.RFC3339, time.RFC3339Nano, time.DateOnly, time.DateTime}

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
	for _, prefix := range supportedJSONPathPrefixes {
		if strings.HasPrefix(strings.ToLower(jsonPath), prefix) {
			return true
		}
	}
	return false
}

func GetFullEventColumnTypeByDestType(destType string) string {
	return fullEventColumnTypeByDestType[destType]
}

func ValidTimestamp(input string) bool {
	if !timestampRegex.MatchString(input) {
		return false
	}

	for _, format := range timestampToParse {
		t, err := time.Parse(format, input)
		if err != nil {
			continue
		}
		return t.After(minTimeInMs) && t.Before(maxTimeInMs)
	}
	return false
}

// ToString converts any value to a string representation.
// - If the value is nil, it returns an empty string.
// - If the value implements the fmt.Stringer interface, it returns the result of the String() method.
// - Otherwise, it returns a string representation using fmt.Sprintf.
func ToString(value interface{}) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(fmt.Stringer); ok {
		return str.String()
	}
	return fmt.Sprintf("%v", value)
}

// IsBlank checks if the given value is considered "blank."
// - A value is considered blank if its string representation is an empty string.
// - The function first converts the value to its string representation using ToString and checks if its length is zero.
func IsBlank(value interface{}) bool {
	return len(ToString(value)) == 0
}

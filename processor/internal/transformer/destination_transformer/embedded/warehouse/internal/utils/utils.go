package utils

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/araddon/dateparse"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
	maxTimestampFormat             = "2012-08-03 18:31:59.257000000 +00:00 UTC"
	validTimestampFormatsMaxLength = len(maxTimestampFormat)

	minTimeInMs = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTimeInMs = time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC)

	jsonrsStd = jsonrs.NewWithLibrary(jsonrs.StdLib)
)

func init() {
	_ = dateparse.MustParse(maxTimestampFormat)
}

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

func IsArray(val any) bool {
	_, ok := val.([]any)
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
	if len(input) > validTimestampFormatsMaxLength {
		return false
	}
	if !reDateTime.MatchString(input) {
		return false
	}

	t, err := parseTimestamp(input)
	if err != nil {
		return false
	}
	return !t.Before(minTimeInMs) && !t.After(maxTimeInMs)
}

func ToTimestamp(val any) any {
	if strVal, ok := val.(string); ok {
		t, err := parseTimestamp(strVal)
		if err != nil {
			return val
		}
		return t.UTC().Format(misc.RFC3339Milli)
	}
	return val
}

// parseTimestamp parses a timestamp string into time.Time.
// If it fails due to a "day out of range" error, it falls back to normalizing the date.
// JS automatically handles this https://www.programiz.com/online-compiler/4gfcMEByAur4q
// console.log(new Date('1988-04-31').toISOString()); // 1988-05-01T00:00:00.000Z
func parseTimestamp(input string) (time.Time, error) {
	t, err := dateparse.ParseAny(input)
	if err == nil {
		return t, nil
	}
	var pe *time.ParseError
	ok := errors.As(err, &pe)
	if !ok {
		return time.Time{}, err
	}
	if pe.Message == ": day out of range" {
		var year, month, day int

		if n, _ := fmt.Sscanf(input, "%d-%d-%d", &year, &month, &day); n == 3 {
			t = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
			return t, nil
		}
	}
	return time.Time{}, err
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

// IsEmptyString checks if the given value is considered "blank."
// - A value is considered blank if its string representation is an empty string.
// - The function first converts the value to its string representation using ToString and checks if its length is zero.
//
// Corresponding lodash implementation: https://playcode.io/2371369
//
//	const isBlank = (value) => {
//	 try {
//	   return _.isEmpty(_.toString(value));
//	 } catch (e) {
//	   console.error(`Error in isBlank: ${e.message}`);
//	   return false;
//	 }
//	};
func IsEmptyString(value interface{}) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case string:
		return v == ""
	case fmt.Stringer:
		return v.String() == ""
	case map[string]any:
		return false
	case []any:
		if len(v) == 0 {
			return true
		}
		if len(v) == 1 {
			if v[0] == nil {
				return false
			}
			return IsEmptyString(v[0])
		}
		return false
	case []types.ValidationError:
		return len(v) == 0
	default:
		return false
	}
}

func IsJSONPathSupportedAsPartOfConfig(destType string) bool {
	_, ok := destinationSupportJSONPathAsPartOfConfig[destType]
	return ok
}

func ExtractMessageID(event *types.TransformerEvent, uuidGenerator func() string) any {
	messageID, exists := event.Message["messageId"]
	if !exists || IsEmptyString(messageID) {
		return "auto-" + uuidGenerator()
	}
	return messageID
}

func ExtractReceivedAt(event *types.TransformerEvent, now func() time.Time) string {
	receivedAt, exists := event.Message["receivedAt"]
	if !exists || IsEmptyString(receivedAt) {
		if len(event.Metadata.ReceivedAt) > 0 {
			return event.Metadata.ReceivedAt
		}
		return now().Format(misc.RFC3339Milli)
	}

	strReceivedAt, isString := receivedAt.(string)
	if !isString || !ValidTimestamp(strReceivedAt) {
		if len(event.Metadata.ReceivedAt) > 0 {
			return event.Metadata.ReceivedAt
		}
		return now().Format(misc.RFC3339Milli)
	}
	return strReceivedAt
}

// MarshalJSON marshals the input to JSON. It escapes HTML characters (e.g. &, <, and > from \u0026, \u003c, and \u003e) by default.
// It also trims the output to avoid trailing spaces.
func MarshalJSON(input any) ([]byte, error) {
	var buf bytes.Buffer

	enc := jsonrsStd.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(input); err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}

// UTF16RuneCountInString returns the UTF-16 code unit count of the string.
func UTF16RuneCountInString(s string) int {
	count := 0
	for _, r := range s {
		count += utf16.RuneLen(r)
	}
	return count
}

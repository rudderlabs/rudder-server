package common

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
)

var (
	asyncDestinations = []string{"MARKETO_BULK_UPLOAD", "BINGADS_AUDIENCE", "ELOQUA", "YANDEX_METRICA_OFFLINE_EVENTS", "BINGADS_OFFLINE_CONVERSIONS", "KLAVIYO_BULK_UPLOAD", "LYTICS_BULK_UPLOAD", "SNOWPIPE_STREAMING", "SALESFORCE_BULK_UPLOAD"}
	sftpDestinations  = []string{"SFTP"}
)

func IsSFTPDestination(destination string) bool {
	return slices.Contains(sftpDestinations, destination)
}

func IsAsyncRegularDestination(destination string) bool {
	return slices.Contains(asyncDestinations, destination)
}

func IsAsyncDestination(destination string) bool {
	return slices.Contains(append(asyncDestinations, sftpDestinations...), destination)
}

// FormatCSVValue stringifies a JSON-derived value for a CSV cell.
// Top-level nil renders as an empty cell so destinations that treat empty
// cells as null (e.g. Salesforce Bulk) get the expected semantics. All
// other values go through stringify, which avoids scientific notation for
// floats while preserving Go's default bracketed shape for arrays and maps.
func FormatCSVValue(value any) string {
	if value == nil {
		return ""
	}
	return stringify(value)
}

func stringify(value any) string {
	switch v := value.(type) {
	case nil:
		return "<nil>"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case []any:
		parts := make([]string, len(v))
		for i, item := range v {
			parts[i] = stringify(item)
		}
		return "[" + strings.Join(parts, " ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+":"+stringify(v[k]))
		}
		return "map[" + strings.Join(parts, " ") + "]"
	default:
		return fmt.Sprintf("%v", v)
	}
}

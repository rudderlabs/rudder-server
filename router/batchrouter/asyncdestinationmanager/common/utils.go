package common

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

var (
	asyncDestinations = []string{
		"MARKETO_BULK_UPLOAD", "BINGADS_AUDIENCE", "ELOQUA", "YANDEX_METRICA_OFFLINE_EVENTS",
		"BINGADS_OFFLINE_CONVERSIONS", "KLAVIYO_BULK_UPLOAD", "LYTICS_BULK_UPLOAD",
		"SNOWPIPE_STREAMING", "SALESFORCE_BULK_UPLOAD", "BQSTREAM_ALL_EVENTS",
	}
	sftpDestinations = []string{"SFTP"}

	allAsyncDestinations = append(asyncDestinations, sftpDestinations...)
)

func IsSFTPDestination(destination string) bool {
	return slices.Contains(sftpDestinations, destination)
}

func IsAsyncRegularDestination(destination string) bool {
	return slices.Contains(asyncDestinations, destination)
}

func IsAsyncDestination(destination string) bool {
	return slices.Contains(allAsyncDestinations, destination)
}

// FormatCSVValue stringifies a JSON-derived value for a CSV cell.
// Top-level nil renders as an empty cell; destinations where an empty cell
// does not mean null (e.g. Salesforce Bulk ignores empty cells and requires
// the #N/A sentinel to clear a field) must encode their sentinel before this
// point. Floats are rendered without scientific notation, and arrays/maps
// are emitted as JSON so nested numbers stay plain and nested nulls become
// `null`. Returns an error when JSON marshalling of a composite value fails.
func FormatCSVValue(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	switch v := value.(type) {
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case []any, map[string]any:
		b, err := jsonrs.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

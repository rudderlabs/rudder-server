package warehouseutils

import (
	"regexp"
	"strings"
)

var (
	queryTypeIndexes map[int]string
	queryTypeRegex   *regexp.Regexp
)

func init() {
	regexes := []string{
		"(?P<SELECT>SELECT)",
		"(?P<UPDATE>UPDATE\\s+.*\\s+SET)",
		"(?P<DELETE_FROM>DELETE\\s+FROM)",
		"(?P<INSERT_INTO>INSERT\\s+INTO)",
		"(?P<COPY>COPY)",
		"(?P<MERGE_INTO>MERGE\\s+INTO)",
		"(?P<CREATE_TEMP_TABLE>CREATE\\s+TEMP(?:ORARY)*\\s+TABLE)",
		"(?P<CREATE_DATABASE>CREATE\\s+DATABASE)",
		"(?P<CREATE_SCHEMA>CREATE\\s+SCHEMA)",
		"(?P<CREATE_TABLE>(?:IF\\s+NOT\\s+EXISTS\\s+.*)*CREATE\\s+(?:OR\\s+REPLACE\\s+)*TABLE)",
		"(?P<CREATE_INDEX>CREATE\\s+INDEX)",
		"(?P<ALTER_TABLE>ALTER\\s+TABLE)",
		"(?P<ALTER_SESSION>ALTER\\s+SESSION)",
		"(?P<DROP_TABLE>(?:IF\\s+.*)*DROP\\s+TABLE)",
		"(?P<SHOW_TABLES>SHOW\\s+TABLES)",
		"(?P<SHOW_PARTITIONS>SHOW\\s+PARTITIONS)",
		"(?P<DESCRIBE_TABLE>DESCRIBE\\s+(?:QUERY\\s+)*TABLE)",
		"(?P<SET_TO>SET\\s+.*\\s+TO)",
	}

	queryTypeRegex = regexp.MustCompile(`^(?i)\s*(` + strings.Join(regexes, "|") + `)\s+`)

	queryTypeIndexes = make(map[int]string)
	for i, n := range queryTypeRegex.SubexpNames() {
		if n != "" {
			queryTypeIndexes[i] = n
		}
	}
}

// GetQueryType returns the type of the query.
func GetQueryType(query string) (string, bool) {
	for i, match := range queryTypeRegex.FindStringSubmatch(query) {
		if match == "" {
			continue
		}
		if queryType, ok := queryTypeIndexes[i]; ok {
			return queryType, true
		}
	}
	return "UNKNOWN", false
}

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
		"(WITH.*\\(.*)*(?P<SELECT>SELECT)",
		"(?P<UPDATE>UPDATE.*SET)",
		"(?P<DELETE_FROM>DELETE.*FROM)",
		"(?P<INSERT_INTO>INSERT.*INTO)",
		"(?P<COPY>COPY)",
		"(?P<MERGE_INTO>MERGE.*INTO)",
		"(?P<CREATE_TEMP_TABLE>CREATE.*TEMP(?:ORARY)*.*TABLE)",
		"(?P<CREATE_DATABASE>CREATE.*DATABASE)",
		"(?P<CREATE_SCHEMA>CREATE.*SCHEMA)",
		"(?P<CREATE_TABLE>(?:IF.*NOT.*EXISTS.*)*CREATE.*(?:OR.*REPLACE.*)*TABLE)",
		"(?P<CREATE_INDEX>CREATE.*INDEX)",
		"(?P<ALTER_TABLE>ALTER.*TABLE)",
		"(?P<ALTER_SESSION>ALTER.*SESSION)",
		"(?P<DROP_TABLE>(?:IF.*)*DROP.*TABLE)",
		"(?P<SHOW_TABLES>SHOW.*TABLES)",
		"(?P<SHOW_PARTITIONS>SHOW.*PARTITIONS)",
		"(?P<SHOW_SCHEMAS>SHOW.*SCHEMAS)",
		"(?P<DESCRIBE_TABLE>DESCRIBE.*(?:QUERY.*)*TABLE)",
		"(?P<SET_TO>SET.*TO)",
	}

	queryTypeRegex = regexp.MustCompile(`^(?ism)\s*(` + strings.Join(regexes, "|") + `)\s+`)

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

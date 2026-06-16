package reservedkeywords

import (
	"embed"
	"log"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	//go:embed tablescolumns.json
	tablesColumnsFile embed.FS

	//go:embed namespaces.json
	namespacesFile embed.FS

	reservedTablesColumns, reservedNamespaces map[string]map[string]struct{}
)

func init() {
	reservedTablesColumns = load(tablesColumnsFile, "tablescolumns.json")
	reservedNamespaces = load(namespacesFile, "namespaces.json")

	// BQStreamAllEvents has the same reserved keywords as BQ, so we can use the same map
	reservedTablesColumns[whutils.BQStreamAllEvents] = reservedTablesColumns[whutils.BQ]
	reservedNamespaces[whutils.BQStreamAllEvents] = reservedNamespaces[whutils.BQ]

	// SnowpipeStreaming has the same reserved keywords as SNOWFLAKE, so we can use the same map
	reservedTablesColumns[whutils.SnowpipeStreaming] = reservedTablesColumns[whutils.SNOWFLAKE]
	reservedNamespaces[whutils.SnowpipeStreaming] = reservedNamespaces[whutils.SNOWFLAKE]
}

func load(file embed.FS, fileName string) map[string]map[string]struct{} {
	data, err := file.ReadFile(fileName)
	if err != nil {
		log.Fatalf("failed to load reserved keywords from %s: %v", fileName, err)
	}

	var tempKeywords map[string][]string
	if err := jsonrs.Unmarshal(data, &tempKeywords); err != nil {
		log.Fatalf("failed to parse reserved keywords from %s: %v", fileName, err)
	}

	return lo.MapValues(tempKeywords, func(keywords []string, _ string) map[string]struct{} {
		return lo.SliceToMap(keywords, func(k string) (string, struct{}) {
			return strings.ToUpper(k), struct{}{}
		})
	})
}

// IsTableOrColumn checks if the given keyword is a reserved table/column keyword for the destination type.
func IsTableOrColumn(destType, keyword string) bool {
	return isKeywordReserved(reservedTablesColumns, destType, keyword)
}

// IsNamespace checks if the given keyword is a reserved namespace keyword for the destination type.
func IsNamespace(destType, keyword string) bool {
	return isKeywordReserved(reservedNamespaces, destType, keyword)
}

func isKeywordReserved(keywords map[string]map[string]struct{}, destType, keyword string) bool {
	_, exists := keywords[destType][strings.ToUpper(keyword)]
	return exists
}

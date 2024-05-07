package warehouseutils

import (
	"strings"
)

var statsSupportedTableNames = map[string]struct{}{
	"tracks":     {},
	"identifies": {},
	"users":      {},
	"pages":      {},
	"screens":    {},
	"aliases":    {},
	"groups":     {},
}

func TableNameForStats(tableName string) string {
	capturedTableName := strings.ToLower(tableName)
	if _, ok := statsSupportedTableNames[capturedTableName]; !ok {
		capturedTableName = "others" // making all other tableName as other, to reduce cardinality
	}
	return capturedTableName
}

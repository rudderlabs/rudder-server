package utils

import (
	"embed"
	"encoding/json"
	"log"
	"strings"

	"github.com/samber/lo"
)

var (
	//go:embed reservedcolumnstables.json
	reservedKeywordsForColumnsTablesFile embed.FS

	//go:embed reservednamespaces.json
	reservedKeywordsForNamespacesFile embed.FS

	reservedKeywordsForColumnsTables map[string]map[string]struct{}
	reservedKeywordsForNamespaces    map[string]map[string]struct{}
)

func init() {
	reservedKeywordsForColumnsTables = loadReservedKeywords(reservedKeywordsForColumnsTablesFile, "reservedcolumnstables.json")
	reservedKeywordsForNamespaces = loadReservedKeywords(reservedKeywordsForNamespacesFile, "reservednamespaces.json")
}

func loadReservedKeywords(reservedKeywordsFile embed.FS, name string) map[string]map[string]struct{} {
	data, err := reservedKeywordsFile.ReadFile(name)
	if err != nil {
		log.Fatalf("failed to load reserved keywords for %s: %v", name, err)
	}

	var tempKeywords map[string][]string
	if err := json.Unmarshal(data, &tempKeywords); err != nil {
		log.Fatalf("failed to parse reserved keywords for %s: %v", name, err)
	}

	return lo.MapValues(tempKeywords, func(keywords []string, _ string) map[string]struct{} {
		return lo.SliceToMap(keywords, func(k string) (string, struct{}) {
			return k, struct{}{}
		})
	})
}

func IsReservedKeywordForColumnsTables(destType, keyword string) bool {
	_, exists := reservedKeywordsForColumnsTables[destType][strings.ToUpper(keyword)]
	return exists
}

func IsReservedKeywordForNamespaces(destType, keyword string) bool {
	_, exists := reservedKeywordsForNamespaces[destType][strings.ToUpper(keyword)]
	return exists
}

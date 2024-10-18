package utils

import (
	"embed"
	"encoding/json"
	"log"
	"strings"

	"github.com/samber/lo"
)

var (
	//go:embed reservedkeywords.json
	reservedKeywordsFile embed.FS

	reservedKeywords map[string]map[string]struct{}
)

func init() {
	reservedKeywords = loadReservedKeywords()
}

func loadReservedKeywords() map[string]map[string]struct{} {
	data, err := reservedKeywordsFile.ReadFile("reservedkeywords.json")
	if err != nil {
		log.Fatalf("failed to load reserved keywords: %v", err)
	}

	var tempKeywords map[string][]string
	if err := json.Unmarshal(data, &tempKeywords); err != nil {
		log.Fatalf("failed to parse reserved keywords: %v", err)
	}

	return lo.MapValues(tempKeywords, func(keywords []string, _ string) map[string]struct{} {
		return lo.SliceToMap(keywords, func(k string) (string, struct{}) {
			return k, struct{}{}
		})
	})
}

func IsReservedKeyword(destType, keyword string) bool {
	_, exists := reservedKeywords[destType][strings.ToUpper(keyword)]
	return exists
}

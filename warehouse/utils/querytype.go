package warehouseutils

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	// queryTypeIndex works for both regexes as long as the groups order is not changed
	queryTypeIndex int
	queryTypeRegex *regexp.Regexp

	unknownQueryTypeRegex = regexp.MustCompile(`^(?i)\s*(?P<type>\w+)\s+`)
)

func init() {
	tokens := []string{
		"SELECT", "UPDATE", "DELETE FROM", "INSERT INTO", "COPY",
		"CREATE TEMP TABLE", "CREATE TEMPORARY TABLE",
		"CREATE DATABASE", "CREATE SCHEMA", "CREATE TABLE", "CREATE INDEX",
		"ALTER TABLE", "ALTER SESSION",
		"DROP TABLE",
	}
	queryTypeRegex = regexp.MustCompile(`^(?i)\s*(?P<type>` + strings.Join(tokens, "|") + `)\s+`)

	var found bool
	for i, name := range queryTypeRegex.SubexpNames() {
		if name == "type" {
			found = true
			queryTypeIndex = i
			break
		}
	}
	if !found {
		panic(fmt.Errorf("warehouseutils: query type index not found"))
	}
}

// GetQueryType returns the type of the query.
func GetQueryType(query string) (string, bool) {
	var (
		expected  bool
		queryType = ""
		submatch  = queryTypeRegex.FindStringSubmatch(query)
	)

	if len(submatch) > queryTypeIndex {
		expected = true
		queryType = strings.ToUpper(submatch[queryTypeIndex])
		if queryType == "CREATE TEMPORARY TABLE" {
			queryType = "CREATE TEMP TABLE"
		}
	}

	if queryType == "" { // get the first word
		submatch = unknownQueryTypeRegex.FindStringSubmatch(query)
		if len(submatch) > queryTypeIndex {
			queryType = strings.ToUpper(submatch[queryTypeIndex])
		}
	}

	if queryType == "" {
		queryType = "UNKNOWN"
	}

	return queryType, expected
}

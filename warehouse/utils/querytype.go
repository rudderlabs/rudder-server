package warehouseutils

import (
	"regexp"
)

var regexes map[*regexp.Regexp]string

func init() {
	regexes = map[*regexp.Regexp]string{
		regexp.MustCompile(`^(?i)\s*(SELECT)\s+`):                                                      "SELECT",
		regexp.MustCompile(`^(?i)\s*UPDATE.*SET\s+`):                                                   "UPDATE",
		regexp.MustCompile(`^(?i)\s*DELETE\s+FROM\s+`):                                                 "DELETE FROM",
		regexp.MustCompile(`^(?i)\s*INSERT\s+INTO\s+`):                                                 "INSERT INTO",
		regexp.MustCompile(`^(?i)\s*COPY\s+`):                                                          "COPY",
		regexp.MustCompile(`^(?i)\s*MERGE\s+INTO\s+`):                                                  "MERGE INTO",
		regexp.MustCompile(`^(?i)\s*CREATE\s+TEMP(?:ORARY)*\s+TABLE\s+`):                               "CREATE TEMP TABLE",
		regexp.MustCompile(`^(?i)\s*CREATE\s+DATABASE\s+`):                                             "CREATE DATABASE",
		regexp.MustCompile(`^(?i)\s*CREATE\s+SCHEMA\s+`):                                               "CREATE SCHEMA",
		regexp.MustCompile(`^(?i)\s*(?:IF\s+NOT\s+EXISTS\s+.*)*CREATE\s+(?:OR\s+REPLACE\s+)*TABLE\s+`): "CREATE TABLE",
		regexp.MustCompile(`^(?i)\s*CREATE\s+INDEX\s+`):                                                "CREATE INDEX",
		regexp.MustCompile(`^(?i)\s*ALTER\s+TABLE\s+`):                                                 "ALTER TABLE",
		regexp.MustCompile(`^(?i)\s*ALTER\s+SESSION\s+`):                                               "ALTER SESSION",
		regexp.MustCompile(`^(?i)\s*(?:IF\s+.*)*DROP\s+TABLE\s+`):                                      "DROP TABLE",
		regexp.MustCompile(`^(?i)\s*SHOW\s+TABLES\s+`):                                                 "SHOW TABLES",
		regexp.MustCompile(`^(?i)\s*SHOW\s+PARTITIONS\s+`):                                             "SHOW PARTITIONS",
		regexp.MustCompile(`^(?i)\s*DESCRIBE\s+(?:QUERY\s+)*TABLE\s+`):                                 "DESCRIBE TABLE",
		regexp.MustCompile(`^(?i)\s*SET\s+.*\s+TO\s+`):                                                 "SET x TO",
	}
}

// GetQueryType returns the type of the query.
func GetQueryType(query string) (string, bool) {
	for regex, token := range regexes {
		if regex.MatchString(query) {
			return token, true
		}
	}
	return "UNKNOWN", false
}

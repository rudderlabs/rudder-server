package bigquery

import "strings"

// identifierEscaper escapes characters in a GoogleSQL quoted identifier.
// Quoted identifiers share string-literal escape sequences, so the backslash
// must be escaped as well as the backtick.
var identifierEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

func quoteIdentifier(identifier string) string {
	return "`" + identifierEscaper.Replace(identifier) + "`"
}

func quoteTablePath(identifiers ...string) string {
	return quoteIdentifier(strings.Join(identifiers, "."))
}

func quoteColumnList(columns string) string {
	parts := strings.Split(columns, ",")
	for i, column := range parts {
		parts[i] = quoteIdentifier(strings.TrimSpace(column))
	}
	return strings.Join(parts, ", ")
}

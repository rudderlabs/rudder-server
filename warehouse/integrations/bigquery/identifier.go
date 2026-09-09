package bigquery

import "strings"

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "\\`") + "`"
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

func quoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

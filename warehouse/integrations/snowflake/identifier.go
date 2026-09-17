package snowflake

import "strings"

func quoteIdentifier(identifier string) string {
	return "\"" + strings.ReplaceAll(identifier, "\"", "\"\"") + "\""
}

func quoteQualifiedIdentifier(identifiers ...string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ".")
}

func quoteColumnList(columns string) string {
	parts := strings.Split(columns, ",")
	for i, column := range parts {
		parts[i] = quoteIdentifier(strings.TrimSpace(column))
	}
	return strings.Join(parts, ", ")
}

func quoteIdentifiers(identifiers []string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ",")
}

// stringLiteralEscaper escapes characters in a Snowflake single-quoted string constant.
// Snowflake interprets backslash escape sequences inside string constants, so the backslash
// must be escaped as well as the single quote.
var stringLiteralEscaper = strings.NewReplacer(`\`, `\\`, `'`, `''`)

func quoteStringLiteral(value string) string {
	return "'" + stringLiteralEscaper.Replace(value) + "'"
}

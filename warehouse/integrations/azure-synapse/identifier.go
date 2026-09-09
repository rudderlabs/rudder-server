package azuresynapse

import "strings"

func quoteIdentifier(identifier string) string {
	return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
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

func quoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteUnicodeStringLiteral(value string) string {
	return "N" + quoteStringLiteral(value)
}

func quoteQualifiedIdentifierLiteral(identifiers ...string) string {
	return quoteUnicodeStringLiteral(quoteQualifiedIdentifier(identifiers...))
}

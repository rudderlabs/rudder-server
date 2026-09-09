package deltalake

import "strings"

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func quoteQualifiedIdentifier(identifiers ...string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ".")
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
